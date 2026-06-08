"""Dataset planning + retrieval for the NEOWISE benchmark.

Each Locust worker is assigned a disjoint global stride across the
NEOWISE-R parquet dataset on S3. This module owns:

  - the unsigned boto3 S3 client (the bucket is public)
  - listing leaf parquet keys under a (year, healpix_k0) prefix
  - the bundled `file_list.txt.gz` index (avoids per-worker `ListObjectsV2`
    stampedes at high worker counts)
  - the stride math that turns (worker_idx, year) into a list of keys
  - downloading a single parquet leaf to a local cache directory
"""

import gzip
from pathlib import Path
from typing import Dict, List, Optional

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from neowise.config import (
    DATA_DIR,
    FILES_PER_K0,
    NUM_K0,
    S3_BUCKET,
    S3_PREFIX,
    TOTAL_WORKERS,
)


def s3_client():
    """Unsigned S3 client for the public NEOWISE bucket."""
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def list_k0_files(s3, year: str, k0: int) -> List[str]:
    """List all leaf parquet keys under year/healpix_k0=k0/, sorted."""
    prefix = f"{S3_PREFIX}/{year}/neowiser-healpix_k5-{year}.parquet/healpix_k0={k0}/"
    out: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("part0.snappy.parquet"):
                out.append(key)
    out.sort()
    return out


def _stride_pick(files: List[str], idx: int, stride: int, n: int) -> List[str]:
    """Every stride-th file starting from idx, capped at n.

    Used for the global per-year file list (~12,288 files), not per-k0.
    With stride=3,072 each worker picks files at positions
    {idx, idx+3072, idx+6144, ...} — disjoint slices across the fleet.
    """
    if not files:
        return []
    return [f for i, f in enumerate(files) if i % stride == idx % stride][:n]


def _local_path_for(key: str) -> Path:
    """Turn an S3 key into a unique local filename."""
    year = next((p for p in key.split("/") if p.startswith("year") or p == "addendum"), "unknown")
    k5 = next((p for p in key.split("/") if p.startswith("healpix_k5=")), "k5=?")
    return DATA_DIR / f"{year}_{k5}.snappy.parquet"


def download(s3, key: str) -> Path:
    """Download a parquet leaf to local disk if not already present."""
    out = _local_path_for(key)
    if out.exists() and out.stat().st_size > 0:
        return out
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".part")
    s3.download_file(S3_BUCKET, key, str(tmp))
    tmp.rename(out)
    return out


# ── bundled file list (eliminates S3 ListObjectsV2 stampede) ────────────────
_BUNDLED_BY_YEAR: Dict[str, Dict[int, List[str]]] = {}
_BUNDLED_LOADED = False


def _load_bundled_index() -> None:
    """One-time load of neowise/file_list.txt.gz into {year: {k0: [keys]}}."""
    global _BUNDLED_LOADED
    if _BUNDLED_LOADED:
        return
    _BUNDLED_LOADED = True

    p = Path(__file__).parent / "file_list.txt.gz"
    if not p.exists():
        print(f"[bundled file list] {p} not found; falling back to live S3 listing")
        return

    print(f"[bundled file list] loading {p} ...")
    with gzip.open(p, "rt") as f:
        for line in f:
            key = line.strip()
            if not key:
                continue
            # key format: .../yearN/.../healpix_k0=M/healpix_k5=K/part0.snappy.parquet
            year = next(
                (p for p in key.split("/") if p.startswith("year") or p == "addendum"), None
            )
            k0_part = next((p for p in key.split("/") if p.startswith("healpix_k0=")), None)
            if not year or not k0_part:
                continue
            k0 = int(k0_part.split("=", 1)[1])
            _BUNDLED_BY_YEAR.setdefault(year, {}).setdefault(k0, []).append(key)

    for _y, by_k0 in _BUNDLED_BY_YEAR.items():
        for k0 in by_k0:
            by_k0[k0].sort()

    n = sum(len(v) for by_k0 in _BUNDLED_BY_YEAR.values() for v in by_k0.values())
    print(f"[bundled file list] loaded {n:,} keys across {len(_BUNDLED_BY_YEAR)} years")


def _bundled_keys_for_year(year: str) -> Optional[Dict[int, List[str]]]:
    """Return {k0: [keys]} for `year` from the bundled list, or None if absent."""
    _load_bundled_index()
    return _BUNDLED_BY_YEAR.get(year)


def plan_year_for_worker(s3, year: str, worker_idx: int) -> List[str]:
    """Return this worker's share of leaf-parquet keys for one year.

    GLOBAL year-wide stride (~12,288 files per year). Each worker gets at
    most `(FILES_PER_K0 * NUM_K0)` keys per year — distributed across
    whatever k0 buckets the stride lands in. With TOTAL_WORKERS=3072 and
    12,288 files/year each worker gets exactly 4 unique files per year,
    zero overlap.

    Uses `neowise/file_list.txt.gz` if present, otherwise falls back to a
    live S3 ListObjectsV2 — fine for small worker counts, but rate-limited
    (`SlowDown`) at thousands of concurrent workers.
    """
    target_n = FILES_PER_K0 * NUM_K0
    bundled = _bundled_keys_for_year(year)
    if bundled is not None:
        # Flatten all k0 sublists in sorted k0 order into one global list,
        # then stride across the whole thing.
        all_files: List[str] = []
        for k0 in sorted(bundled):
            all_files.extend(bundled[k0])
        return _stride_pick(all_files, worker_idx, TOTAL_WORKERS, target_n)

    # Live S3 fallback.
    all_files = []
    for k0 in range(NUM_K0):
        all_files.extend(list_k0_files(s3, year, k0))
    return _stride_pick(all_files, worker_idx, TOTAL_WORKERS, target_n)
