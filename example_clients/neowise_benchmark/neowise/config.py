"""Environment-variable-driven configuration for the NEOWISE load test.

Read once at import time so values are stable for the life of the worker
process. Override by setting the relevant env vars before invoking locust.
"""

import os
from pathlib import Path
from typing import List

# ── credentials & endpoints ─────────────────────────────────────────────────
ZEROBUS_SERVER_ENDPOINT = os.getenv("ZEROBUS_SERVER_ENDPOINT", "")
DATABRICKS_WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL", "")
ZEROBUS_TABLE_NAME = os.getenv("ZEROBUS_TABLE_NAME", "")
DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "")
DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "")

REQUIRED_ENV_VARS = (
    "ZEROBUS_SERVER_ENDPOINT",
    "DATABRICKS_WORKSPACE_URL",
    "ZEROBUS_TABLE_NAME",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
)

# ── workload sizing ─────────────────────────────────────────────────────────
# Stride denominator for per-worker file assignment. MUST equal the locust -w
# (number of workers); otherwise the stride math is wrong and workers will
# either overlap or skip files.
TOTAL_WORKERS = int(os.getenv("TOTAL_WORKERS", "2048"))

# Target files per HEALPix k0 directory per worker per year. The actual count
# is capped by the global stride: (FILES_PER_K0 * 12) files per year, but
# never more than (12,288 / TOTAL_WORKERS) since each worker gets a unique
# slice.
FILES_PER_K0 = int(os.getenv("FILES_PER_K0", "1"))

DEFAULT_YEARS = (
    "year1 year2 year3 year4 year5 year6 year7 year8 year9 year10 year11 addendum"
)
NEOWISE_YEARS: List[str] = os.getenv("NEOWISE_YEARS", DEFAULT_YEARS).split()

# Rows per pyarrow decode batch. 500 keeps the per-batch CPU block (~72 ms on
# the native encoder) well under the gevent heartbeat tolerance.
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

STREAMS_PER_USER = int(os.getenv("STREAMS_PER_USER", "1"))

# Constant wait between Locust @task invocations (ms). 0 = drive as fast as
# the SDK and gRPC channel allow.
WAIT_TIME_MS = float(os.getenv("WAIT_TIME_MS", "0"))

# Local cache for parquet leaves downloaded from S3.
# Size this volume to FILES_PER_K0 * 12 * 200 MB + headroom (~3.6 GB/year).
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/neowise"))

# ── dataset constants (do not override) ─────────────────────────────────────
S3_BUCKET = "nasa-irsa-wise"
S3_PREFIX = "wise/neowiser/catalogs/p1bs_psd/healpix_k5"
NUM_K0 = 12  # HEALPix order 0: 12 tiles


def validate() -> None:
    """Raise ValueError if any required env var is unset.

    Called by NeowiseUser.__init__ rather than at import time so a stray
    `import neowise.config` from tooling doesn't fail.
    """
    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
