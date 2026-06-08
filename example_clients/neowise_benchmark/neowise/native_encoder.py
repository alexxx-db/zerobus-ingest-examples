"""Python wrapper around the C extension `neowise.neowise_native`.

Pulls columns out of a pyarrow.RecordBatch into the order the C extension
expects, then calls into native code to produce one `bytes` per row in proto2
wire format. Use the result with `stream.ingest_record_nowait(...)`.
"""

import time
from typing import List

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from neowise import neowise_native  # built by repo-root setup.py


# Column orders MUST match the FIELDS table in neowise_native.c.
INT64_NAMES = [
    "frame_num",
    "src",
    "nb",
    "na",
    "w1flg",
    "w2flg",
    "w1flg_1",
    "w2flg_1",
    "w1flg_2",
    "w2flg_2",
    "w1flg_3",
    "w2flg_3",
    "w1flg_4",
    "w2flg_4",
    "w1flg_5",
    "w2flg_5",
    "w1flg_6",
    "w2flg_6",
    "w1flg_7",
    "w2flg_7",
    "w1flg_8",
    "w2flg_8",
    "w1cc_map",
    "w2cc_map",
    "qual_frame",
    "det_bit",
    "sso_flg",
    "tmass_key",
    "n_2mass",
    "allwise_cntr",
    "n_allwise",
    "cntr",
    "spt_ind",
    "htm20",
]

DOUBLE_NAMES = [
    "ra",
    "dec",
    "sigra",
    "sigdec",
    "sigradec",
    "glon",
    "glat",
    "elon",
    "elat",
    "w1x",
    "w1y",
    "w2x",
    "w2y",
    "w1sky",
    "w1sigsk",
    "w1conf",
    "w2sky",
    "w2sigsk",
    "w2conf",
    "w1fitr",
    "w2fitr",
    "w1snr",
    "w2snr",
    "w1flux",
    "w1sigflux",
    "w2flux",
    "w2sigflux",
    "w1mpro",
    "w1sigmpro",
    "w1rchi2",
    "w2mpro",
    "w2sigmpro",
    "w2rchi2",
    "rchi2",
    "w1sat",
    "w2sat",
    "w1mag",
    "w1sigm",
    "w1mcor",
    "w2mag",
    "w2sigm",
    "w2mcor",
    "w1mag_1",
    "w1sigm_1",
    "w2mag_1",
    "w2sigm_1",
    "w1mag_2",
    "w1sigm_2",
    "w2mag_2",
    "w2sigm_2",
    "w1mag_3",
    "w1sigm_3",
    "w2mag_3",
    "w2sigm_3",
    "w1mag_4",
    "w1sigm_4",
    "w2mag_4",
    "w2sigm_4",
    "w1mag_5",
    "w1sigm_5",
    "w2mag_5",
    "w2sigm_5",
    "w1mag_6",
    "w1sigm_6",
    "w2mag_6",
    "w2sigm_6",
    "w1mag_7",
    "w1sigm_7",
    "w2mag_7",
    "w2sigm_7",
    "w1mag_8",
    "w1sigm_8",
    "w2mag_8",
    "w2sigm_8",
    "xscprox",
    "qi_fact",
    "saa_sep",
    "r_2mass",
    "pa_2mass",
    "j_m_2mass",
    "j_msig_2mass",
    "h_m_2mass",
    "h_msig_2mass",
    "k_m_2mass",
    "k_msig_2mass",
    "r_allwise",
    "pa_allwise",
    "w1mpro_allwise",
    "w1sigmpro_allwise",
    "w2mpro_allwise",
    "w2sigmpro_allwise",
    "w3mpro_allwise",
    "w3sigmpro_allwise",
    "w4mpro_allwise",
    "w4sigmpro_allwise",
    "mjd",
    "x",
    "y",
    "z",
]

STRING_NAMES = [
    "source_id",
    "scan_id",
    "w1frtr",
    "w2frtr",
    "satnum",
    "w1cc_map_str",
    "w2cc_map_str",
    "cc_flags",
    "moon_masked",
    "ph_qual",
]


def encode_batch_to_bytes(batch: "pa.RecordBatch", client_ts_ms: int = 0) -> List[bytes]:
    """Pull columns from a RecordBatch and hand them to the C encoder."""
    i64_cols = [
        batch.column(n).fill_null(0).to_numpy(zero_copy_only=False).astype(np.int64, copy=False) for n in INT64_NAMES
    ]
    f64_cols = [
        batch.column(n).fill_null(0.0).to_numpy(zero_copy_only=False).astype(np.float64, copy=False)
        for n in DOUBLE_NAMES
    ]
    str_cols = [pc.cast(batch.column(n), pa.binary()).to_pylist() for n in STRING_NAMES]
    if not client_ts_ms:
        client_ts_ms = int(time.time() * 1000)
    return neowise_native.encode_batch(i64_cols, f64_cols, str_cols, batch.num_rows, client_ts_ms)
