/* NEOWISE row encoder — Python C extension.
 *
 * Skips the Python protobuf descriptor walk; encodes each NeowiseRow's wire
 * bytes directly from typed column buffers. Tags are pre-computed at module
 * init from the field table below. Fields are NEOWISE schema (143 cols) plus
 * `client_ts_ms` at field number 200.
 *
 * Build:
 *   python setup.py build_ext --inplace
 *
 * Use:
 *   from neowise import neowise_native
 *   bytes_list = neowise_native.encode_batch(
 *       i64_cols, f64_cols, str_cols, num_rows, client_ts_ms,
 *   )
 *
 * Args (column order MUST match native_encoder.py):
 *   i64_cols     : list[np.ndarray int64]   (34 entries, order = INT64_NAMES)
 *   f64_cols     : list[np.ndarray float64] (99 entries, order = DOUBLE_NAMES)
 *   str_cols     : list[list[bytes]]        (10 entries, order = STRING_NAMES)
 *   num_rows     : int
 *   client_ts_ms : int (stamped on every row in the batch)
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <string.h>

/* -------- field table (hand-typed, order = proto schema order) -------- */

typedef enum { TY_I64 = 0, TY_F64 = 1, TY_STR = 2, TY_TS = 3 } FType;

typedef struct {
  int field_no;
  FType type;
  int col_idx;    /* index inside the per-type column array */
  uint8_t tag[2]; /* pre-computed at init */
  int tag_len;    /* 1 for fields 1..15 of wire-type 0/1/2; 2 for 16..2047 */
} Field;

/* 144 entries: 143 from the parquet schema + client_ts_ms (field 200).
 * The col_idx column gives the index within the same-type column list.
 */
static Field FIELDS[] = {
    /* field_no, type, col_idx — order matches NeowiseRow schema */
    {1, TY_STR, 0, {0}, 0},    /* source_id */
    {2, TY_I64, 0, {0}, 0},    /* frame_num */
    {3, TY_STR, 1, {0}, 0},    /* scan_id */
    {4, TY_I64, 1, {0}, 0},    /* src */
    {5, TY_F64, 0, {0}, 0},    /* ra */
    {6, TY_F64, 1, {0}, 0},    /* dec */
    {7, TY_F64, 2, {0}, 0},    /* sigra */
    {8, TY_F64, 3, {0}, 0},    /* sigdec */
    {9, TY_F64, 4, {0}, 0},    /* sigradec */
    {10, TY_F64, 5, {0}, 0},   /* glon */
    {11, TY_F64, 6, {0}, 0},   /* glat */
    {12, TY_F64, 7, {0}, 0},   /* elon */
    {13, TY_F64, 8, {0}, 0},   /* elat */
    {14, TY_F64, 9, {0}, 0},   /* w1x */
    {15, TY_F64, 10, {0}, 0},  /* w1y */
    {16, TY_F64, 11, {0}, 0},  /* w2x */
    {17, TY_F64, 12, {0}, 0},  /* w2y */
    {18, TY_F64, 13, {0}, 0},  /* w1sky */
    {19, TY_F64, 14, {0}, 0},  /* w1sigsk */
    {20, TY_F64, 15, {0}, 0},  /* w1conf */
    {21, TY_F64, 16, {0}, 0},  /* w2sky */
    {22, TY_F64, 17, {0}, 0},  /* w2sigsk */
    {23, TY_F64, 18, {0}, 0},  /* w2conf */
    {24, TY_F64, 19, {0}, 0},  /* w1fitr */
    {25, TY_F64, 20, {0}, 0},  /* w2fitr */
    {26, TY_F64, 21, {0}, 0},  /* w1snr */
    {27, TY_F64, 22, {0}, 0},  /* w2snr */
    {28, TY_F64, 23, {0}, 0},  /* w1flux */
    {29, TY_F64, 24, {0}, 0},  /* w1sigflux */
    {30, TY_F64, 25, {0}, 0},  /* w2flux */
    {31, TY_F64, 26, {0}, 0},  /* w2sigflux */
    {32, TY_F64, 27, {0}, 0},  /* w1mpro */
    {33, TY_F64, 28, {0}, 0},  /* w1sigmpro */
    {34, TY_F64, 29, {0}, 0},  /* w1rchi2 */
    {35, TY_F64, 30, {0}, 0},  /* w2mpro */
    {36, TY_F64, 31, {0}, 0},  /* w2sigmpro */
    {37, TY_F64, 32, {0}, 0},  /* w2rchi2 */
    {38, TY_F64, 33, {0}, 0},  /* rchi2 */
    {39, TY_I64, 2, {0}, 0},   /* nb */
    {40, TY_I64, 3, {0}, 0},   /* na */
    {41, TY_STR, 2, {0}, 0},   /* w1frtr */
    {42, TY_STR, 3, {0}, 0},   /* w2frtr */
    {43, TY_F64, 34, {0}, 0},  /* w1sat */
    {44, TY_F64, 35, {0}, 0},  /* w2sat */
    {45, TY_STR, 4, {0}, 0},   /* satnum */
    {46, TY_F64, 36, {0}, 0},  /* w1mag */
    {47, TY_F64, 37, {0}, 0},  /* w1sigm */
    {48, TY_I64, 4, {0}, 0},   /* w1flg */
    {49, TY_F64, 38, {0}, 0},  /* w1mcor */
    {50, TY_F64, 39, {0}, 0},  /* w2mag */
    {51, TY_F64, 40, {0}, 0},  /* w2sigm */
    {52, TY_I64, 5, {0}, 0},   /* w2flg */
    {53, TY_F64, 41, {0}, 0},  /* w2mcor */
    {54, TY_F64, 42, {0}, 0},  /* w1mag_1 */
    {55, TY_F64, 43, {0}, 0},  /* w1sigm_1 */
    {56, TY_I64, 6, {0}, 0},   /* w1flg_1 */
    {57, TY_F64, 44, {0}, 0},  /* w2mag_1 */
    {58, TY_F64, 45, {0}, 0},  /* w2sigm_1 */
    {59, TY_I64, 7, {0}, 0},   /* w2flg_1 */
    {60, TY_F64, 46, {0}, 0},  /* w1mag_2 */
    {61, TY_F64, 47, {0}, 0},  /* w1sigm_2 */
    {62, TY_I64, 8, {0}, 0},   /* w1flg_2 */
    {63, TY_F64, 48, {0}, 0},  /* w2mag_2 */
    {64, TY_F64, 49, {0}, 0},  /* w2sigm_2 */
    {65, TY_I64, 9, {0}, 0},   /* w2flg_2 */
    {66, TY_F64, 50, {0}, 0},  /* w1mag_3 */
    {67, TY_F64, 51, {0}, 0},  /* w1sigm_3 */
    {68, TY_I64, 10, {0}, 0},  /* w1flg_3 */
    {69, TY_F64, 52, {0}, 0},  /* w2mag_3 */
    {70, TY_F64, 53, {0}, 0},  /* w2sigm_3 */
    {71, TY_I64, 11, {0}, 0},  /* w2flg_3 */
    {72, TY_F64, 54, {0}, 0},  /* w1mag_4 */
    {73, TY_F64, 55, {0}, 0},  /* w1sigm_4 */
    {74, TY_I64, 12, {0}, 0},  /* w1flg_4 */
    {75, TY_F64, 56, {0}, 0},  /* w2mag_4 */
    {76, TY_F64, 57, {0}, 0},  /* w2sigm_4 */
    {77, TY_I64, 13, {0}, 0},  /* w2flg_4 */
    {78, TY_F64, 58, {0}, 0},  /* w1mag_5 */
    {79, TY_F64, 59, {0}, 0},  /* w1sigm_5 */
    {80, TY_I64, 14, {0}, 0},  /* w1flg_5 */
    {81, TY_F64, 60, {0}, 0},  /* w2mag_5 */
    {82, TY_F64, 61, {0}, 0},  /* w2sigm_5 */
    {83, TY_I64, 15, {0}, 0},  /* w2flg_5 */
    {84, TY_F64, 62, {0}, 0},  /* w1mag_6 */
    {85, TY_F64, 63, {0}, 0},  /* w1sigm_6 */
    {86, TY_I64, 16, {0}, 0},  /* w1flg_6 */
    {87, TY_F64, 64, {0}, 0},  /* w2mag_6 */
    {88, TY_F64, 65, {0}, 0},  /* w2sigm_6 */
    {89, TY_I64, 17, {0}, 0},  /* w2flg_6 */
    {90, TY_F64, 66, {0}, 0},  /* w1mag_7 */
    {91, TY_F64, 67, {0}, 0},  /* w1sigm_7 */
    {92, TY_I64, 18, {0}, 0},  /* w1flg_7 */
    {93, TY_F64, 68, {0}, 0},  /* w2mag_7 */
    {94, TY_F64, 69, {0}, 0},  /* w2sigm_7 */
    {95, TY_I64, 19, {0}, 0},  /* w2flg_7 */
    {96, TY_F64, 70, {0}, 0},  /* w1mag_8 */
    {97, TY_F64, 71, {0}, 0},  /* w1sigm_8 */
    {98, TY_I64, 20, {0}, 0},  /* w1flg_8 */
    {99, TY_F64, 72, {0}, 0},  /* w2mag_8 */
    {100, TY_F64, 73, {0}, 0}, /* w2sigm_8 */
    {101, TY_I64, 21, {0}, 0}, /* w2flg_8 */
    {102, TY_F64, 74, {0}, 0}, /* xscprox */
    {103, TY_I64, 22, {0}, 0}, /* w1cc_map */
    {104, TY_STR, 5, {0}, 0},  /* w1cc_map_str */
    {105, TY_I64, 23, {0}, 0}, /* w2cc_map */
    {106, TY_STR, 6, {0}, 0},  /* w2cc_map_str */
    {107, TY_STR, 7, {0}, 0},  /* cc_flags */
    {108, TY_I64, 24, {0}, 0}, /* qual_frame */
    {109, TY_F64, 75, {0}, 0}, /* qi_fact */
    {110, TY_F64, 76, {0}, 0}, /* saa_sep */
    {111, TY_STR, 8, {0}, 0},  /* moon_masked */
    {112, TY_I64, 25, {0}, 0}, /* det_bit */
    {113, TY_STR, 9, {0}, 0},  /* ph_qual */
    {114, TY_I64, 26, {0}, 0}, /* sso_flg */
    {115, TY_I64, 27, {0}, 0}, /* tmass_key */
    {116, TY_F64, 77, {0}, 0}, /* r_2mass */
    {117, TY_F64, 78, {0}, 0}, /* pa_2mass */
    {118, TY_I64, 28, {0}, 0}, /* n_2mass */
    {119, TY_F64, 79, {0}, 0}, /* j_m_2mass */
    {120, TY_F64, 80, {0}, 0}, /* j_msig_2mass */
    {121, TY_F64, 81, {0}, 0}, /* h_m_2mass */
    {122, TY_F64, 82, {0}, 0}, /* h_msig_2mass */
    {123, TY_F64, 83, {0}, 0}, /* k_m_2mass */
    {124, TY_F64, 84, {0}, 0}, /* k_msig_2mass */
    {125, TY_I64, 29, {0}, 0}, /* allwise_cntr */
    {126, TY_F64, 85, {0}, 0}, /* r_allwise */
    {127, TY_F64, 86, {0}, 0}, /* pa_allwise */
    {128, TY_I64, 30, {0}, 0}, /* n_allwise */
    {129, TY_F64, 87, {0}, 0}, /* w1mpro_allwise */
    {130, TY_F64, 88, {0}, 0}, /* w1sigmpro_allwise */
    {131, TY_F64, 89, {0}, 0}, /* w2mpro_allwise */
    {132, TY_F64, 90, {0}, 0}, /* w2sigmpro_allwise */
    {133, TY_F64, 91, {0}, 0}, /* w3mpro_allwise */
    {134, TY_F64, 92, {0}, 0}, /* w3sigmpro_allwise */
    {135, TY_F64, 93, {0}, 0}, /* w4mpro_allwise */
    {136, TY_F64, 94, {0}, 0}, /* w4sigmpro_allwise */
    {137, TY_F64, 95, {0}, 0}, /* mjd */
    {138, TY_I64, 31, {0}, 0}, /* cntr */
    {139, TY_F64, 96, {0}, 0}, /* x */
    {140, TY_F64, 97, {0}, 0}, /* y */
    {141, TY_F64, 98, {0}, 0}, /* z */
    {142, TY_I64, 32, {0}, 0}, /* spt_ind */
    {143, TY_I64, 33, {0}, 0}, /* htm20 */
    {200, TY_TS, 0, {0}, 0},   /* client_ts_ms — scalar passed per call */
};

#define N_FIELDS (sizeof(FIELDS) / sizeof(FIELDS[0]))
#define N_I64_COLS 34
#define N_F64_COLS 99
#define N_STR_COLS 10

/* -------- varint helpers -------- */

static inline int write_varint(uint8_t *buf, uint64_t v) {
  int n = 0;
  while (v > 0x7F) {
    buf[n++] = (uint8_t)((v & 0x7F) | 0x80);
    v >>= 7;
  }
  buf[n++] = (uint8_t)v;
  return n;
}

/* -------- tag pre-computation (runs once at module init) -------- */

static void compute_tags(void) {
  for (size_t i = 0; i < N_FIELDS; i++) {
    int wire;
    switch (FIELDS[i].type) {
    case TY_I64:
    case TY_TS:
      wire = 0;
      break; /* varint */
    case TY_F64:
      wire = 1;
      break; /* fixed64 */
    case TY_STR:
      wire = 2;
      break; /* length-delimited */
    default:
      wire = 0;
      break;
    }
    uint64_t tag = ((uint64_t)FIELDS[i].field_no << 3) | (uint64_t)wire;
    FIELDS[i].tag_len = write_varint(FIELDS[i].tag, tag);
  }
}

/* -------- encode_batch -------- */

static PyObject *encode_batch(PyObject *self, PyObject *args) {
  PyObject *i64_list, *f64_list, *str_list;
  Py_ssize_t num_rows;
  long long client_ts_ms;

  if (!PyArg_ParseTuple(args, "OOOnL", &i64_list, &f64_list, &str_list,
                        &num_rows, &client_ts_ms)) {
    return NULL;
  }

  /* Borrow buffer pointers from each numpy/list column once. */
  const int64_t *i64_ptr[N_I64_COLS];
  const double *f64_ptr[N_F64_COLS];
  PyObject *str_col[N_STR_COLS];

  for (int i = 0; i < N_I64_COLS; i++) {
    PyObject *arr = PyList_GET_ITEM(i64_list, i);
    Py_buffer buf;
    if (PyObject_GetBuffer(arr, &buf, PyBUF_C_CONTIGUOUS | PyBUF_FORMAT) != 0)
      return NULL;
    i64_ptr[i] = (const int64_t *)buf.buf;
    PyBuffer_Release(&buf); /* fine — numpy keeps the underlying memory alive */
  }
  for (int i = 0; i < N_F64_COLS; i++) {
    PyObject *arr = PyList_GET_ITEM(f64_list, i);
    Py_buffer buf;
    if (PyObject_GetBuffer(arr, &buf, PyBUF_C_CONTIGUOUS | PyBUF_FORMAT) != 0)
      return NULL;
    f64_ptr[i] = (const double *)buf.buf;
    PyBuffer_Release(&buf);
  }
  for (int i = 0; i < N_STR_COLS; i++) {
    str_col[i] = PyList_GET_ITEM(str_list, i); /* a Python list of bytes */
  }

  PyObject *out = PyList_New(num_rows);
  if (!out)
    return NULL;

  /* Row buffer. Worst-case: 143 fields × ~12 B = ~1.7 KB. Strings can be
   * longer; we bound at 64 KB which is more than any NEOWISE row.
   */
  static uint8_t row_buf[65536];

  for (Py_ssize_t r = 0; r < num_rows; r++) {
    int p = 0;

    for (size_t f = 0; f < N_FIELDS; f++) {
      const Field *fd = &FIELDS[f];
      memcpy(&row_buf[p], fd->tag, fd->tag_len);
      p += fd->tag_len;

      switch (fd->type) {
      case TY_I64: {
        p += write_varint(&row_buf[p], (uint64_t)i64_ptr[fd->col_idx][r]);
        break;
      }
      case TY_F64: {
        double v = f64_ptr[fd->col_idx][r];
        memcpy(&row_buf[p], &v, 8);
        p += 8;
        break;
      }
      case TY_STR: {
        PyObject *s = PyList_GET_ITEM(str_col[fd->col_idx], r);
        char *sp;
        Py_ssize_t slen;
        if (s == Py_None) {
          sp = "";
          slen = 0;
        } else if (PyBytes_AsStringAndSize(s, &sp, &slen) < 0) {
          Py_DECREF(out);
          return NULL;
        }
        p += write_varint(&row_buf[p], (uint64_t)slen);
        if (slen > 0) {
          memcpy(&row_buf[p], sp, slen);
          p += slen;
        }
        break;
      }
      case TY_TS: {
        p += write_varint(&row_buf[p], (uint64_t)client_ts_ms);
        break;
      }
      }
    }

    PyObject *b = PyBytes_FromStringAndSize((const char *)row_buf, p);
    if (!b) {
      Py_DECREF(out);
      return NULL;
    }
    PyList_SET_ITEM(out, r, b);
  }

  return out;
}

/* -------- module setup -------- */

static PyMethodDef Methods[] = {{"encode_batch", encode_batch, METH_VARARGS,
                                 "encode_batch(i64_cols, f64_cols, str_cols, "
                                 "num_rows, client_ts_ms) -> list[bytes]"},
                                {NULL, NULL, 0, NULL}};

static struct PyModuleDef moduledef = {PyModuleDef_HEAD_INIT, "neowise_native",
                                       NULL, -1, Methods};

PyMODINIT_FUNC PyInit_neowise_native(void) {
  compute_tags();
  return PyModule_Create(&moduledef);
}
