"""The Locust `User` class that runs the NEOWISE ingestion workload.

One `NeowiseUser` = one set of fire-and-forget Zerobus streams (`STREAMS_PER_USER`)
sending pre-encoded NEOWISE rows to the configured target table. The worker
walks its assigned slice of the dataset (see `neowise.dataset`) one parquet
file at a time, then goes idle once every year listed in `NEOWISE_YEARS`
has been streamed.
"""

import gc
import time
from typing import List

import gevent
import pyarrow.parquet as pq
from locust import constant, task, User

from neowise import config
from neowise.dataset import download, plan_year_for_worker, s3_client
from neowise.native_encoder import encode_batch_to_bytes
from neowise.neowise_row_pb2 import NeowiseRow
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

SUBMIT_EVENT_TYPE = "submit"
FAILURE_EVENT_TYPE = "failure"


class NeowiseUser(User):
    """One Locust user = one set of streams ingesting this worker's NEOWISE slice."""

    wait_time = constant(config.WAIT_TIME_MS / 1000)
    host = config.ZEROBUS_SERVER_ENDPOINT

    def __init__(self, environment):
        super().__init__(environment)
        config.validate()

        self.zerobus_endpoint = config.ZEROBUS_SERVER_ENDPOINT
        self.workspace_url = config.DATABRICKS_WORKSPACE_URL
        self.table_name = config.ZEROBUS_TABLE_NAME
        self.client_id = config.DATABRICKS_CLIENT_ID
        self.client_secret = config.DATABRICKS_CLIENT_SECRET

        # Worker index — assigned by the Locust master at registration.
        # Falls back to 0 for the standalone (master-less) case;
        # `master == -1` in some Locust versions, also normalized to 0.
        self.worker_index = getattr(self.environment.runner, "worker_index", 0) or 0
        if self.worker_index < 0:
            self.worker_index = 0
        print(
            f"NeowiseUser worker_index={self.worker_index} / total={config.TOTAL_WORKERS}"
        )

        # Lightweight init — defer S3 listing, downloads, and stream creation
        # to on_start so __init__ stays fast and the worker registers with
        # the master before any heartbeat-timed-out reconnect cycle.
        self._s3 = None
        self._year_idx = -1
        self._current_year_keys: List[str] = []
        self._file_idx = 0
        self.parquet_file = None
        self.batch_iter = None
        self.current_rows: List[bytes] = []  # pre-serialized proto bytes
        self.row_index = 0
        self._stream_idx = 0
        self.sdk = None
        self.streams: List = []
        # Set to True once this worker has streamed its full slice of every
        # year in NEOWISE_YEARS. When True, @task becomes a long sleep —
        # the user stays alive (so Locust doesn't try to respawn it and the
        # streams stay open for in-flight commits to finish) but no new
        # rows are sent.
        self._exhausted = False

    def on_start(self):
        """Slow startup, run after the worker is registered with master."""
        # Stagger by worker_index so 1024 workers don't all hit S3 at once.
        # 50 ms × idx -> worst case ~51 s of jitter across 1024 workers.
        time.sleep(min(0.05 * self.worker_index, 30.0))

        self._s3 = s3_client()
        self._advance_to_next_file()
        self.sdk = ZerobusSdk(self.zerobus_endpoint, self.workspace_url)
        self._create_streams()
        print(
            f"NeowiseUser ready: {config.STREAMS_PER_USER} stream(s), "
            f"worker={self.worker_index}"
        )

        # Periodically report which year + file index this worker is on, as
        # a Locust event. Aggregated in the master stats CSV / web UI under
        # request_type='year-progress' so fleet progress is visible at a glance.
        self._progress_greenlet = gevent.spawn(self._poll_year_progress)

    def _poll_year_progress(self):
        while True:
            year = "exhausted" if self._exhausted else config.NEOWISE_YEARS[self._year_idx]
            self.environment.events.request.fire(
                request_type="year-progress",
                name=year,
                start_time=time.time(),
                response_time=self._file_idx,  # files done in current year
                response_length=len(self._current_year_keys),
                response=None,
                context={},
                exception=None,
            )
            gevent.sleep(30)

    # ── data loop ───────────────────────────────────────────────────────────
    def _advance_to_next_year(self):
        """Move to the next year in NEOWISE_YEARS, or mark the user exhausted.

        Does NOT wrap — once a worker has streamed its slice of every year,
        `self._exhausted = True` and @task becomes a long sleep. We don't
        raise StopUser because Locust will respawn dying users during the
        spawn phase (which would re-stream the same rows). The user lives
        but stays silent until the -t deadline ends the run.
        """
        self._year_idx += 1
        if self._year_idx >= len(config.NEOWISE_YEARS):
            print(
                f"worker {self.worker_index} exhausted all "
                f"{len(config.NEOWISE_YEARS)} years; going idle"
            )
            self._exhausted = True
            return
        year = config.NEOWISE_YEARS[self._year_idx]
        print(f"[{year}] worker {self.worker_index} listing files ...")
        self._current_year_keys = plan_year_for_worker(self._s3, year, self.worker_index)
        self._file_idx = 0
        print(
            f"[{year}] worker {self.worker_index} got "
            f"{len(self._current_year_keys)} files"
        )

    def _advance_to_next_file(self):
        """Move the read cursor to the next parquet file in the plan."""
        while True:
            if self._exhausted:
                return
            if self._file_idx >= len(self._current_year_keys):
                self._advance_to_next_year()
                continue
            key = self._current_year_keys[self._file_idx]
            self._file_idx += 1
            local = download(self._s3, key)
            print(
                f"[{config.NEOWISE_YEARS[self._year_idx]}] file "
                f"{self._file_idx}/{len(self._current_year_keys)}: {local.name}"
            )
            self.parquet_file = pq.ParquetFile(str(local))
            self.batch_iter = self.parquet_file.iter_batches(batch_size=config.BATCH_SIZE)
            self._load_next_batch()
            return

    def _load_next_batch(self):
        del self.current_rows[:]
        gc.collect()
        try:
            batch = next(self.batch_iter)
        except StopIteration:
            self._advance_to_next_file()
            return
        # Native C encoder: significantly faster end-to-end than the python
        # proto-serialize path. client_ts_ms is stamped per-batch (same value
        # for all rows in this batch); if you want per-row timestamps, move
        # the stamp into the @task and edit the bytes inline (cheap because
        # the field is at a known offset).
        self.current_rows = encode_batch_to_bytes(
            batch, client_ts_ms=int(time.time() * 1000)
        )
        self.row_index = 0

    # ── streams ─────────────────────────────────────────────────────────────
    def _make_stream(self):
        """Open one ingest stream — fire-and-forget (no ack callback)."""
        options = StreamConfigurationOptions(
            record_type=RecordType.PROTO,
            max_inflight_records=50000,
            recovery=True,
        )
        table_props = TableProperties(self.table_name, NeowiseRow.DESCRIPTOR)
        return self.sdk.create_stream(
            self.client_id, self.client_secret, table_props, options
        )

    def _create_streams(self):
        self.streams = [self._make_stream() for _ in range(config.STREAMS_PER_USER)]

    def _recreate_stream(self, idx: int):
        try:
            self.streams[idx] = self._make_stream()
            print(f"Stream {idx} recreated")
        except Exception as e:
            print(f"Stream {idx} recreate failed: {e}")

    def _fire_request_event(self, name, start_time, response_time_ms, exception=None):
        self.environment.events.request.fire(
            request_type="grpc",
            name=name,
            start_time=start_time,
            response_time=response_time_ms,
            response_length=0,
            response=None,
            context={},
            exception=exception,
        )

    # ── ingest task ─────────────────────────────────────────────────────────
    @task
    def ingest_row(self):
        if self._exhausted:
            time.sleep(60)
            return

        if self.row_index >= len(self.current_rows):
            self._load_next_batch()
            if self._exhausted:
                time.sleep(60)
                return

        idx = self._stream_idx % len(self.streams)
        self._stream_idx += 1
        stream = self.streams[idx]

        start_time = time.time()
        start_perf = time.perf_counter()
        try:
            row_bytes = self.current_rows[self.row_index]
            stream.ingest_record_nowait(row_bytes)  # fire-and-forget pre-encoded bytes
            self.row_index += 1
            self._fire_request_event(
                SUBMIT_EVENT_TYPE, start_time, (time.perf_counter() - start_perf) * 1000
            )
        except Exception as e:
            self._fire_request_event(
                FAILURE_EVENT_TYPE,
                start_time,
                (time.perf_counter() - start_perf) * 1000,
                exception=e,
            )
            self._recreate_stream(idx)

    def on_stop(self):
        print("Test done. Flushing streams.")
        for i, stream in enumerate(self.streams):
            try:
                stream.flush()
                stream.close()
            except Exception as e:
                print(f"Stream {i} close failed: {e}")
