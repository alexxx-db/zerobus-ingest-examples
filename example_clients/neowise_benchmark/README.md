# zerobus-neowise-benchmark

End-to-end ingestion benchmark for [Databricks Zerobus](https://docs.databricks.com/aws/en/ingestion/zerobus-overview) using the public **NASA/IRSA NEOWISE-R Single-exposure Source Table** as the data source. Each Locust worker streams a disjoint slice of the dataset into a Unity Catalog table via the official [`databricks-zerobus-ingest-sdk`](https://github.com/databricks/zerobus-sdk) Python SDK.

The workload is designed to scale to thousands of workers — at 2,048 workers it cleanly partitions the ~12,288 parquet leaf files per year across the fleet with zero overlap.

## Dataset

`s3://nasa-irsa-wise/wise/neowiser/catalogs/p1bs_psd/healpix_k5/` — NASA/IRSA NEOWISE-R single-exposure source catalog. Public, unsigned (no AWS credentials needed).

- **Files**: 135,838 snappy-compressed parquet leaves across 12 years (`year1` … `year11` + `addendum`), partitioned by HEALPix sky tiling. A full year is 12 HEALPix-order-0 tiles × ~1,024 finer tiles = ~12,288 files; `addendum` has 670.
- **Rows**: ~200 billion source detections.
- **Schema**: 143 columns in the source parquet (mostly `float64`, plus a few `BIGINT` and `STRING`). The Delta target adds `client_ts_ms` (BIGINT) bringing the table to 144 columns. See `neowise/create_table.sql`.
- **Wire size**: ~1,171 bytes per row in proto2 (~1 KB). Encoded by `neowise.neowise_native` from a pyarrow batch.

The bundled `neowise/file_list.txt.gz` (389 KB) ships all 135,838 S3 keys in the image, so workers skip `ListObjectsV2` (which returns `SlowDown` at thousands of concurrent listers).

## Repository layout

```
neowise_loadtest.py           # Locust entrypoint (thin)
setup.py                      # builds neowise.neowise_native (C row encoder)
requirements.txt              # Python deps (pip)
Dockerfile                    # container image — see Run section
neowise/
├── __init__.py
├── config.py                 # env-var-driven config
├── locust_tuning.py          # Locust monkey-patches for high-worker-count runs
├── dataset.py                # S3 + per-worker file planning
├── user.py                   # NeowiseUser Locust class
├── native_encoder.py         # pyarrow -> proto2 wire bytes (calls C ext)
├── neowise_native.c          # C row encoder source
├── neowise_row_pb2.py        # generated protobuf for the NEOWISE row type
├── create_table.sql          # Delta DDL for the target Unity Catalog table
└── file_list.txt.gz          # pre-bundled index of all NEOWISE-R parquet keys
```

## Prerequisites

Follow the official [Zerobus Ingest setup guide](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest) end-to-end. Concretely you need:

1. A Databricks workspace in a [Zerobus-supported region](https://docs.databricks.com/aws/en/ingestion/zerobus-limits).
2. A service principal with `USE CATALOG` / `USE SCHEMA` / `SELECT` + `MODIFY` on the target table, and an OAuth 2.0 client ID + secret for it.
3. The Zerobus shard endpoint for your region (`https://<shard-id>.zerobus.<region>.cloud.databricks.com` on AWS, `.azuredatabricks.net` on Azure) — discoverable from the workspace as described in the setup guide above.
4. The target table created with `neowise/create_table.sql` — edit the `<catalog>.<schema>.<table>` placeholder, run it against your workspace.

Background: [Zerobus Ingest overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview).

## Configuration

All settings come from environment variables (see `neowise/config.py`):

| Variable | Required | Default | Description |
|---|---|---|---|
| `ZEROBUS_SERVER_ENDPOINT` | yes | — | `https://<shard>.zerobus.<region>.cloud.databricks.com` |
| `DATABRICKS_WORKSPACE_URL` | yes | — | `https://<workspace>.cloud.databricks.com` |
| `ZEROBUS_TABLE_NAME` | yes | — | Fully qualified `catalog.schema.table` |
| `DATABRICKS_CLIENT_ID` | yes | — | OAuth 2.0 client ID |
| `DATABRICKS_CLIENT_SECRET` | yes | — | OAuth 2.0 client secret |
| `TOTAL_WORKERS` | no | `2048` | **Must equal Locust `-w`** — stride denominator for file assignment |
| `FILES_PER_K0` | no | `1` | Target files per HEALPix k0 per worker per year |
| `NEOWISE_YEARS` | no | `year1 ... year11 addendum` | Space-separated year list |
| `BATCH_SIZE` | no | `500` | Rows per pyarrow decode batch |
| `STREAMS_PER_USER` | no | `1` | Zerobus streams per Locust user |
| `WAIT_TIME_MS` | no | `0` | Constant wait between `@task` calls |
| `DATA_DIR` | no | `/tmp/neowise` | Local parquet cache directory |

**Recommended scale: 2,048 workers** with a spawn rate of `-r 0.5` (one new user every two seconds). 12,288 files/year ÷ 2,048 = 6 unique files per worker per year, no overlap and no skips. Slower ramp lets server-side autoscaling warm up cleanly without throttling. Other clean choices are `512`, `1024`, `3072`, and `4096` (all divisors of 12,288 = 2¹² × 3).

## Run

This is a distributed benchmark. One Locust master, `N` Locust workers (one worker process per CPU core — Python GIL). The Docker image is the build artifact; both roles run the same image.

### 1. Build and push

```bash
docker build -t <your-registry>/zerobus-neowise-benchmark:latest .
docker push     <your-registry>/zerobus-neowise-benchmark:latest
```

### 2. Locust commands

Image entrypoint is `locust -f neowise_loadtest.py`; trailing args become Locust flags.

**Master** — control channel on `5557`, web UI on `8089`:

```bash
locust -f neowise_loadtest.py \
  --master --headless --expect-workers 2048 \
  -u 2048 -r 0.5 -t 6h \
  --csv=/data/neowise --csv-full-history
```

**Worker**:

```bash
locust -f neowise_loadtest.py --worker --master-host=<MASTER_HOST>
```

Flags:
- `--expect-workers N`: master waits for N workers before starting (required in headless mode).
- `-u N`: target user count; match to the worker replica count (one user per worker).
- `-r RATE`: spawn rate users/sec. `0.5` recommended at 2k+ so Zerobus + RIG autoscaling can warm up.
- `-t DURATION`: run time (`6h`, `2h`, ...).
- `--csv PREFIX --csv-full-history`: per-second CSV stats. Master-only — workers send stats up, master aggregates.

Every `ZEROBUS_*` / `DATABRICKS_*` / `TOTAL_WORKERS` env var (see [Configuration](#configuration)) must be set on both the master and every worker — Locust does not forward env vars between processes.

### 3. Kubernetes

```yaml
# locust-master.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: neowise-master
spec:
  replicas: 1
  selector: { matchLabels: { app: neowise-master } }
  template:
    metadata: { labels: { app: neowise-master } }
    spec:
      containers:
      - name: locust
        image: <your-registry>/zerobus-neowise-benchmark:latest
        args:
          - --master
          - --headless
          - --expect-workers=2048
          - -u
          - "2048"
          - -r
          - "0.5"
          - -t
          - 6h
          - --csv=/data/neowise
          - --csv-full-history
        ports:
        - { name: control, containerPort: 5557 }
        - { name: web,     containerPort: 8089 }
        env:
        - { name: ZEROBUS_SERVER_ENDPOINT,  value: "https://<shard>.zerobus.<region>.cloud.databricks.com" }
        - { name: DATABRICKS_WORKSPACE_URL, value: "https://<workspace>.cloud.databricks.com" }
        - { name: ZEROBUS_TABLE_NAME,       value: "<catalog>.<schema>.<table>" }
        - { name: TOTAL_WORKERS,            value: "2048" }
        - { name: DATABRICKS_CLIENT_ID,     valueFrom: { secretKeyRef: { name: zerobus-oauth, key: client_id } } }
        - { name: DATABRICKS_CLIENT_SECRET, valueFrom: { secretKeyRef: { name: zerobus-oauth, key: client_secret } } }
        resources:
          requests: { cpu: "4", memory: "16Gi" }
          limits:   { cpu: "8", memory: "32Gi" }
---
apiVersion: v1
kind: Service
metadata:
  name: neowise-master
spec:
  selector: { app: neowise-master }
  ports:
  - { name: control, port: 5557, targetPort: 5557 }
  - { name: web,     port: 8089, targetPort: 8089 }
```

```yaml
# locust-worker.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: neowise-worker
spec:
  replicas: 2048
  selector: { matchLabels: { app: neowise-worker } }
  template:
    metadata: { labels: { app: neowise-worker } }
    spec:
      containers:
      - name: locust
        image: <your-registry>/zerobus-neowise-benchmark:latest
        args: [ --worker, --master-host=neowise-master ]
        env:
        - { name: ZEROBUS_SERVER_ENDPOINT,  value: "https://<shard>.zerobus.<region>.cloud.databricks.com" }
        - { name: DATABRICKS_WORKSPACE_URL, value: "https://<workspace>.cloud.databricks.com" }
        - { name: ZEROBUS_TABLE_NAME,       value: "<catalog>.<schema>.<table>" }
        - { name: TOTAL_WORKERS,            value: "2048" }
        - { name: DATABRICKS_CLIENT_ID,     valueFrom: { secretKeyRef: { name: zerobus-oauth, key: client_id } } }
        - { name: DATABRICKS_CLIENT_SECRET, valueFrom: { secretKeyRef: { name: zerobus-oauth, key: client_secret } } }
        resources:
          requests: { cpu: "1500m", memory: "2Gi", ephemeral-storage: "10Gi" }
          limits:   { cpu: "1500m", memory: "2Gi", ephemeral-storage: "10Gi" }
```

Apply:

```bash
kubectl create secret generic zerobus-oauth \
  --from-literal=client_id=<...> --from-literal=client_secret=<...>

kubectl apply -f locust-master.yaml
kubectl wait --for=condition=available deploy/neowise-master --timeout=2m
kubectl apply -f locust-worker.yaml
kubectl rollout status deploy/neowise-worker
kubectl logs -f deploy/neowise-master
```

The test starts when all 2048 workers register. CSV stats land on the master at `/data/neowise_*.csv` — mount a PVC there if you want them off-pod. Tear down with `kubectl delete -f locust-worker.yaml -f locust-master.yaml`.

### Resource sizing

| Pod | CPU | Memory | Ephemeral storage |
|---|---|---|---|
| Worker | 1.5 | 2 GiB | 10 GiB (parquet cache in `DATA_DIR`) |
| Master | 4 | 16 GiB | — |

### Caveats

- `TOTAL_WORKERS` must equal the worker replica count. Update both together.
- Env vars do not propagate from master to worker. Set them on both specs.
- One worker process per pod. Don't pack multiple Locust workers in a pod with `--processes`; gevent + parquet decode + native encode is already CPU-bound at 1.5 cores.
- Workers do not write stats files. CSV / logs are master-only.

## Monitoring

Create a [Databricks SQL dashboard](https://docs.databricks.com/aws/en/dashboards/) against the target table. Each ingested row carries a `client_ts_ms` stamped by the worker at encode time, so server-side throughput is an aggregate over that column.

Dashboard parameters: `table_name` (string), `time_range` (time range picker).

### Throughput — rows/sec and MB/sec, 5-second buckets

```sql
WITH buckets AS (
  SELECT
    FLOOR(client_ts_ms / 5000) * 5 AS bucket_secs,
    COUNT(*)                / 5.0       AS avg_rps,
    COUNT(*) * 1171.0 / 1048576.0 / 5.0 AS avg_mbps   -- 1171 ≈ avg proto2 wire bytes per NEOWISE row
  FROM IDENTIFIER(:table_name)
  WHERE client_ts_ms BETWEEN unix_millis(:time_range.min) AND unix_millis(:time_range.max)
  GROUP BY bucket_secs
)
SELECT TO_TIMESTAMP(bucket_secs) AS ts, avg_rps, avg_mbps
FROM buckets
ORDER BY ts
```

## Performance notes

Optimizations to increase per-worker throughput:

- Native C row encoder — faster than the Python `proto.SerializeToString` path.
- Bundled S3 file index — avoids `ListObjectsV2` rate limiting at high concurrency.
- Stride-based file partitioning — workers compute their slice; no master coordination.
- Locust heartbeat intervals stretched 10× — defaults overwhelm the master at 2k+ workers.
- Cached Locust `default_args_dict()` — defaults re-evaluated once per CLI arg on every spawn message.
- gRPC + gevent cooperation initialized at import time.
- Staggered worker startup so S3 connections don't open in the same millisecond.
- Deferred slow init (S3, downloads, stream creation) moved from `__init__` into `on_start`.

## How worker-to-file assignment works

For each year in `NEOWISE_YEARS`:
1. Flatten all 12 HEALPix-k0 sublists (sorted by k0) into a single ordered list of ~12,288 keys.
2. Each worker takes every `TOTAL_WORKERS`-th key starting from its `worker_idx`, capped at `FILES_PER_K0 * 12` keys.
3. Once the worker has streamed its slice of one year, it advances to the next.
4. When all years are exhausted the worker goes idle (no wrap-around — re-streaming the same rows would skew the benchmark).

Concrete examples:
- `TOTAL_WORKERS=2048`, `FILES_PER_K0=1` → 6 unique files per worker per year, every leaf owned by exactly one worker (recommended).
- `TOTAL_WORKERS=512`, `FILES_PER_K0=2` → 24 files per worker per year.
- `TOTAL_WORKERS=3072`, `FILES_PER_K0=1` → 4 unique files per worker per year.

## Locust request events

The workload emits three Locust request types so you can plot them in the web UI / CSV exports:

- `grpc/submit` — every successful `ingest_record_nowait` call (response time is the submit latency in ms)
- `grpc/failure` — failed submits (triggers stream recreation)
- `year-progress/<year>` — fired every 30s per worker; `response_time` is the files-done count, `response_length` is the year's total file count. Aggregated across the fleet, this shows year-by-year progress.

## Rebuilding the bundled file list

If the upstream dataset grows (a new NEOWISE year is added), regenerate `neowise/file_list.txt.gz`:

```bash
cd neowise
: > /tmp/file_list.txt
for y in year1 year2 year3 year4 year5 year6 year7 year8 year9 year10 year11 addendum; do
  for k0 in 0 1 2 3 4 5 6 7 8 9 10 11; do
    aws s3 ls --no-sign-request --recursive \
      "s3://nasa-irsa-wise/wise/neowiser/catalogs/p1bs_psd/healpix_k5/$y/neowiser-healpix_k5-$y.parquet/healpix_k0=$k0/" \
      | awk '/part0\.snappy\.parquet$/ {print $4}' >> /tmp/file_list.txt
  done
done
sort -u /tmp/file_list.txt -o /tmp/file_list.txt
gzip -9 -c /tmp/file_list.txt > file_list.txt.gz
```
