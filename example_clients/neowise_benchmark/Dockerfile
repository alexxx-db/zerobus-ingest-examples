# NEOWISE benchmark — Locust image that ingests the public NEOWISE-R
# parquet dataset into a Databricks Zerobus-ingested Unity Catalog table.
#
# Build:
#   docker build -t zerobus-neowise-benchmark .
#
# Run (single-process, headless):
#   docker run --rm \
#     -e ZEROBUS_SERVER_ENDPOINT=https://<shard>.zerobus.<region>.cloud.databricks.com \
#     -e DATABRICKS_WORKSPACE_URL=https://<workspace>.cloud.databricks.com \
#     -e ZEROBUS_TABLE_NAME=<catalog>.<schema>.<table> \
#     -e DATABRICKS_CLIENT_ID=<oauth-client-id> \
#     -e DATABRICKS_CLIENT_SECRET=<oauth-client-secret> \
#     zerobus-neowise-benchmark \
#     --headless -u 100 -r 10 -t 5m
#
# See README.md for distributed / Kubernetes deployment.

FROM python:3.12-slim

# Build deps for the native C row encoder (and pyarrow's wheel only needs libstdc++,
# but we install build-essential because we always compile neowise_native ourselves).
RUN apt-get update -qq \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Optional: point pip at a custom index (corporate mirror, devpi, etc.).
# Default is empty -> pip uses pypi.org.
ARG PIP_INDEX_URL=
ENV PIP_INDEX_URL=${PIP_INDEX_URL}

# Install Python deps first so the layer is cacheable.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Project source.
COPY neowise/ ./neowise/
COPY setup.py neowise_loadtest.py ./

# Build the neowise.neowise_native C extension in-place (drops the .so under
# neowise/). This is the only build step — the Python files are imported as-is.
RUN python setup.py build_ext --inplace \
 && rm -rf build/

# Local parquet cache (sized to FILES_PER_K0 * 12 * ~200 MB per year + headroom).
# Mount a real volume in production runs; on Kubernetes use an emptyDir or PVC.
RUN mkdir -p /tmp/neowise
ENV DATA_DIR=/tmp/neowise

# Unbuffered stdout so logs stream out of `docker logs` / `kubectl logs`.
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["locust", "-f", "neowise_loadtest.py"]
CMD ["--help"]
