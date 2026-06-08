"""NEOWISE-R Single-exposure Source Table load test for Databricks Zerobus.

Streams the public NASA/IRSA NEOWISE-R parquet dataset into a Zerobus-ingested
Unity Catalog table. See README.md for setup, env vars, and run instructions.

Run with:
    locust -f neowise_loadtest.py --headless -u <users> -r <spawn-rate> -t <time>

See `neowise/`:
  - config.py         env-var-driven configuration
  - locust_tuning.py  monkey-patches for high-worker-count runs (heartbeat
                      stretching, argparser cache, grpc+gevent init)
  - dataset.py        S3 listing, bundled index, per-worker file planning
  - user.py           the `NeowiseUser` Locust class
"""

# locust_tuning patches Locust internals at import time. Import it FIRST,
# before any other Locust submodule is loaded, so the patches are in place
# when Locust spins up its argparser, runners, and gRPC channels.
from neowise import locust_tuning  # noqa: F401 — imported for side effects
from neowise.user import NeowiseUser  # noqa: F401 — Locust picks this up via `-f`
