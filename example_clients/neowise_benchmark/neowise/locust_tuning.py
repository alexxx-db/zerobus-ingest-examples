"""Locust monkey-patches for high-worker-count runs.

This module is imported for its SIDE EFFECTS — it patches Locust internals at
import time and must be imported before any other Locust submodule is
exercised. The entrypoint (`neowise_loadtest.py`) imports it first.

Three things happen here:

1. `locust.argument_parser.default_args_dict` is cached.

   In `locust/runners.py` (worker spawn handler), Locust does:

       custom_args_from_master = {
           k: v
           for k, v in job["parsed_options"].items()
           if k not in argument_parser.default_args_dict() ...
       }

   The right operand of `not in` is re-evaluated for every iteration, so
   `default_args_dict()` is called once PER argument in the spawn message.
   Each call rebuilds the entire argparser (~75 `add_argument` calls in
   Locust 2.44). At high worker counts this is a measurable fraction of
   spawn-time CPU; the defaults never change after startup so caching the
   first result is safe.

2. `grpc.experimental.gevent.init_gevent()` is called.

   Patches gRPC's C-core to cooperate with gevent's monkey-patched sockets.
   Without this, gRPC calls block the event loop and Locust heartbeats miss
   their deadlines.

3. Locust's heartbeat MODULE-LEVEL constants are stretched ~10x.

   At 2,000+ workers the master can't service all the default 1 Hz
   heartbeats and starts marking workers missing, which makes them
   self-quit ("Test done. Flushing"). The heartbeat loops in
   `locust/runners.py` read these names as module-level constants — e.g.

       gevent.sleep(HEARTBEAT_INTERVAL)
       if client.heartbeat <= HEARTBEAT_DEAD_INTERNAL: ...
       if last < time.time() - MASTER_HEARTBEAT_TIMEOUT: ...
       gevent.sleep(WORKER_REPORT_INTERVAL)

   so rebinding the module attribute is the supported knob. The
   `@events.init` listener below also mirrors the values onto the runner
   instance for completeness (e.g. `WorkerNode.__init__` reads
   `HEARTBEAT_LIVENESS` once at construction).

   Defaults in Locust 2.44 (verified against installed source):
     HEARTBEAT_INTERVAL       = 1     -> 10
     HEARTBEAT_LIVENESS       = 3     -> 60
     HEARTBEAT_DEAD_INTERNAL  = -60   -> -600
     MASTER_HEARTBEAT_TIMEOUT = 60    -> 600
     WORKER_REPORT_INTERVAL   = 3.0   -> 10
"""

# ── 1. argparser default-dict cache ─────────────────────────────────────────
import locust.argument_parser as _ap

_default_args_cache = None
_orig_default_args_dict = _ap.default_args_dict


def _cached_default_args_dict():
    global _default_args_cache
    if _default_args_cache is None:
        _default_args_cache = _orig_default_args_dict()
    return _default_args_cache


_ap.default_args_dict = _cached_default_args_dict


# ── 2. gRPC + gevent cooperation ────────────────────────────────────────────
import grpc.experimental.gevent as _grpc_gevent  # noqa: E402

_grpc_gevent.init_gevent()


# ── 3. heartbeat interval stretching ────────────────────────────────────────
import locust.runners as _lr  # noqa: E402

_lr.HEARTBEAT_INTERVAL = 10
_lr.HEARTBEAT_LIVENESS = 60
_lr.HEARTBEAT_DEAD_INTERNAL = -600
_lr.MASTER_HEARTBEAT_TIMEOUT = 600
_lr.WORKER_REPORT_INTERVAL = 10


from locust import events  # noqa: E402


@events.init.add_listener
def _on_locust_init(environment, **kwargs):
    """Mirror the module-constant heartbeat tweaks on the runner instance."""
    environment.runner.heartbeat_interval = 10
    environment.runner.heartbeat_liveness = 60
    environment.runner.worker_heartbeat_timeout = 600
    environment.runner.master_heartbeat_timeout = 600
