"""
sync.py
-------
Background config sync process.

Runs on a 2-second loop. Reads all RMS configuration from MongoDB and
writes it into Redis DB 12 (config cache). This is the ONLY writer to DB 12.

The pipeline stages (algo_signal_check, client_signal_check, order_executer)
are PURELY READERS of DB 12 — they never write configuration.

Design rationale (LLD §11):
  - Zero MongoDB reads at pipeline runtime
  - Config changes propagate within 2 seconds
  - DB 12 can be FLUSHDB'd safely — sync repopulates within 2 seconds
  - DB 13 (live streams) is completely unaffected by a DB 12 flush

Builds four caches:
  - algo_configs       Hash: algo_name → JSON RMS params
  - algo_clients       Hash: algo_name → JSON array of client internal_ids
  - client_rms         Hash: internal_id:algo → JSON per-client params
  - segment_rms_check  Hash: segment_name → JSON OI + spread thresholds
"""

import time
from services.algo_service import build_cache as algo_build_cache
from services.segment_service import make_cache_in_redis as segment_make_cache
from services.client_service import build_cache as client_build_cache
from utils.db_helpers import get_redis, RedisDB
from utils.setup_logging import get_logger

logger = get_logger(__name__)

SYNC_INTERVAL_SEC = 2   # loop sleep interval


def run() -> None:
    """
    Main sync loop. Runs forever until KeyboardInterrupt.
    Errors in a single cycle are logged but do not stop the loop.
    """
    redis_cfg = get_redis(RedisDB.CONFIG_CACHE)

    logger.info("sync.py started — refreshing Redis DB 12 every %ds", SYNC_INTERVAL_SEC)

    while True:
        try:
            cycle_start = time.monotonic()

            # Sync algo configs and client mappings
            algo_build_cache(redis_cfg)

            # Sync per-client RMS params
            client_build_cache(redis_cfg)

            # Sync segment RMS rules
            segment_make_cache(redis_cfg)

            elapsed_ms = int((time.monotonic() - cycle_start) * 1000)
            logger.debug("Sync cycle complete in %dms", elapsed_ms)

        except KeyboardInterrupt:
            logger.info("sync.py shutting down.")
            break
        except Exception as exc:
            logger.exception("Sync cycle failed: %s", exc)

        time.sleep(SYNC_INTERVAL_SEC)


if __name__ == "__main__":
    run()
