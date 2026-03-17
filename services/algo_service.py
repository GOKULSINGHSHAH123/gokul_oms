"""
algo_service.py
---------------
CRUD for algo RMS configurations.

Responsibilities:
  1. MongoDB CRUD for the algo_config collection
  2. build_cache() — reads all active algos from MongoDB and writes
     two Redis hashes into DB 12:
       - algo_configs  : algo_name → JSON RMS params
       - algo_clients  : algo_name → JSON array of client internal_ids

Called by sync.py every 2 seconds.
"""

import json
from typing import Optional
from utils.db_helpers import get_mongo, get_redis, RedisDB
from utils.schema import Collections, Databases
from utils.setup_logging import get_logger

logger = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# MongoDB CRUD helpers
# ──────────────────────────────────────────────────────────────────────────────

def _get_collection():
    return get_mongo()[Databases.RMS][Collections.ALGO_CONFIG]


def get_algo(algo_name: str) -> Optional[dict]:
    """Fetch a single algo config document by name."""
    return _get_collection().find_one({"algo_name": algo_name}, {"_id": 0})


def create_algo(algo_doc: dict) -> str:
    """
    Insert a new algo config document.
    Returns the inserted document's algo_name.
    """
    _get_collection().insert_one(algo_doc)
    logger.info("Created algo config: %s", algo_doc.get("algo_name"))
    return algo_doc["algo_name"]


def update_algo(algo_name: str, updates: dict) -> bool:
    """
    Update fields on an existing algo config.
    Returns True if a document was modified.
    """
    result = _get_collection().update_one(
        {"algo_name": algo_name},
        {"$set": updates},
    )
    if result.modified_count:
        logger.info("Updated algo config: %s fields=%s", algo_name, list(updates.keys()))
        return True
    return False


def delete_algo(algo_name: str) -> bool:
    """Soft-delete by setting is_active=False (never hard-delete algo config)."""
    return update_algo(algo_name, {"is_active": False})


def list_algos(active_only: bool = True) -> list:
    """List all algo configs (optionally filter to active only)."""
    query = {"is_active": True} if active_only else {}
    return list(_get_collection().find(query, {"_id": 0}))


# ──────────────────────────────────────────────────────────────────────────────
# Cache builder — called by sync.py every 2 seconds
# ──────────────────────────────────────────────────────────────────────────────

def build_cache(redis_cfg=None) -> None:
    """
    Read all algo configs from MongoDB and write to Redis DB 12.

    Writes:
      - algo_configs Hash: algo_name → JSON of RMS params

    Uses a Redis pipeline for atomic multi-key write.
    """
    if redis_cfg is None:
        redis_cfg = get_redis(RedisDB.CONFIG_CACHE)

    algos = list_algos(active_only=False)  # sync all, including inactive

    if not algos:
        logger.warning("build_cache: no algo configs found in MongoDB — DB 12 not updated")
        return

    pipe = redis_cfg.pipeline()
    pipe.delete("algo_configs")

    for algo in algos:
        algo_name = algo.get("algo_name")
        if not algo_name:
            continue

        rms_fields = {
            "status":                    algo.get("is_active", False),
            "trade_limit_per_second":    algo.get("max_trade_limit_per_sec", 0),
            "trade_limit_per_day":       algo.get("max_trade_limit_per_day", 0),
            "allowed_segments":          algo.get("allowed_segments", []),
            "lot_size":                  algo.get("lot_size", 1),
            "lot_size_multiple":         algo.get("lot_size_multiple", 1),
            "per_trade_lot_limit":       algo.get("max_order_lot_qty", 0),
            "daily_lot_limit":           algo.get("max_net_lot_qty", 0),
            "max_value_of_symbol_check": algo.get("max_order_value", 0),
        }
        pipe.hset("algo_configs", algo_name, json.dumps(rms_fields))

    pipe.execute()
    logger.debug("build_cache: synced %d algo configs to DB 12", len(algos))
