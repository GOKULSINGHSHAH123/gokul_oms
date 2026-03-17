"""
segment_service.py
------------------
CRUD for segment RMS rules.

Responsibilities:
  1. MongoDB CRUD for the segment_config collection
  2. make_cache_in_redis() — reads all segment configs from MongoDB and
     writes one Redis hash into DB 12:
       - segment_rms_check : segment_name → JSON OI + spread thresholds

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
    return get_mongo()[Databases.RMS][Collections.SEGMENT_CONFIG]


def get_segment(segment_name: str) -> Optional[dict]:
    """Fetch a single segment config by name."""
    return _get_collection().find_one({"segment": segment_name}, {"_id": 0})


def create_segment(segment_doc: dict) -> str:
    """Insert a new segment RMS config document."""
    _get_collection().insert_one(segment_doc)
    logger.info("Created segment config: %s", segment_doc.get("segment"))
    return segment_doc["segment"]


def update_segment(segment_name: str, updates: dict) -> bool:
    """Update fields on an existing segment config."""
    result = _get_collection().update_one(
        {"segment": segment_name},
        {"$set": updates},
    )
    if result.modified_count:
        logger.info("Updated segment config: %s", segment_name)
        return True
    return False


def list_segments() -> list:
    """List all segment configs."""
    return list(_get_collection().find({}, {"_id": 0}))


# ──────────────────────────────────────────────────────────────────────────────
# Cache builder — called by sync.py every 2 seconds
# ──────────────────────────────────────────────────────────────────────────────

def make_cache_in_redis(redis_cfg=None) -> None:
    """
    Read all segment configs from MongoDB and write to Redis DB 12.

    Writes:
      - segment_rms_check Hash: segment_name → JSON of OI threshold + spread %

    Uses Redis pipeline for atomic update.
    """
    if redis_cfg is None:
        redis_cfg = get_redis(RedisDB.CONFIG_CACHE)

    segments = list_segments()

    if not segments:
        logger.warning("make_cache_in_redis: no segment configs found — DB 12 not updated")
        return

    pipe = redis_cfg.pipeline()
    pipe.delete("segment_rms_check")

    for seg in segments:
        seg_name = seg.get("segment")
        if not seg_name:
            continue

        rms_fields = {
            "oi_threshold":    seg.get("oi", 0),
            "max_spread_pct":  seg.get("spread", 100),
        }
        pipe.hset("segment_rms_check", seg_name, json.dumps(rms_fields))

    pipe.execute()
    logger.debug("make_cache_in_redis: synced %d segment configs to DB 12", len(segments))
