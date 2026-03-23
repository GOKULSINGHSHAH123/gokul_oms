"""
client_service.py
-----------------
CRUD for per-client RMS parameters.

Responsibilities:
  1. MongoDB CRUD for the clinet_rms_params collection (db: rms)
  2. build_cache() — reads all client RMS entries from MongoDB and writes
     one Redis hash into DB 12:
       - client_rms : "{clientId}:{algoName}" → JSON per-client RMS params

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
    return get_mongo()[Databases.RMS][Collections.CLIENT_RMS]


def _get_broker_map() -> dict:
    """Return {client_id: broker} from client_creds collection."""
    db = get_mongo()[Databases.RMS]
    docs = db["client_rms_params"].find({}, {"_id": 0, "client_id": 1, "broker": 1})
    return {d["client_id"]: d.get("broker", "") for d in docs if "client_id" in d}

def get_client_rms(client_id: str, algo_name: str) -> Optional[dict]:
    """Fetch a single client RMS entry by client_id + algo_name."""
    return _get_collection().find_one(
        {"client_id": client_id, "algo_name": algo_name}, {"_id": 0}
    )


def create_client_rms(doc: dict) -> str:
    """Insert a new client RMS document. Returns composite key."""
    _get_collection().insert_one(doc)
    key = f"{doc.get('client_id')}:{doc.get('algo_name')}"
    logger.info("Created client_rms: %s", key)
    return key


def update_client_rms(client_id: str, algo_name: str, updates: dict) -> bool:
    """Update fields on an existing client RMS entry."""
    result = _get_collection().update_one(
        {"client_id": client_id, "algo_name": algo_name},
        {"$set": updates},
    )
    if result.modified_count:
        logger.info("Updated client_rms: %s:%s fields=%s", client_id, algo_name, list(updates.keys()))
        return True
    return False


def list_client_rms() -> list:
    """List client RMS documents where is_active=True and temp=True."""
    return list(_get_collection().find({"is_active": True, "is_temp": True}, {"_id": 0}))


# ──────────────────────────────────────────────────────────────────────────────
# Cache builder — called by sync.py every 2 seconds
# ──────────────────────────────────────────────────────────────────────────────

def build_cache(redis_cfg=None) -> None:
    """
    Read all client RMS entries from MongoDB and write to Redis DB 12.

    Writes:
      - client_rms  Hash: "{client_id}:{algo_name}" → JSON of per-client RMS params
      - algo_clients Hash: algo_name → JSON array of client_ids

    Uses a Redis pipeline for atomic update.
    """
    if redis_cfg is None:
        redis_cfg = get_redis(RedisDB.CONFIG_CACHE)

    entries = list_client_rms()

    if not entries:
        logger.warning("build_cache: no client_rms entries found in MongoDB — DB 12 not updated")
        return

    broker_map = _get_broker_map()
    print(broker_map)

    # Build algo_clients mapping (algo_name → [client_id, ...])
    algo_clients: dict = {}
    for entry in entries:
        algo_name = entry.get("algo_name")
        client_id = entry.get("client_id")
        if algo_name and client_id:
            algo_clients.setdefault(algo_name, []).append(client_id)

    pipe = redis_cfg.pipeline()
    pipe.delete("client_rms")
    pipe.delete("algo_clients")

    for entry in entries:
        client_id = entry.get("client_id")
        algo_name = entry.get("algo_name")
        if not client_id or not algo_name:
            continue

        rms_fields = {
            "quantity_multiplier": entry.get("quantity_multiplier", 1.0),
            "broker": broker_map.get(client_id, ""),
        }
        pipe.hset("client_rms", f"{client_id}:{algo_name}", json.dumps(rms_fields))

    for algo_name, client_ids in algo_clients.items():
        pipe.hset("algo_clients", algo_name, json.dumps(client_ids))

    pipe.execute()
    logger.debug("build_cache: synced %d client_rms entries to DB 12", len(entries))
