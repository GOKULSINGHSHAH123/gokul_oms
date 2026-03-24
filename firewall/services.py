"""
services.py
-----------
MongoDB query helpers for the firewall pipeline.
Consolidates only the read functions needed by firewall:
  - list_algos
  - list_segments
  - list_client_rms
  - _get_broker_map
"""

from firewall.db_helpers import get_mongo
from firewall.schema import Collections, Databases
from firewall.setup_logging import get_logger

logger = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Algo configs
# ──────────────────────────────────────────────────────────────────────────────

def list_algos(active_only: bool = True) -> list:
    """List all algo configs (optionally filter to active only)."""
    col = get_mongo()[Databases.RMS][Collections.ALGO_CONFIG]
    query = {"is_active": True} if active_only else {}
    return list(col.find(query, {"_id": 0}))


# ──────────────────────────────────────────────────────────────────────────────
# Segment configs
# ──────────────────────────────────────────────────────────────────────────────

def list_segments() -> list:
    """List all segment configs."""
    col = get_mongo()[Databases.RMS][Collections.SEGMENT_CONFIG]
    return list(col.find({}, {"_id": 0}))


# ──────────────────────────────────────────────────────────────────────────────
# Client RMS
# ──────────────────────────────────────────────────────────────────────────────

def _get_broker_map() -> dict:
    """Return {client_id: broker} from client_rms_params collection."""
    db = get_mongo()[Databases.RMS]
    docs = db[Collections.CLIENT_RMS].find({}, {"_id": 0, "client_id": 1, "broker": 1})
    return {d["client_id"]: d.get("broker", "") for d in docs if "client_id" in d}


def list_client_rms() -> list:
    """List client RMS documents where is_active=True and is_temp=True."""
    col = get_mongo()[Databases.RMS][Collections.CLIENT_RMS]
    return list(col.find({"is_active": True, "is_temp": True}, {"_id": 0}))
