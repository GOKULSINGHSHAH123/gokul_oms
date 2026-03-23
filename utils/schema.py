"""
schema.py
---------
MongoDB collection name and database registry.
Single source of truth for all collection names used by the OMS pipeline.

Usage:
    from utils.schema import Collections, get_collection

    col = get_collection(Collections.ALGO_SIGNALS)
    col.insert_one(document)
"""

import datetime
from utils.db_helpers import get_mongo


# ──────────────────────────────────────────────────────────────────────────────
# Database Names
# ──────────────────────────────────────────────────────────────────────────────

class Databases:
    ALGO_SIGNALS    = "algo_signals"       # daily signal + handoff collection
    FINAL_RESPONSE  = "Info"               # order outcome summaries
    RMS             = "Info"                # algo / segment / client RMS config
    ERRORS          = "error_db"           # pipeline rejection records


# ──────────────────────────────────────────────────────────────────────────────
# Collection Name Helpers
# ──────────────────────────────────────────────────────────────────────────────

def algo_signals_collection(date: datetime.date = None) -> str:
    """
    Returns the daily algo_signals collection name.
    Pattern: algo_signals_{YYYY-MM-DD}

    Previously named symphonyorder_raw / orders_{date}.
    This is the PRIMARY HANDOFF collection between OMS (Gokul) and
    Response Handler (Gyan). See shared_dependencies.md §3.1.
    """
    if date is None:
        date = datetime.date.today()
    return f"algo_signals_{date.strftime('%Y-%m-%d')}"


# ──────────────────────────────────────────────────────────────────────────────
# Static Collection Names
# ──────────────────────────────────────────────────────────────────────────────

class Collections:
    # RMS config (written by algo_service / segment_service / client_service, read by sync.py)
    ALGO_CONFIG     = "algo_rms_params"
    SEGMENT_CONFIG  = "segment_rms_params"
    CLIENT_RMS      = "client_rms_params"    # note: intentional typo matches MongoDB collection name

    # Error persistence (written exclusively by error_handler.py)
    FINAL_RESPONSE  = "final_response"
    SYMPHONY_ORDER_RAW = "symphonyorder_raw"   # legacy name; kept for backward compat

    # Position / audit
    CUMULATIVE      = "cumulative_{date}"      # format at runtime


def get_cumulative_collection(date: datetime.date = None) -> str:
    if date is None:
        date = datetime.date.today()
    return f"cumulative_{date.strftime('%Y-%m-%d')}"


# ──────────────────────────────────────────────────────────────────────────────
# Collection Accessor
# ──────────────────────────────────────────────────────────────────────────────

def get_collection(collection_name: str, database_name: str = None):
    """
    Return a pymongo Collection object.

    Args:
        collection_name: Name of the collection (use helpers above).
        database_name:   Override database. Defaults are inferred from
                         collection name patterns.
    """
    client = get_mongo()

    if database_name:
        return client[database_name][collection_name]

    # Infer database from collection name
    if collection_name.startswith("algo_signals_"):
        return client[Databases.ALGO_SIGNALS][collection_name]
    elif collection_name in (Collections.FINAL_RESPONSE, Collections.SYMPHONY_ORDER_RAW):
        return client[Databases.FINAL_RESPONSE][collection_name]
    elif collection_name in (Collections.ALGO_CONFIG, Collections.SEGMENT_CONFIG, Collections.CLIENT_RMS):
        return client[Databases.RMS][collection_name]
    elif collection_name.startswith("cumulative_"):
        return client[Databases.FINAL_RESPONSE][collection_name]
    else:
        raise ValueError(f"Unknown collection '{collection_name}' — specify database_name explicitly.")
