"""
error_handler.py
----------------
Central failure collector for the entire pipeline.

Consumes Error_Stream (DB 13) and persists all rejections to MongoDB.
This is the ONLY writer of rejection records to MongoDB.
All pipeline stages only write to Error_Stream — they never touch MongoDB directly.
This keeps MongoDB writes off the hot path.

Error types handled:
  - algo_signal_validation_error  (Stages 1, 2, 3)
  - client_signal_validation_error (Stage 4 / client_signal_check)
  - algo_execution_error           (order_executer)

DB reads  : DB 13 (Error_Stream)
DB writes : MongoDB (symphonyorder_raw + final_response_db) — both collections
"""

import json
import time
import datetime
from utils.db_helpers import get_redis, RedisDB, get_mongo
from utils.setup_logging import get_logger

logger = get_logger(__name__)

ERROR_STREAM   = "Error_Stream"
CONSUMER_GROUP = "error_handler_group"
CONSUMER_NAME  = "error_handler_worker"

# MongoDB collection names (legacy names kept per LLD §10)
SYMPHONY_ORDER_RAW = "symphonyorder_raw"
FINAL_RESPONSE_COL = "final_response"


# ──────────────────────────────────────────────────────────────────────────────
# Error classification
# ──────────────────────────────────────────────────────────────────────────────

ERROR_TYPE_MAP = {
    "algo_signal_validation_error":   "Rejected",
    "client_signal_validation_error": "Rejected",
    "algo_execution_error":           "Rejected",
}


def classify_error(error_type: str) -> str:
    """Return the mod_status / orderStatus to write to MongoDB."""
    return ERROR_TYPE_MAP.get(error_type, "Rejected")


# ──────────────────────────────────────────────────────────────────────────────
# MongoDB persistence
# ──────────────────────────────────────────────────────────────────────────────

def _persist_to_mongo(error_event: dict) -> None:
    """
    Write rejection records to two MongoDB collections:
      1. symphonyorder_raw  — sets mod_status = Rejected
      2. final_response_db  — sets orderStatus = Rejected

    Both writes are required per LLD §10 error type table.
    """
    mongo = get_mongo()
    signal_raw = error_event.get("signal", "{}")
    try:
        signal = json.loads(signal_raw) if isinstance(signal_raw, str) else signal_raw
    except json.JSONDecodeError:
        signal = {}

    error_type  = error_event.get("error_type", "unknown")
    order_status = classify_error(error_type)
    order_id    = signal.get("orderUniqueIdentifier", error_event.get("client_id", "unknown"))
    client_id   = signal.get("clientID", error_event.get("client_id", ""))
    algo_name   = signal.get("algoName", "")
    reason      = error_event.get("reason", "")
    stage       = error_event.get("stage", "")
    now         = datetime.datetime.utcnow().isoformat()

    # 1. Write to symphonyorder_raw (per LLD §10)
    symphony_doc = {
        "orderUniqueIdentifier": order_id,
        "clientID":              client_id,
        "algoName":              algo_name,
        "exchangeInstrumentID":  signal.get("exchangeInstrumentID"),
        "exchangeSegment":       signal.get("exchangeSegment"),
        "orderSide":             signal.get("orderSide"),
        "orderQuantity":         signal.get("orderQuantity"),
        "limitPrice":            signal.get("limitPrice"),
        "orderType":             signal.get("orderType"),
        "productType":           signal.get("productType"),
        "mod_status":            order_status,
        "error_type":            error_type,
        "error_stage":           stage,
        "error_reason":          reason,
        "rejected_at":           now,
        "responseFlag":          False,
    }
    mongo["final_response_db"][SYMPHONY_ORDER_RAW].update_one(
        {"orderUniqueIdentifier": order_id},
        {"$set": symphony_doc},
        upsert=True,
    )

    # 2. Write summary to final_response_db (per LLD §10)
    final_doc = {
        "orderUniqueIdentifier": order_id,
        "clientID":              client_id,
        "algoName":              algo_name,
        "orderStatus":           order_status,
        "error_type":            error_type,
        "error_stage":           stage,
        "error_reason":          reason,
        "rejected_at":           now,
    }
    mongo["final_response_db"][FINAL_RESPONSE_COL].update_one(
        {"orderUniqueIdentifier": order_id},
        {"$set": final_doc},
        upsert=True,
    )

    logger.info(
        "Rejection persisted: orderID=%s type=%s stage=%s reason=%.80s",
        order_id, error_type, stage, reason,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Consumer loop
# ──────────────────────────────────────────────────────────────────────────────

def run() -> None:
    """
    Blocking consumer loop.
    Reads Error_Stream (DB 13) via consumer group for at-least-once delivery.
    """
    redis_streams = get_redis(RedisDB.LIVE_STREAMS)

    # Ensure consumer group
    try:
        redis_streams.xgroup_create(ERROR_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        logger.info("Consumer group '%s' created on '%s'", CONSUMER_GROUP, ERROR_STREAM)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    logger.info("error_handler started — consuming '%s'", ERROR_STREAM)

    while True:
        try:
            messages = redis_streams.xreadgroup(
                CONSUMER_GROUP,
                CONSUMER_NAME,
                {ERROR_STREAM: ">"},
                count=20,
                block=2000,
            )

            if not messages:
                continue

            for _stream, entries in messages:
                for msg_id, fields in entries:
                    try:
                        _persist_to_mongo(dict(fields))
                    except Exception as exc:
                        logger.exception("Failed to persist error msg_id=%s: %s", msg_id, exc)
                        # Do NOT ACK — let it be retried
                        continue
                    finally:
                        redis_streams.xack(ERROR_STREAM, CONSUMER_GROUP, msg_id)

        except KeyboardInterrupt:
            logger.info("Shutting down error_handler.")
            break
        except Exception as exc:
            logger.exception("Unexpected error in error_handler loop: %s", exc)
            time.sleep(1)


if __name__ == "__main__":
    run()
