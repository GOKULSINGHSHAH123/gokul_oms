"""
order_executer.py
-----------------
Reads order_request:<internal_id> from Redis DB 14 and executes each order
via HTTP POST to the broker API.

Responsibilities:
  - Sliding-window rate limiter: max 10 orders per second per client
  - HTTP POST to broker with Authorization header
  - On HTTP 200: writes record to MongoDB algo_signals_{date} collection
    (the PRIMARY HANDOFF collection read by Response Handler / Gyan)
  - On failure: pushes algo_execution_error to Error_Stream (DB 13)
  - Checkpoint: saves last processed message ID after every message
    (allows crash recovery without reprocessing)

DB reads  : DB 14 (order_request:<internal_id>)
DB writes : DB 13 (Error_Stream on failure), MongoDB algo_signals_{date} on success
"""

import json
import os
import sys
import time
import datetime
import collections
import requests
from utils.db_helpers import get_redis, RedisDB, get_mongo
from utils.schema import algo_signals_collection
from utils.setup_logging import get_logger

logger = get_logger(__name__)

ORDER_REQUEST_PREFIX  = "order_request:"
ERROR_STREAM          = "Error_Stream"
CONSUMER_GROUP        = "order_executer_group"

RATE_LIMIT_PER_SECOND = 10        # max orders per second per client
RATE_WINDOW_MS        = 1000      # rolling window in milliseconds
BROKER_TIMEOUT_SEC    = 5         # HTTP request timeout

BROKER_BASE_URL: str = os.getenv("BROKER_BASE_URL", "https://api.broker.example.com")
BROKER_ORDER_PATH: str = os.getenv("BROKER_ORDER_PATH", "/orders/place")


# ──────────────────────────────────────────────────────────────────────────────
# Sliding window rate limiter
# ──────────────────────────────────────────────────────────────────────────────

class SlidingWindowRateLimiter:
    """
    Maintains a deque of order timestamps.
    Before each HTTP POST, blocks until the oldest timestamp in the window
    is more than 1 second old (if at or above the limit).
    """

    def __init__(self, limit: int = RATE_LIMIT_PER_SECOND, window_ms: int = RATE_WINDOW_MS):
        self._limit     = limit
        self._window_ms = window_ms
        self._timestamps: collections.deque = collections.deque()

    def acquire(self) -> None:
        """Block until a slot is available within the rate limit window."""
        while True:
            now_ms = int(time.time() * 1000)
            # Evict timestamps outside the window
            while self._timestamps and (now_ms - self._timestamps[0]) >= self._window_ms:
                self._timestamps.popleft()

            if len(self._timestamps) < self._limit:
                self._timestamps.append(now_ms)
                return

            # Wait until the oldest slot expires
            oldest = self._timestamps[0]
            sleep_ms = self._window_ms - (now_ms - oldest) + 1
            logger.debug("Rate limit hit — sleeping %dms", sleep_ms)
            time.sleep(sleep_ms / 1000.0)


# ──────────────────────────────────────────────────────────────────────────────
# MongoDB write — PRIMARY HANDOFF to Response Handler (Gyan)
# ──────────────────────────────────────────────────────────────────────────────

def _write_to_mongo(payload: dict, broker_response: dict) -> None:
    """
    Write the order document to algo_signals_{date} BEFORE returning to the caller.

    CRITICAL: This collection is the PRIMARY HANDOFF point between OMS (Gokul)
    and the Response Handler (Gyan). Gyan's response_log.py does a find_one
    lookup by orderUniqueIdentifier on every broker WebSocket event.

    Race condition rule (from shared_dependencies.md §2):
    If the broker response arrives before this document is committed, Gyan
    retries for up to 120 seconds before dropping the event.
    Therefore: always write BEFORE (or immediately after) the HTTP POST,
    never async.

    Fields written here must match the contract in shared_dependencies.md §3.1.
    """
    mongo = get_mongo()
    collection_name = algo_signals_collection()
    col = mongo["algo_signals"][collection_name]

    document = {
        "orderUniqueIdentifier": payload["orderUniqueIdentifier"],
        "algoName":              payload["algoName"],
        "clientID":              payload["clientID"],
        "exchangeInstrumentID":  int(payload["exchangeInstrumentID"]),
        "exchangeSegment":       payload["exchangeSegment"],
        "orderSide":             payload["orderSide"],
        "orderQuantity":         int(payload["orderQuantity"]),
        "limitPrice":            float(payload["limitPrice"]),
        "orderType":             payload["orderType"],
        "productType":           payload["productType"],
        "appOrderId":            broker_response.get("AppOrderID", ""),
        "responseFlag":          False,   # Gyan sets True when fill/cancel/reject arrives
        "leaves_quantity":       int(payload["orderQuantity"]),  # Gyan updates on partial fill
        "mod_status":            "Placed",
        "placed_at":             datetime.datetime.utcnow().isoformat(),
    }

    # Use upsert to handle rare duplicate inserts (idempotent)
    col.update_one(
        {"orderUniqueIdentifier": payload["orderUniqueIdentifier"]},
        {"$set": document},
        upsert=True,
    )
    logger.info(
        "MongoDB write OK: orderID=%s appOrderID=%s",
        payload["orderUniqueIdentifier"],
        broker_response.get("AppOrderID", ""),
    )


# ──────────────────────────────────────────────────────────────────────────────
# HTTP execution
# ──────────────────────────────────────────────────────────────────────────────

def _place_order(payload: dict) -> dict:
    """
    Execute HTTP POST to the broker.
    Returns parsed JSON response on HTTP 200.
    Raises on any HTTP error or network exception.
    """
    url = f"{BROKER_BASE_URL}{BROKER_ORDER_PATH}"
    headers = {
        "Authorization": f"Bearer {payload['authToken']}",
        "Content-Type":  "application/json",
    }

    # Strip auth token from the payload body (it is only in the header)
    body = {k: v for k, v in payload.items() if k != "authToken"}

    response = requests.post(url, json=body, headers=headers, timeout=BROKER_TIMEOUT_SEC)
    response.raise_for_status()   # raises HTTPError on 4xx / 5xx
    return response.json()


# ──────────────────────────────────────────────────────────────────────────────
# Error helper
# ──────────────────────────────────────────────────────────────────────────────

def _push_error(redis_streams, payload: dict, reason: str) -> None:
    event = {
        "error_type": "algo_execution_error",
        "stage":      "order_executer",
        "reason":     reason,
        "signal":     json.dumps(payload),
        "client_id":  str(payload.get("clientID", "")),
        "timestamp":  int(time.time() * 1000),
    }
    redis_streams.xadd(ERROR_STREAM, event)
    logger.warning("Execution error pushed to %s: %s", ERROR_STREAM, reason)


# ──────────────────────────────────────────────────────────────────────────────
# Main consumer loop
# ──────────────────────────────────────────────────────────────────────────────

def run(client_id: str) -> None:
    """
    Blocking consumer for order_request:<client_id> (DB 14).
    One instance per client.

    Args:
        client_id: Internal client ID whose order_request stream to consume.
    """
    redis_throttler = get_redis(RedisDB.THROTTLER)
    redis_streams   = get_redis(RedisDB.LIVE_STREAMS)

    stream_key    = f"{ORDER_REQUEST_PREFIX}{client_id}"
    consumer_name = f"order_executer_{client_id}"
    rate_limiter  = SlidingWindowRateLimiter()

    # Ensure consumer group
    try:
        redis_throttler.xgroup_create(stream_key, CONSUMER_GROUP, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    logger.info("order_executer started for client=%s stream=%s", client_id, stream_key)

    while True:
        try:
            messages = redis_throttler.xreadgroup(
                CONSUMER_GROUP,
                consumer_name,
                {stream_key: ">"},
                count=1,         # one at a time — rate limiter is per-message
                block=2000,
            )

            if not messages:
                continue

            for _stream, entries in messages:
                for msg_id, fields in entries:
                    payload = {}
                    try:
                        payload = json.loads(fields.get("payload", "{}"))

                        # Enforce rate limit BEFORE HTTP call
                        rate_limiter.acquire()

                        # Execute HTTP POST
                        broker_response = _place_order(payload)

                        # Write to MongoDB (handoff to Gyan)
                        _write_to_mongo(payload, broker_response)

                        logger.info(
                            "Order placed OK: client=%s orderID=%s",
                            client_id, payload.get("orderUniqueIdentifier"),
                        )

                    except Exception as exc:
                        logger.error(
                            "Order execution FAILED: client=%s orderID=%s reason=%s",
                            client_id, payload.get("orderUniqueIdentifier", ""), exc,
                        )
                        _push_error(redis_streams, payload, str(exc))

                    finally:
                        # Checkpoint — always ACK so message is not reprocessed
                        redis_throttler.xack(stream_key, CONSUMER_GROUP, msg_id)

        except KeyboardInterrupt:
            logger.info("Shutting down order_executer for client=%s", client_id)
            break
        except Exception as exc:
            logger.exception("Unexpected error in order_executer loop: %s", exc)
            time.sleep(1)


if __name__ == "__main__":
    _client_id = os.getenv("CLIENT_ID") or (sys.argv[1] if len(sys.argv) > 1 else None)
    if not _client_id:
        print("Usage: python order_executer.py <client_id>  OR  CLIENT_ID=<id> python ...")
        sys.exit(1)
    run(_client_id)
