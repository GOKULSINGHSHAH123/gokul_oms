"""
client_signal_check.py
----------------------
Per-client signal processor.

Reads each client_signal:<client_id> stream (DB 13), applies the quantity
multiplier, fetches the auth token, builds the broker payload, and pushes
to the order_request:<internal_id> throttler stream (DB 14).

One instance of this process runs per active client.
The client_id is passed via environment variable or CLI arg.

DB reads  : DB 12 (client_rms), DB 10 (apiMap)
DB writes : DB 14 (order_request:<internal_id>)
"""

import json
import os
import sys
import time
import uuid
from utils.db_helpers import get_redis, RedisDB
from utils.setup_logging import get_logger

logger = get_logger(__name__)

CLIENT_SIGNAL_PREFIX  = "client_signal:"
ORDER_REQUEST_PREFIX  = "order_request:"
ERROR_STREAM          = "Error_Stream"

CONSUMER_GROUP = "client_signal_processor"


def _get_client_rms(redis_cfg, client_id: str, algo_name: str) -> dict:
    rms_key = f"{client_id}:{algo_name}"
    raw = redis_cfg.hget("client_rms", rms_key)
    if not raw:
        raise RuntimeError(f"client_rms missing for key '{rms_key}'")
    return json.loads(raw)


def _get_auth_token(redis_auth, client_id: str) -> str:
    api_map_raw = redis_auth.get("apiMap")
    if not api_map_raw:
        raise RuntimeError("apiMap not found in DB 10 — session manager may be down")
    api_map = json.loads(api_map_raw)
    token = api_map.get(str(client_id))
    if not token:
        raise RuntimeError(f"No auth token in apiMap for client '{client_id}'")
    return token


def process_client_signal(payload: dict, redis_cfg, redis_auth, redis_throttler) -> None:
    """
    Full per-client processing for one signal payload.

    Steps:
      1. Fetch client RMS config (DB 12)
      2. Run client RMS check (passthrough — reserved)
      3. Apply quantity multiplier
      4. Fetch auth token (DB 10)
      5. Generate order ID
      6. Build broker payload
      7. Push to order_request stream (DB 14)
    """
    client_id  = payload["clientID"]
    algo_name  = payload["algoName"]

    # 1. Client RMS config
    client_rms = _get_client_rms(redis_cfg, client_id, algo_name)

    # 2. Client RMS check (passthrough — future rules go here)
    # e.g. daily loss limit check, max open positions

    # 3. Apply quantity multiplier
    multiplier   = float(client_rms.get("quantity_multiplier", 1.0))
    original_qty = int(float(payload["orderQuantity"]))
    new_qty      = int(original_qty * multiplier)
    if new_qty <= 0:
        raise RuntimeError(f"Computed qty {new_qty} <= 0 after multiplier {multiplier}")

    # 4. Auth token
    auth_token = _get_auth_token(redis_auth, client_id)

    # 5. Generate unique order ID
    order_unique_id = f"{algo_name}_{uuid.uuid4().hex}"

    # 6. Build broker payload
    broker_payload = {
        "orderUniqueIdentifier": order_unique_id,
        "algoName":              algo_name,
        "clientID":              str(client_id),
        "exchangeInstrumentID":  payload["exchangeInstrumentID"],
        "exchangeSegment":       payload["exchangeSegment"],
        "orderSide":             payload["orderSide"],
        "orderQuantity":         new_qty,
        "limitPrice":            payload["limitPrice"],
        "orderType":             payload["orderType"],
        "productType":           payload["productType"],
        "authToken":             auth_token,
        "timestamp":             int(time.time() * 1000),
    }

    # 7. Push to throttler (DB 14)
    throttler_key = f"{ORDER_REQUEST_PREFIX}{client_id}"
    redis_throttler.xadd(throttler_key, {"payload": json.dumps(broker_payload)})

    logger.info(
        "client_signal_check OK: client=%s algo=%s orderID=%s qty=%s",
        client_id, algo_name, order_unique_id, new_qty,
    )


def _push_error(redis_streams, error_type: str, payload: dict, reason: str) -> None:
    event = {
        "error_type": error_type,
        "stage":      "client_signal_check",
        "reason":     reason,
        "signal":     json.dumps(payload),
        "client_id":  str(payload.get("clientID", "")),
        "timestamp":  int(time.time() * 1000),
    }
    redis_streams.xadd(ERROR_STREAM, event)
    logger.warning("Error pushed to %s: %s", ERROR_STREAM, reason)


def run(client_id: str) -> None:
    """
    Blocking consumer loop for a single client's signal stream.

    Args:
        client_id: The client whose client_signal:<id> stream to consume.
    """
    redis_cfg       = get_redis(RedisDB.CONFIG_CACHE)
    redis_auth      = get_redis(RedisDB.AUTH_TOKENS)
    redis_throttler = get_redis(RedisDB.THROTTLER)
    redis_streams   = get_redis(RedisDB.LIVE_STREAMS)

    stream_key = f"{CLIENT_SIGNAL_PREFIX}{client_id}"
    consumer_name = f"csc_worker_{client_id}"

    # Ensure consumer group exists
    try:
        redis_streams.xgroup_create(stream_key, CONSUMER_GROUP, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    logger.info("client_signal_check started for client=%s stream=%s", client_id, stream_key)

    while True:
        try:
            messages = redis_streams.xreadgroup(
                CONSUMER_GROUP,
                consumer_name,
                {stream_key: ">"},
                count=5,
                block=2000,
            )

            if not messages:
                continue

            for _stream, entries in messages:
                for msg_id, fields in entries:
                    try:
                        payload = json.loads(fields.get("payload", "{}"))
                        process_client_signal(payload, redis_cfg, redis_auth, redis_throttler)
                    except Exception as exc:
                        logger.error("client_signal_check FAILED msg_id=%s: %s", msg_id, exc)
                        try:
                            payload = json.loads(fields.get("payload", "{}"))
                        except Exception:
                            payload = {}
                        _push_error(redis_streams, "client_signal_validation_error", payload, str(exc))
                    finally:
                        redis_streams.xack(stream_key, CONSUMER_GROUP, msg_id)

        except KeyboardInterrupt:
            logger.info("Shutting down client_signal_check for client=%s", client_id)
            break
        except Exception as exc:
            logger.exception("Unexpected error in client_signal_check loop: %s", exc)
            time.sleep(1)


if __name__ == "__main__":
    _client_id = os.getenv("CLIENT_ID") or (sys.argv[1] if len(sys.argv) > 1 else None)
    if not _client_id:
        print("Usage: python client_signal_check.py <client_id>  OR  CLIENT_ID=<id> python ...")
        sys.exit(1)
    run(_client_id)
