"""
algo_signal_check.py
--------------------
Multi-process consumer — INDEPENDENT PROCESSES VERSION.

Each worker process runs its own xreadgroup loop against Redis Streams.
Redis acts as the load balancer — no Python multiprocessing.Pool,
no apply_async, no synchronous .get() blocking between batches.

Key design:
  1. N independent worker processes — each calls xreadgroup directly.
     Redis distributes messages across consumers automatically.
  2. Per-process in-memory config cache — refreshed by a background
     thread every CACHE_REFRESH_INTERVAL seconds.
  3. Per-process Redis connections — opened once at process startup.
  4. ThreadPoolExecutor (fanout) — threads inside each worker for
     per-client I/O fanout (pure I/O, threads are fine here).
  5. Single pipeline market read — OI + bid + ask in one round-trip.
  6. Batched counter increments — trade_count_second + trade_count_day
     in one pipeline call.
  7. Atomic rate-limit gate — Lua script for check-and-increment so
     concurrent workers don't race past the per-second limit.

What still hits Redis directly (must be live / accurate):
  - trade_count_second, trade_count_day, lot_count_day  (counters)
  - OI_LATEST, bid/ask                                  (market data)
  - xadd to throttler + audit streams                   (writes)

Instrument cache — two-level lookup (cache-aside):
  Level 1 : _MEMORY["instruments"]  — in-process dict, keyed by
            ExchangeInstrumentID (string). Entries are permanent —
            they live until the worker process restarts. No TTL,
            no eviction. If instrument metadata changes, restart workers.
  Level 2 : Redis DB 11 (MASTERFILE) — consulted only on a cache miss.
            Only HASH keys are tried: HGETALL "<id>_hash".
            The result is stored back into level-1 permanently.

  No startup SCAN. Instruments are loaded lazily, one per unique
  ExchangeInstrumentID seen in live signals. Per-process RSS stays
  proportional to the number of distinct instruments actually traded.

  Note: within a single signal's processing path (Stage 2 + Stage 3
  both call get_instrument for the same id) the second call hits
  level-1 and costs nothing.

Stage map:
  Stage 1 — signal_field_check   : pure in-memory field validation
  Stage 2 — algo_check           : algo-level RMS (counters hit Redis)
  Stage 3 — segment_check        : market quality checks
  Stage 4 — client_check + fanout: per-client RMS + fan-out

Instrument key structure in Redis DB 11 (MASTERFILE):
  - STRING keys  e.g. "100141"       -> JSON-encoded dict with PascalCase fields
  - HASH keys    e.g. "100145_hash"  -> Hash with PascalCase fields
  Both are loaded into _MEMORY["instruments"] keyed by ExchangeInstrumentID.
  Field names are PascalCase: ExchangeSegment, Series, LotSize, etc.
"""

import json
import os
import time
import datetime
import uuid
import signal
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.db_helpers import get_redis, RedisDB
from utils.setup_logging import get_logger
from services.algo_service import list_algos
from services.client_service import list_client_rms, _get_broker_map
from services.segment_service import list_segments

logger = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Tunables
# ──────────────────────────────────────────────────────────────────────────────

CACHE_REFRESH_INTERVAL = 3    # seconds between config/rms cache refresh
MAX_FANOUT_WORKERS     = 20   # parallel threads per signal for client fan-out
NUM_WORKER_PROCESSES   = 2    # independent consumer processes to spawn

# ──────────────────────────────────────────────────────────────────────────────
# Redis stream / key constants
# ──────────────────────────────────────────────────────────────────────────────

ALGO_SIGNAL_STREAM  = "Algo_Signal"
ERROR_STREAM        = "Error_Stream"
ORDER_REQUEST_STREAM        = "Client_API_Place_Order"    # single shared stream for all clients
ORDER_REQUEST_CONSUMER_GROUP = "group1"  # consumer group on order_request stream
CONSUMER_GROUP = "oms_pipeline"

# ──────────────────────────────────────────────────────────────────────────────
# Required signal fields
# ──────────────────────────────────────────────────────────────────────────────

REQUIRED_SIGNAL_FIELDS = [
    "exchangeSegment",
    "exchangeInstrumentID",
    "productType",
    "orderType",
    "orderSide",
    "orderQuantity",
    "limitPrice",
    "stopPrice",
    "algoName",
    "extraPercent",
    "algoTime",
]

# ──────────────────────────────────────────────────────────────────────────────
# Lua script: atomic check-and-increment for per-second rate limiting.
# Returns the new counter value ONLY if it is within the limit,
# otherwise returns -1 so the caller can reject without a race.
#
# KEYS[1] = rate key   e.g. "trade_count_second:MyAlgo:1718000000"
# ARGV[1] = ttl (seconds)
# ARGV[2] = limit  (0 = unlimited)
# ──────────────────────────────────────────────────────────────────────────────

_RATE_LIMIT_LUA = """
local current = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[1])
local limit = tonumber(ARGV[2])
if limit > 0 and current > limit then
    return -1
end
return current
"""


# ══════════════════════════════════════════════════════════════════════════════
# Per-process in-memory cache  +  CacheUpdater thread
# ══════════════════════════════════════════════════════════════════════════════

_MEMORY = {
    "algo_configs":      {},
    "segment_rms_check": {},
    "instruments":       {},   # keyed by ExchangeInstrumentID (string)
    "client_rms":        {},
    "algo_clients":      {},
}

_MEMORY_LOCK = threading.RLock()


def _mem_get(table: str, field: str):
    with _MEMORY_LOCK:
        return _MEMORY[table].get(field)


def _mem_get_table(table: str) -> dict:
    """Shallow copy so the fanout loop doesn't hold the lock."""
    with _MEMORY_LOCK:
        return dict(_MEMORY[table])


# ──────────────────────────────────────────────────────────────────────────────
# Two-level instrument lookup  (cache-aside)
# ──────────────────────────────────────────────────────────────────────────────

def get_instrument(instrument_id: str) -> dict | None:
    """
    Return the instrument dict for *instrument_id*, or None if not found.

    Lookup order
    ────────────
    1. In-process cache (_MEMORY["instruments"]) — O(1), zero I/O.
       Once an entry exists it is permanent for the life of the process.

    2. Redis DB 11 (MASTERFILE) — only on a cache miss.
       Uses HGETALL on key "<id>_hash".
       Result is written to level-1 and never expires.

    Thread-safety
    ─────────────
    Level-1 reads and writes are serialised by _MEMORY_LOCK (RLock).
    Two threads hitting the same unseen id simultaneously may both reach
    Redis; the last writer wins, which is harmless since both fetched
    the same data.
    """
    with _MEMORY_LOCK:
        entry = _MEMORY["instruments"].get(instrument_id)

    if entry is not None:
        return entry["_data"]

    data = _fetch_single_instrument_from_redis(instrument_id)

    if data is None:
        logger.warning(
            "Instrument id=%s NOT found in MASTERFILE (HASH key '%s_hash' empty or missing)",
            instrument_id, instrument_id,
        )
        return None

    with _MEMORY_LOCK:
        _MEMORY["instruments"][instrument_id] = {"_data": data}

    logger.debug("Instrument cache MISS -> fetched id=%s from Redis", instrument_id)
    return data


def _fetch_single_instrument_from_redis(instrument_id: str) -> dict | None:
    """
    Fetch a single instrument from Redis DB 11 (MASTERFILE).
    Only HASH keys are used: HGETALL "<instrument_id>_hash".
    Returns a plain dict of PascalCase instrument fields, or None if the key
    does not exist or is empty.
    """
    hash_key = f"{instrument_id}_hash"
    data     = _proc_redis_master.hgetall(hash_key)
    return data if data else None


class CacheUpdater(threading.Thread):
    """
    Daemon thread that runs inside every worker process.
    Refreshes algo_configs / segment_rms_check / client_rms / algo_clients
    directly from MongoDB every CACHE_REFRESH_INTERVAL seconds.
    """

    def __init__(self):
        super().__init__(daemon=True, name="CacheUpdater")

    def run(self):
        logger.info("CacheUpdater started in pid=%s", os.getpid())
        try:
            self._refresh()
        except Exception as exc:
            logger.error("CacheUpdater initial refresh failed: %s", exc)
        while True:
            time.sleep(CACHE_REFRESH_INTERVAL)
            try:
                self._refresh()
            except Exception as exc:
                logger.error("CacheUpdater refresh failed: %s", exc)

    def _refresh(self):
        # --- algo_configs ---
        new_algo_configs = {}
        for algo in list_algos(active_only=True):
            algo_name = algo.get("algo_name")
            if not algo_name:
                continue
            new_algo_configs[algo_name] = {
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

        # --- segment_rms_check ---
        new_segment_rms_check = {}
        for seg in list_segments():
            seg_name = seg.get("segment")
            if not seg_name:
                continue
            new_segment_rms_check[seg_name] = {
                "oi_threshold":   seg.get("oi", 0),
                "max_spread_pct": seg.get("spread", 100),
            }

        # --- client_rms + algo_clients ---
        new_client_rms  = {}
        new_algo_clients: dict = {}
        broker_map = _get_broker_map()
        for entry in list_client_rms():
            client_id = entry.get("client_id")
            algo_name = entry.get("algo_name")
            if not client_id or not algo_name:
                continue
            new_client_rms[f"{client_id}:{algo_name}"] = {
                "quantity_multiplier": entry.get("quantity_multiplier", 1.0),
                "broker":              broker_map.get(client_id, ""),
            }
            new_algo_clients.setdefault(algo_name, []).append(client_id)

        with _MEMORY_LOCK:
            _MEMORY["algo_configs"]      = new_algo_configs
            _MEMORY["segment_rms_check"] = new_segment_rms_check
            _MEMORY["client_rms"]        = new_client_rms
            _MEMORY["algo_clients"]      = new_algo_clients
        logger.debug("CacheUpdater refreshed from MongoDB in pid=%s", os.getpid())


# ══════════════════════════════════════════════════════════════════════════════
# Per-process Redis handles  (set once in _worker_init)
# ══════════════════════════════════════════════════════════════════════════════

_proc_redis_cfg       = None
_proc_redis_market    = None
_proc_redis_quality   = None
_proc_redis_streams   = None
_proc_redis_throttler = None
_proc_redis_master    = None
_proc_rate_limit_sha  = None


def _worker_init(worker_index: int):
    global _proc_redis_cfg, _proc_redis_market
    global _proc_redis_quality, _proc_redis_streams, _proc_redis_throttler
    global _proc_redis_master, _proc_rate_limit_sha
    global _consumer_name

    _consumer_name = f"worker_{worker_index}_{os.getpid()}"

    _proc_redis_cfg       = get_redis(RedisDB.CONFIG_CACHE)
    _proc_redis_market    = get_redis(RedisDB.MARKET_DATA)
    _proc_redis_quality   = get_redis(RedisDB.QUALITY_STOCKS)
    _proc_redis_streams   = get_redis(RedisDB.LIVE_STREAMS)
    _proc_redis_throttler = get_redis(RedisDB.THROTTLER)
    _proc_redis_master    = get_redis(RedisDB.MASTERFILE)

    _proc_rate_limit_sha  = _proc_redis_cfg.script_load(_RATE_LIMIT_LUA)

    logger.info(
        "Worker pid=%s name=%s — instrument cache is LAZY + PERMANENT (no startup scan)",
        os.getpid(), _consumer_name,
    )

    updater = CacheUpdater()
    updater.start()
    time.sleep(0.5)

    logger.info("Worker pid=%s name=%s ready", os.getpid(), _consumer_name)


# ══════════════════════════════════════════════════════════════════════════════
# Stage 1 — signal_field_check  (pure in-memory)
# ══════════════════════════════════════════════════════════════════════════════

class FieldValidationError(Exception):
    def __init__(self, field: str, reason: str):
        self.field  = field
        self.reason = reason
        super().__init__(f"{field}: {reason}")


def signal_field_check(signal: dict) -> None:
    for field in REQUIRED_SIGNAL_FIELDS:
        if field not in signal or signal[field] in (None, "", "None"):
            raise FieldValidationError(field, "required field missing or empty")

    try:
        qty = float(signal["orderQuantity"])
        if qty <= 0:
            raise ValueError
    except (ValueError, TypeError):
        raise FieldValidationError("orderQuantity", "must be a positive number")

    try:
        price = float(signal["limitPrice"])
        if price < 0:
            raise ValueError
    except (ValueError, TypeError):
        raise FieldValidationError("limitPrice", "must be zero or a positive number")

    if signal["orderSide"] not in ("BUY", "SELL"):
        raise FieldValidationError("orderSide", "must be exactly 'BUY' or 'SELL'")

    algo = str(signal.get("algoName", "")).strip()
    if not algo or len(algo) < 2:
        raise FieldValidationError("algoName", "unrecognised or empty algo name")


# ══════════════════════════════════════════════════════════════════════════════
# Stage 2 — algo_check
# ══════════════════════════════════════════════════════════════════════════════

def _seconds_until_midnight() -> int:
    now      = datetime.datetime.now()
    midnight = (now + datetime.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return int((midnight - now).total_seconds())


def _atomic_rate_check(redis_conn, key: str, ttl: int, limit: int) -> int:
    global _proc_rate_limit_sha

    def _run(sha_or_none):
        if sha_or_none:
            return redis_conn.evalsha(sha_or_none, 1, key, ttl, limit)
        return redis_conn.eval(_RATE_LIMIT_LUA, 1, key, ttl, limit)

    try:
        result = _run(_proc_rate_limit_sha)
    except Exception as exc:
        if "NOSCRIPT" in str(exc):
            _proc_rate_limit_sha = redis_conn.script_load(_RATE_LIMIT_LUA)
            result = _run(_proc_rate_limit_sha)
        else:
            raise

    if result == -1:
        raise RuntimeError(f"Rate limit exceeded for key '{key}' (limit={limit})")
    return int(result)


def _increment_counter(redis_conn, key: str, ttl: int, amount: int = 1) -> int:
    pipe = redis_conn.pipeline(transaction=False)
    pipe.incrby(key, amount)
    pipe.expire(key, ttl)
    return pipe.execute()[0]


def algo_check(signal: dict) -> dict:
    """
    Config reads  -> _MEMORY (zero Redis I/O)
    Instrument    -> get_instrument() [level-1 cache, or Redis on miss]
    Counter I/O   -> Redis (must be accurate across all worker processes)
    """
    redis_cfg     = _proc_redis_cfg
    algo_name     = signal["algoName"]
    instrument_id = str(signal["exchangeInstrumentID"])
    segment       = signal["exchangeSegment"]
    quantity      = int(float(signal["orderQuantity"]))
    limit_price   = float(signal["limitPrice"])

    instrument = get_instrument(instrument_id)
    if not instrument:
        raise RuntimeError(f"Masterfile: instrument '{instrument_id}' not found")

    master_segment = instrument.get("ExchangeSegment", "")
    if master_segment != segment:
        raise ValueError(
            f"Masterfile segment mismatch: signal={segment}, masterfile={master_segment}"
        )

    algo_cfg = _mem_get("algo_configs", algo_name)
    if not algo_cfg:
        raise KeyError(f"algo_configs has no entry for '{algo_name}'")

    if not algo_cfg.get("status", False):
        raise RuntimeError(f"Algo '{algo_name}' is inactive (status=false)")

    allowed_segments = algo_cfg.get("allowed_segments", [])
    if allowed_segments and segment not in allowed_segments:
        raise RuntimeError(
            f"Segment '{segment}' not in allowed_segments {allowed_segments}"
        )

    lot_size = int(instrument.get("LotSize") or algo_cfg.get("lot_size", 1))
    if quantity < lot_size:
        raise RuntimeError(f"Quantity {quantity} < lot_size {lot_size}")
    if quantity % lot_size != 0:
        raise RuntimeError(
            f"Quantity {quantity} is not a multiple of lot_size {lot_size}"
        )

    lots = quantity // lot_size
    per_trade_lot_limit = int(algo_cfg.get("per_trade_lot_limit", 0))
    if per_trade_lot_limit and lots > per_trade_lot_limit:
        raise RuntimeError(
            f"Per-trade lots {lots} > per_trade_lot_limit {per_trade_lot_limit}"
        )

    max_value   = float(algo_cfg.get("max_value_of_symbol_check", 0))
    order_value = quantity * limit_price
    if max_value and order_value > max_value:
        raise RuntimeError(
            f"Order value {order_value} > max_value_of_symbol_check {max_value}"
        )

    epoch           = int(time.time())
    midnight_ttl    = _seconds_until_midnight()
    rate_key        = f"trade_count_second:{algo_name}:{epoch}"
    trade_limit_sec = int(algo_cfg.get("trade_limit_per_second", 0))
    _atomic_rate_check(redis_cfg, rate_key, ttl=2, limit=trade_limit_sec)

    day_key         = f"trade_count_day:{algo_name}"
    trade_count_day = _increment_counter(redis_cfg, day_key, midnight_ttl)
    trade_limit_day = int(algo_cfg.get("trade_limit_per_day", 0))
    if trade_limit_day and trade_count_day > trade_limit_day:
        raise RuntimeError(
            f"Daily trade limit exceeded: {trade_count_day}/{trade_limit_day} for '{algo_name}'"
        )

    lot_day_key     = f"lot_count_day:{algo_name}"
    new_lot_day     = _increment_counter(redis_cfg, lot_day_key, midnight_ttl, amount=lots)
    daily_lot_limit = int(algo_cfg.get("daily_lot_limit", 0))
    if daily_lot_limit and new_lot_day > daily_lot_limit:
        raise RuntimeError(
            f"Daily lot limit exceeded: {new_lot_day}/{daily_lot_limit} for '{algo_name}'"
        )

    logger.debug(
        "algo_check PASSED for %s algo=%s qty=%s", instrument_id, algo_name, quantity
    )
    return algo_cfg


# ══════════════════════════════════════════════════════════════════════════════
# Stage 3 — segment_check
# ══════════════════════════════════════════════════════════════════════════════

_DERIVATIVES_SERIES = {"OPTIDX", "OPTSTK", "FUTIDX", "FUTSTK"}
_EQUITY_SERIES      = {"EQ"}


def segment_check(signal: dict) -> None:
    redis_market  = _proc_redis_market
    redis_quality = _proc_redis_quality
    instrument_id = str(signal["exchangeInstrumentID"])
    segment       = signal["exchangeSegment"]

    instrument = get_instrument(instrument_id)
    if not instrument:
        raise RuntimeError(
            f"Masterfile: instrument '{instrument_id}' not found in segment_check"
        )

    series  = instrument.get("Series", "").upper()
    seg_cfg = _mem_get("segment_rms_check", segment)
    if not seg_cfg:
        raise RuntimeError(
            f"segment_rms_check has no config for segment '{segment}'"
        )

    # if series in _DERIVATIVES_SERIES:
    #     _derivatives_check(instrument_id, seg_cfg, redis_market)
    # elif series in _EQUITY_SERIES:
    #     _equity_check(instrument_id, redis_quality)
    # else:
    #     raise RuntimeError(f"Unknown instrument series '{series}' — no check path defined")

    logger.debug("segment_check PASSED for %s series=%s", instrument_id, series)


def _derivatives_check(instrument_id: str, seg_cfg: dict, redis_market) -> None:
    pipe = redis_market.pipeline(transaction=False)
    pipe.hget("OI_LATEST", instrument_id)
    pipe.hget(f"bid:{instrument_id}", "bid")
    pipe.hget(f"bid:{instrument_id}", "ask")
    oi_raw, bid_raw, ask_raw = pipe.execute()

    if oi_raw is None:
        raise RuntimeError(f"OI_LATEST has no data for instrument '{instrument_id}'")

    current_oi   = float(oi_raw)
    oi_threshold = float(seg_cfg.get("oi_threshold", 0))
    if oi_threshold and current_oi < oi_threshold:
        raise RuntimeError(
            f"OI too low: {current_oi} < threshold {oi_threshold} for '{instrument_id}'"
        )

    if bid_raw and ask_raw:
        bid, ask = float(bid_raw), float(ask_raw)
        if ask > 0:
            spread_pct = (ask - bid) / ask * 100
            max_spread = float(seg_cfg.get("max_spread_pct", 100))
            if spread_pct > max_spread:
                raise RuntimeError(
                    f"Bid-ask spread {spread_pct:.2f}% > max {max_spread}% "
                    f"for '{instrument_id}'"
                )


def _equity_check(instrument_id: str, redis_quality) -> None:
    if not redis_quality.hexists("quality_stocks", instrument_id):
        raise RuntimeError(
            f"Instrument '{instrument_id}' not in quality_stocks "
            f"(not in top 150 liquid stocks)"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Stage 4 — client_check + fan-out
# ══════════════════════════════════════════════════════════════════════════════

def client_check_and_fanout(signal: dict, algo_cfg: dict) -> int:
    algo_name = signal["algoName"]

    client_ids = _mem_get("algo_clients", algo_name)
    if not client_ids:
        logger.warning("No clients mapped to algo '%s' — signal dropped", algo_name)
        return 0

    client_rms_table = _mem_get_table("client_rms")
    fanout_count     = 0
    errors           = []

    with ThreadPoolExecutor(
        max_workers=min(MAX_FANOUT_WORKERS, len(client_ids))
    ) as pool:
        futures = {
            pool.submit(
                _process_single_client,
                signal, algo_name, client_id, client_rms_table,
            ): client_id
            for client_id in client_ids
        }

        for future in as_completed(futures):
            client_id = futures[future]
            try:
                future.result()
                fanout_count += 1
            except Exception as exc:
                logger.error(
                    "client_check FAILED client=%s algo=%s: %s",
                    client_id, algo_name, exc,
                )
                errors.append((client_id, str(exc)))

    for client_id, reason in errors:
        _push_error(
            error_type="ClientOrderValidationError",
            signal=signal,
            reason=reason,
            stage="client_check",
            client_id=client_id,
        )

    return fanout_count


def _process_single_client(
    signal: dict,
    algo_name: str,
    client_id: str,
    client_rms_table: dict,
) -> None:
    rms_key    = f"{client_id}:{algo_name}"
    client_rms = client_rms_table.get(rms_key)

    multiplier   = float(client_rms.get("quantity_multiplier", 1.0)) if client_rms else 1.0
    broker       = client_rms.get("broker", "") if client_rms else ""
    original_qty = int(float(signal["orderQuantity"]))
    new_qty      = int(original_qty * multiplier)
    if new_qty <= 0:
        raise RuntimeError(
            f"Computed qty {new_qty} <= 0 after multiplier {multiplier}"
        )

    order_unique_id = f"{algo_name}_{uuid.uuid4().hex}"
    broker_payload = {
        "orderUniqueIdentifier": order_unique_id,
        "algoName": algo_name,
        "clientID": str(client_id),

        "exchangeInstrumentID": signal["exchangeInstrumentID"],
        "exchangeSegment": signal["exchangeSegment"],

        "orderSide": signal["orderSide"],
        "orderQuantity": new_qty,

        "limitPrice": signal["limitPrice"],
        "stopPrice": signal.get("stopPrice", 0),

        "orderType": signal["orderType"],
        "productType": signal.get("productType", "NRML"),

        "timeInForce": signal.get("timeInForce", "DAY"),
        "disclosedQuantity": signal.get("disclosedQuantity", 0),

        "upperPriceLimit": signal.get("upperPriceLimit", 0),
        "lowerPriceLimit": signal.get("lowerPriceLimit", 0),

        "timePeriod": signal.get("timePeriod", 0),
        "extraPercent": signal.get("extraPercent", 0),

        "algoPrice": signal.get("algoPrice", signal["limitPrice"]),
        "algoTime": signal.get("algoTime", time.time()),

        "ltpExceedMaxValueCheck": signal.get("ltpExceedMaxValueCheck", 0),

        "algoSignalUniqueIdentifier": signal.get("algoSignalUniqueIdentifier", "0"),

        "source": "signal_engine",

        "broker":broker,

        "timestamp": int(time.time() * 1000),
    }
    # ── Single shared stream — clientID is inside the payload ─────────────────
    _proc_redis_throttler.xadd(ORDER_REQUEST_STREAM, {"payload": json.dumps(broker_payload)})

    logger.info(
        "Fan-out OK: client=%s algo=%s orderID=%s qty=%s",
        client_id, algo_name, order_unique_id, new_qty,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Error helper
# ══════════════════════════════════════════════════════════════════════════════

def _push_error(error_type, signal, reason, stage, client_id=None):
    payload = {
        "error_type": error_type,
        "stage":      stage,
        "reason":     reason,
        "signal":     json.dumps(signal),
        "client_id":  str(client_id) if client_id else "",
        "timestamp":  int(time.time() * 1000),
    }
    _proc_redis_streams.xadd(ERROR_STREAM, payload)
    logger.warning("Error -> %s  stage=%s  reason=%s", ERROR_STREAM, stage, reason)


# ══════════════════════════════════════════════════════════════════════════════
# Message processor  (called inside each worker's own loop)
# ══════════════════════════════════════════════════════════════════════════════

def _process_message(msg_id: str, fields: dict) -> None:
    t_start = time.monotonic()
    print(_MEMORY["client_rms"])
    try:
        if "payload" in fields:
            try:
                signal = json.loads(fields["payload"])
            except (json.JSONDecodeError, Exception) as exc:
                _push_error(
                    "AlgoSignalValidationError", {},
                    f"malformed JSON payload: {exc}", "signal_field_check",
                )
                return
            if not isinstance(signal, dict):
                _push_error(
                    "AlgoSignalValidationError", {},
                    f"payload must be a JSON object, got {type(signal).__name__}",
                    "signal_field_check",
                )
                return
        else:
            signal = dict(fields)

        logger.debug("Processing msg_id=%s algoName=%s", msg_id, signal.get("algoName"))

        # MCXFO adjustment: divide orderQuantity by Multiplier from masterfile
        if signal.get("exchangeSegment") == "MCXFO":
            instrument_id = str(signal.get("exchangeInstrumentID", ""))
            logger.info(
                "[MCXFO] Multiplier adjustment triggered | msg=%s algo=%s instrument=%s raw_qty=%s",
                msg_id, signal.get("algoName"), instrument_id, signal.get("orderQuantity"),
            )
            try:
                instrument = get_instrument(instrument_id)
                if instrument is None:
                    logger.error(
                        "[MCXFO] Instrument '%s' not found in masterfile — cannot apply multiplier | msg=%s",
                        instrument_id, msg_id,
                    )
                    _push_error(
                        "AlgoSignalValidationError", signal,
                        f"MCXFO multiplier adjustment failed: instrument '{instrument_id}' not found in masterfile",
                        "signal_field_check",
                    )
                    return

                raw_multiplier = instrument.get("Multiplier")
                if raw_multiplier is None:
                    logger.error(
                        "[MCXFO] Multiplier field missing in masterfile for instrument '%s' | msg=%s",
                        instrument_id, msg_id,
                    )
                    _push_error(
                        "AlgoSignalValidationError", signal,
                        f"MCXFO multiplier field missing in masterfile for instrument '{instrument_id}'",
                        "signal_field_check",
                    )
                    return

                multiplier = float(raw_multiplier)
                if multiplier <= 0:
                    logger.error(
                        "[MCXFO] Invalid Multiplier=%s for instrument '%s' — must be > 0 | msg=%s",
                        raw_multiplier, instrument_id, msg_id,
                    )
                    _push_error(
                        "AlgoSignalValidationError", signal,
                        f"MCXFO Multiplier={raw_multiplier} for instrument '{instrument_id}' must be > 0",
                        "signal_field_check",
                    )
                    return

                original_qty = float(signal["orderQuantity"])
                adjusted_qty = original_qty / multiplier
                signal["orderQuantity"] = adjusted_qty
                logger.info(
                    "[MCXFO] Multiplier applied | msg=%s algo=%s instrument=%s "
                    "original_qty=%s multiplier=%s adjusted_qty=%s",
                    msg_id, signal.get("algoName"), instrument_id,
                    original_qty, multiplier, adjusted_qty,
                )

            except (ValueError, TypeError) as exc:
                logger.error(
                    "[MCXFO] Type conversion error during multiplier adjustment | msg=%s instrument=%s error=%s",
                    msg_id, instrument_id, exc,
                )
                _push_error(
                    "AlgoSignalValidationError", signal,
                    f"MCXFO multiplier type error for instrument '{instrument_id}': {exc}",
                    "signal_field_check",
                )
                return
            except Exception as exc:
                logger.exception(
                    "[MCXFO] Unexpected error during multiplier adjustment | msg=%s instrument=%s error=%s",
                    msg_id, instrument_id, exc,
                )
                _push_error(
                    "AlgoSignalValidationError", signal,
                    f"MCXFO multiplier unexpected error for instrument '{instrument_id}': {exc}",
                    "signal_field_check",
                )
                return

        t1 = time.monotonic()
        try:
            signal_field_check(signal)
        except FieldValidationError as exc:
            _push_error("AlgoSignalValidationError", signal, str(exc), "signal_field_check")
            return
        t1_ms = (time.monotonic() - t1) * 1000

        t2 = time.monotonic()
        try:
            algo_cfg = algo_check(signal)
        except Exception as exc:
            _push_error("AlgoSignalValidationError", signal, str(exc), "algo_check")
            return
        t2_ms = (time.monotonic() - t2) * 1000

        t3 = time.monotonic()
        try:
            segment_check(signal)
        except Exception as exc:
            _push_error("AlgoSignalValidationError", signal, str(exc), "segment_check")
            return
        t3_ms = (time.monotonic() - t3) * 1000

        t4 = time.monotonic()
        try:
            n = client_check_and_fanout(signal, algo_cfg)
            logger.info("Signal fanned out to %s client(s): algo=%s", n, signal.get("algoName"))
        except Exception as exc:
            _push_error("ClientOrderValidationError", signal, str(exc), "client_check")
            return
        t4_ms = (time.monotonic() - t4) * 1000

        total_ms = (time.monotonic() - t_start) * 1000
        logger.info(
            "TIMING msg=%s algo=%s | s1=%.2fms s2=%.2fms s3=%.2fms s4=%.2fms | total=%.2fms",
            msg_id, signal.get("algoName"), t1_ms, t2_ms, t3_ms, t4_ms, total_ms,
        )

    except Exception as exc:
        logger.exception("Unhandled exception processing msg_id=%s: %s", msg_id, exc)

    finally:
        _proc_redis_streams.xack(ALGO_SIGNAL_STREAM, CONSUMER_GROUP, msg_id)


# ══════════════════════════════════════════════════════════════════════════════
# Per-process consumer loop
# ══════════════════════════════════════════════════════════════════════════════

def _ensure_consumer_group(redis_streams) -> None:
    try:
        redis_streams.xgroup_create(
            ALGO_SIGNAL_STREAM, CONSUMER_GROUP, id="0", mkstream=True
        )
        logger.info(
            "Consumer group '%s' created on '%s'", CONSUMER_GROUP, ALGO_SIGNAL_STREAM
        )
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            logger.debug("Consumer group already exists — OK")
        else:
            raise


def _ensure_order_consumer_group() -> None:
    """Create the consumer group on the shared order_request stream (throttler DB)."""
    try:
        _proc_redis_throttler.xgroup_create(
            ORDER_REQUEST_STREAM, ORDER_REQUEST_CONSUMER_GROUP, id="0", mkstream=True
        )
        logger.info(
            "Consumer group '%s' created on '%s'",
            ORDER_REQUEST_CONSUMER_GROUP, ORDER_REQUEST_STREAM,
        )
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            logger.debug("Order consumer group already exists — OK")
        else:
            raise


def _worker_loop(worker_index: int) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    _worker_init(worker_index)

    redis_streams = _proc_redis_streams
    _ensure_consumer_group(redis_streams)
    _ensure_order_consumer_group()

    logger.info(
        "Worker pid=%s index=%d listening on '%s'",
        os.getpid(), worker_index, ALGO_SIGNAL_STREAM,
    )

    while True:
        try:
            messages = redis_streams.xreadgroup(
                CONSUMER_GROUP,
                _consumer_name,
                {ALGO_SIGNAL_STREAM: ">"},
                count=10,
                block=2000,
            )

            if not messages:
                continue

            for _stream, batch in messages:
                for msg_id, fields in batch:
                    _process_message(msg_id, fields)

        except KeyboardInterrupt:
            break
        except Exception as exc:
            if "NOGROUP" in str(exc):
                logger.warning("Stream or consumer group missing — re-creating: %s", exc)
                try:
                    _ensure_consumer_group(redis_streams)
                except Exception as re_exc:
                    logger.error("Failed to re-create consumer group: %s", re_exc)
                time.sleep(1)
                continue
            logger.exception(
                "Unexpected error in worker pid=%s loop: %s", os.getpid(), exc
            )
            time.sleep(1)

    logger.info("Worker pid=%s shutting down.", os.getpid())


# ══════════════════════════════════════════════════════════════════════════════
# Main entry point
# ══════════════════════════════════════════════════════════════════════════════

def run() -> None:
    logger.info(
        "algo_signal_check starting — spawning %d worker processes", NUM_WORKER_PROCESSES
    )

    processes = []
    for i in range(NUM_WORKER_PROCESSES):
        p = multiprocessing.Process(
            target=_worker_loop,
            args=(i,),
            name=f"algo_signal_worker_{i}",
            daemon=True,
        )
        p.start()
        processes.append(p)
        logger.info("Spawned worker %d pid=%s", i, p.pid)

    try:
        while True:
            for idx, p in enumerate(processes):
                if not p.is_alive():
                    logger.warning(
                        "Worker %d (pid=%s) died with exit code %s — restarting",
                        idx, p.pid, p.exitcode,
                    )
                    new_p = multiprocessing.Process(
                        target=_worker_loop,
                        args=(idx,),
                        name=f"algo_signal_worker_{idx}",
                        daemon=True,
                    )
                    new_p.start()
                    processes[idx] = new_p
                    logger.info("Restarted worker %d as pid=%s", idx, new_p.pid)
            time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Shutting down algo_signal_check — terminating workers.")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join(timeout=10)
        logger.info("All workers stopped.")


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    run()