"""
test_algo_signal.py
───────────────────
Comprehensive test suite for algo_signal_check pipeline.

Sends test signals to Redis stream 'Algo_Signal' and validates
that each stage (field_check, algo_check, segment_check, client_check)
correctly accepts or rejects signals.

Usage:
    python3 test_algo_signal.py [--redis-host 127.0.0.1] [--redis-port 6379]

Each test case documents:
  - TC ID, description, target stage
  - The signal payload
  - Expected outcome (PASS / REJECT + reason)
"""

import json
import time
import argparse
import redis

# ─── Config ───────────────────────────────────────────────────────────────────

STREAM_NAME = "Algo_Signal"
ERROR_STREAM = "Error_Stream"

# ─── Base valid signal (copy of your working command) ─────────────────────────

BASE_SIGNAL = {
    "exchangeSegment":      "MCXFO",
    "exchangeInstrumentID": "486502",
    "productType":          "NRML",
    "orderType":            "LIMIT",
    "orderSide":            "BUY",
    "orderQuantity":        100,
    "limitPrice":           9100,
    "stopPrice":            0,
    "algoName":             "TestAlgo",
    "extraPercent":         0,
    "algoTime":             int(time.time()),
}


def make_signal(**overrides) -> dict:
    """Return a copy of BASE_SIGNAL with field overrides / removals."""
    sig = dict(BASE_SIGNAL)
    for k, v in overrides.items():
        if v == "__DELETE__":
            sig.pop(k, None)
        else:
            sig[k] = v
    return sig


# ══════════════════════════════════════════════════════════════════════════════
# TEST CASE DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

TEST_CASES = [

    # ──────────────────────────────────────────────────────────────────────────
    # STAGE 0: BASELINE — valid signal, should pass all stages
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":       "TC-00",
        "stage":    "All Stages",
        "desc":     "Valid baseline signal — should pass all 4 stages and fan out",
        "signal":   make_signal(),
        "expected": "PASS",
        "reason":   "All fields valid, algo active, segment allowed, clients mapped",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # STAGE 1: signal_field_check — required field validation
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":       "TC-01",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "Missing required field: exchangeSegment",
        "signal":   make_signal(exchangeSegment="__DELETE__"),
        "expected": "REJECT",
        "reason":   "exchangeSegment: required field missing or empty",
    },
    {
        "id":       "TC-02",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "Missing required field: exchangeInstrumentID",
        "signal":   make_signal(exchangeInstrumentID="__DELETE__"),
        "expected": "REJECT",
        "reason":   "exchangeInstrumentID: required field missing or empty",
    },
    {
        "id":       "TC-03",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "Missing required field: algoName",
        "signal":   make_signal(algoName="__DELETE__"),
        "expected": "REJECT",
        "reason":   "algoName: unrecognised or empty algo name",
    },
    {
        "id":       "TC-04",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "Empty string for orderType",
        "signal":   make_signal(orderType=""),
        "expected": "REJECT",
        "reason":   "orderType: required field missing or empty",
    },
    {
        "id":       "TC-05",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "None value for productType",
        "signal":   make_signal(productType=None),
        "expected": "REJECT",
        "reason":   "productType: required field missing or empty",
    },
    {
        "id":       "TC-06",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "'None' string for limitPrice",
        "signal":   make_signal(limitPrice="None"),
        "expected": "REJECT",
        "reason":   "limitPrice: required field missing or empty",
    },
    {
        "id":       "TC-07",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "orderQuantity is zero",
        "signal":   make_signal(orderQuantity=0),
        "expected": "REJECT",
        "reason":   "orderQuantity: must be a positive number",
    },
    {
        "id":       "TC-08",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "orderQuantity is negative",
        "signal":   make_signal(orderQuantity=-50),
        "expected": "REJECT",
        "reason":   "orderQuantity: must be a positive number",
    },
    {
        "id":       "TC-09",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "orderQuantity is non-numeric string",
        "signal":   make_signal(orderQuantity="abc"),
        "expected": "REJECT",
        "reason":   "orderQuantity: must be a positive number",
    },
    {
        "id":       "TC-10",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "limitPrice is negative",
        "signal":   make_signal(limitPrice=-100),
        "expected": "REJECT",
        "reason":   "limitPrice: must be zero or a positive number",
    },
    {
        "id":       "TC-11",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "orderSide is invalid (not BUY/SELL)",
        "signal":   make_signal(orderSide="SHORT"),
        "expected": "REJECT",
        "reason":   "orderSide: must be exactly 'BUY' or 'SELL'",
    },
    {
        "id":       "TC-12",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "orderSide lowercase 'buy' (case-sensitive check)",
        "signal":   make_signal(orderSide="buy"),
        "expected": "REJECT",
        "reason":   "orderSide: must be exactly 'BUY' or 'SELL'",
    },
    {
        "id":       "TC-13",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "algoName is single char (len < 2)",
        "signal":   make_signal(algoName="A"),
        "expected": "REJECT",
        "reason":   "algoName: unrecognised or empty algo name",
    },
    {
        "id":       "TC-14",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "algoName is whitespace only",
        "signal":   make_signal(algoName="   "),
        "expected": "REJECT",
        "reason":   "algoName: unrecognised or empty algo name",
    },
    {
        "id":       "TC-15",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "Missing multiple required fields (stopPrice + extraPercent)",
        "signal":   make_signal(stopPrice="__DELETE__", extraPercent="__DELETE__"),
        "expected": "REJECT",
        "reason":   "stopPrice: required field missing or empty",
    },
    {
        "id":       "TC-16",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "limitPrice is zero (valid — zero is allowed)",
        "signal":   make_signal(limitPrice=0),
        "expected": "PASS (field check)",
        "reason":   "limitPrice=0 is valid; may fail later stages depending on config",
    },
    {
        "id":       "TC-17",
        "stage":    "Stage 1 — signal_field_check",
        "desc":     "orderSide is SELL (valid alternative)",
        "signal":   make_signal(orderSide="SELL"),
        "expected": "PASS",
        "reason":   "SELL is a valid orderSide value",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # STAGE 1.5: MCXFO Multiplier adjustment (pre-field-check)
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":       "TC-18",
        "stage":    "Pre-Stage 1 — MCXFO Multiplier",
        "desc":     "MCXFO signal with instrument not in masterfile",
        "signal":   make_signal(exchangeSegment="MCXFO", exchangeInstrumentID="999999"),
        "expected": "REJECT",
        "reason":   "MCXFO multiplier adjustment failed: instrument not found in masterfile",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # STAGE 2: algo_check — algo-level RMS validation
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":       "TC-20",
        "stage":    "Stage 2 — algo_check",
        "desc":     "Unknown algo name (not in algo_configs)",
        "signal":   make_signal(algoName="NonExistentAlgo99"),
        "expected": "REJECT",
        "reason":   "algo_configs has no entry for 'NonExistentAlgo99'",
    },
    {
        "id":       "TC-21",
        "stage":    "Stage 2 — algo_check",
        "desc":     "Instrument ID not in masterfile (Redis DB 11)",
        "signal":   make_signal(exchangeSegment="NSECM", exchangeInstrumentID="000001"),
        "expected": "REJECT",
        "reason":   "Masterfile: instrument not found",
    },
    {
        "id":       "TC-22",
        "stage":    "Stage 2 — algo_check",
        "desc":     "Segment mismatch: signal segment ≠ masterfile segment",
        "signal":   make_signal(exchangeSegment="NSECM"),
        "expected": "REJECT",
        "reason":   "Masterfile segment mismatch: signal vs masterfile segment differ",
    },
    {
        "id":       "TC-23",
        "stage":    "Stage 2 — algo_check",
        "desc":     "Quantity not a multiple of lot size",
        "signal":   make_signal(orderQuantity=7),
        "expected": "REJECT",
        "reason":   "Quantity is not a multiple of lot_size",
    },
    {
        "id":       "TC-24",
        "stage":    "Stage 2 — algo_check",
        "desc":     "Quantity less than lot size",
        "signal":   make_signal(orderQuantity=0.5),
        "expected": "REJECT",
        "reason":   "Quantity < lot_size",
    },
    {
        "id":       "TC-25",
        "stage":    "Stage 2 — algo_check",
        "desc":     "Per-trade lot limit exceeded (very large quantity)",
        "signal":   make_signal(orderQuantity=999999999),
        "expected": "REJECT",
        "reason":   "Per-trade lots > per_trade_lot_limit",
    },
    {
        "id":       "TC-26",
        "stage":    "Stage 2 — algo_check",
        "desc":     "Order value exceeds max_value_of_symbol_check",
        "signal":   make_signal(orderQuantity=100, limitPrice=99999999),
        "expected": "REJECT",
        "reason":   "Order value > max_value_of_symbol_check",
    },
    {
        "id":       "TC-27",
        "stage":    "Stage 2 — algo_check",
        "desc":     "Segment not in algo's allowed_segments list",
        "signal":   make_signal(exchangeSegment="BSECM"),
        "expected": "REJECT",
        "reason":   "Segment not in allowed_segments",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # STAGE 2: Rate limiting (requires rapid-fire sends)
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":       "TC-30",
        "stage":    "Stage 2 — algo_check (rate limit)",
        "desc":     "Per-second rate limit exceeded (send multiple signals rapidly)",
        "signal":   make_signal(),
        "expected": "REJECT (after N sends in 1 sec)",
        "reason":   "Rate limit exceeded for trade_count_second",
        "_rapid_fire": True,
        "_rapid_count": 50,
    },

    # ──────────────────────────────────────────────────────────────────────────
    # STAGE 3: segment_check — market quality checks
    # (currently commented out in source, but included for completeness)
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":       "TC-40",
        "stage":    "Stage 3 — segment_check",
        "desc":     "Segment config missing for given exchangeSegment",
        "signal":   make_signal(exchangeSegment="UNKNOWN_SEG"),
        "expected": "REJECT",
        "reason":   "segment_rms_check has no config for segment (if segment check is enabled for this path)",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # STAGE 4: client_check + fan-out
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":       "TC-50",
        "stage":    "Stage 4 — client_check",
        "desc":     "No clients mapped to the algo (algo_clients is empty for this algo)",
        "signal":   make_signal(algoName="OrphanAlgo"),
        "expected": "REJECT / 0 fan-outs",
        "reason":   "No clients mapped to algo — signal dropped",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # PAYLOAD FORMAT TESTS
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":       "TC-60",
        "stage":    "Pre-Stage 1 — JSON parsing",
        "desc":     "Malformed JSON payload",
        "signal":   "__RAW_INVALID_JSON__",
        "expected": "REJECT",
        "reason":   "malformed JSON payload",
    },
    {
        "id":       "TC-61",
        "stage":    "Pre-Stage 1 — JSON parsing",
        "desc":     "Payload is a JSON array instead of object",
        "signal":   [1, 2, 3],
        "expected": "REJECT",
        "reason":   "payload must be a JSON object, got list",
    },
]


# ══════════════════════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════════════════════

def send_signal(r: redis.Redis, signal_data, stream=STREAM_NAME):
    if signal_data == "__RAW_INVALID_JSON__":
        payload = "{this is not valid json!!!"
    else:
        payload = json.dumps(signal_data)
    msg_id = r.xadd(stream, {"payload": payload})
    return msg_id


def run_tests(host: str, port: int, password: str = None):
    r = redis.Redis(host=host, port=port, password=password, decode_responses=True, db=0)
    r.ping()

    print("=" * 90)
    print(f"  ALGO SIGNAL CHECK — TEST SUITE")
    print(f"  Redis: {host}:{port}   Stream: {STREAM_NAME}")
    print(f"  Total test cases: {len(TEST_CASES)}")
    print("=" * 90)

    results = []

    for tc in TEST_CASES:
        tc_id   = tc["id"]
        desc    = tc["desc"]
        stage   = tc["stage"]
        expected = tc["expected"]

        rapid = tc.get("_rapid_fire", False)
        count = tc.get("_rapid_count", 1)

        print(f"\n{'─' * 80}")
        print(f"  {tc_id}: {desc}")
        print(f"  Stage:    {stage}")
        print(f"  Expected: {expected}")
        print(f"  Reason:   {tc['reason']}")

        # Print signal payload
        sig = tc["signal"]
        if sig == "__RAW_INVALID_JSON__":
            print(f"  Signal:   {{this is not valid json!!!")
        else:
            sig_str = json.dumps(sig, indent=4)
            indented = "\n".join("            " + line for line in sig_str.splitlines())
            print(f"  Signal:   {indented.lstrip()}")

        if rapid:
            print(f"  >> Sending {count} signals rapidly...")
            for i in range(count):
                msg_id = send_signal(r, tc["signal"])
            print(f"  >> Last msg_id: {msg_id}")
        else:
            msg_id = send_signal(r, tc["signal"])
            print(f"  >> msg_id: {msg_id}")

        results.append({
            "id": tc_id,
            "desc": desc,
            "stage": stage,
            "expected": expected,
            "reason": tc["reason"],
            "msg_id": msg_id if not rapid else f"{count}x rapid",
            "status": "SENT",
        })

        time.sleep(0.1)

    print(f"\n{'═' * 90}")
    print(f"  All {len(TEST_CASES)} test signals sent.")
    print(f"  Check worker logs and Error_Stream for actual pass/fail results.")
    print(f"  To read errors:  redis-cli XRANGE Error_Stream - + COUNT 100")
    print(f"{'═' * 90}\n")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Algo Signal Check Test Suite")
    parser.add_argument("--redis-host", default="host")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-password", default="RMS_TESTING_AWS_DABBA")
    args = parser.parse_args()

    run_tests(args.redis_host, args.redis_port, args.redis_password)