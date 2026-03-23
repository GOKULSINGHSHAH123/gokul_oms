"""
inject_test_signals.py
----------------------
Injects 1000 test signals into the Algo_Signal Redis stream to verify
the pipeline stages. Each signal is annotated with its expected outcome.

Run:
    python3 -m tests.inject_test_signals

Groups:
  #  1-15  Valid signals       → should pass all 4 stages
  # 16-30  Stage 1 failures    → field missing / empty / wrong type
  # 31-45  Stage 2 failures    → algo/instrument/lot/value rule violations
  # 46-55  Segment mismatch    → instrument in wrong segment
  # 56-65  Boundary values     → exactly at limits (valid edge)
  # 66-80  Mixed / duplicate   → rapid-fire, repeated algos, zero prices
  # 81-100 Severe corruption   → non-JSON payload, None strings, garbage fields
"""

import json
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_helpers import get_redis, RedisDB

STREAM = "Algo_Signal"

# ── Real instruments pulled from MASTERFILE ──────────────────────────────────
# (instrument_id, segment, lot_size)
NSEFO  = [("66647", "NSEFO", 30), ("48381", "NSEFO", 3750), ("171219", "NSEFO", 175)]
NSECM  = [("760415", "NSECM", 1),  ("9116",  "NSECM", 1),   ("758448", "NSECM", 1)]
MCXFO  = [("503955", "MCXFO", 1),  ("545479","MCXFO", 1)]
FAKE   = [("9999999", "NSEFO", 1), ("0000001","NSECM", 1)]   # not in masterfile

ALGO   = "TestAlgo"
FAKE_ALGO = "GhostAlgo"


def base(iid, seg, lot, side="BUY", qty_lots=1, price=100.0,
         algo=ALGO, product="NRML", order_type="LIMIT"):
    """Return a complete, valid signal dict."""
    return {
        "exchangeSegment":      seg,
        "exchangeInstrumentID": iid,
        "productType":          product,
        "orderType":            order_type,
        "orderSide":            side,
        "orderQuantity":        str(lot * qty_lots),
        "limitPrice":           str(price),
        "stopPrice":            "0",
        "algoName":             algo,
        "extraPercent":         "0.5",
        "algoTime":             str(int(time.time())),
    }


def push(redis_stream, label, signal):
    payload = json.dumps(signal)
    msg_id  = redis_stream.xadd(STREAM, {"payload": payload})
    print(f"  [{msg_id}]  {label}")
    return msg_id


def run():
    r = get_redis(RedisDB.LIVE_STREAMS)
    total = 0

    print(f"\nInjecting test signals into '{STREAM}'...\n")

    # ══════════════════════════════════════════════════════════════════════════
    # GROUP 1 — Valid signals (should pass all stages)
    # ══════════════════════════════════════════════════════════════════════════
    print("── GROUP 1: Valid signals (expect full pass) ──")

    iid, seg, lot = NSEFO[0]
    cases = [
        ("VALID-01  BUY  NSEFO  1 lot  NRML",
            base(iid, seg, lot, "BUY",  1, 150.0)),
        ("VALID-02  SELL NSEFO  1 lot  NRML",
            base(iid, seg, lot, "SELL", 1, 150.0)),
        ("VALID-03  BUY  NSEFO  5 lots MIS",
            base(iid, seg, lot, "BUY",  5, 200.0, product="MIS")),
        ("VALID-04  BUY  NSEFO  limitPrice=0 (market-style)",
            base(iid, seg, lot, "BUY",  1, 0.0)),
        ("VALID-05  BUY  NSEFO  large qty 50 lots",
            base(iid, seg, lot, "BUY",  50, 99.5)),
    ]
    iid2, seg2, lot2 = NSEFO[1]
    cases += [
        ("VALID-06  NSEFO instrument-2  1 lot",
            base(iid2, seg2, lot2, "BUY", 1, 500.0)),
        ("VALID-07  NSEFO instrument-3  1 lot",
            base(*NSEFO[2], "SELL", 1, 300.0)),
    ]
    iid3, seg3, lot3 = NSECM[0]
    cases += [
        ("VALID-08  NSECM instrument  qty=1",
            base(iid3, seg3, lot3, "BUY",  1, 250.0)),
        ("VALID-09  NSECM sell        qty=10",
            base(iid3, seg3, lot3, "SELL", 10, 249.9)),
        ("VALID-10  NSECM instrument-2",
            base(*NSECM[1], "BUY", 1, 180.0)),
        ("VALID-11  NSECM instrument-3",
            base(*NSECM[2], "BUY", 1, 320.0)),
        ("VALID-12  extraPercent=0",
            {**base(iid, seg, lot), "extraPercent": "0"}),
        ("VALID-13  extraPercent=5.0",
            {**base(iid, seg, lot), "extraPercent": "5.0"}),
        ("VALID-14  algoTime far future",
            {**base(iid, seg, lot), "algoTime": str(int(time.time()) + 86400)}),
        ("VALID-15  stopPrice non-zero",
            {**base(iid, seg, lot), "stopPrice": "95.0"}),
    ]
    for label, sig in cases:
        push(r, label, sig)
        total += 1

    # ══════════════════════════════════════════════════════════════════════════
    # GROUP 2 — Stage 1 failures: missing / empty / bad required fields
    # ══════════════════════════════════════════════════════════════════════════
    print("\n── GROUP 2: Stage 1 failures (field validation) ──")

    iid, seg, lot = NSEFO[0]
    good = base(iid, seg, lot)

    missing_field_cases = [
        ("FAIL-S1-01  missing exchangeSegment",      "exchangeSegment"),
        ("FAIL-S1-02  missing exchangeInstrumentID", "exchangeInstrumentID"),
        ("FAIL-S1-03  missing productType",          "productType"),
        ("FAIL-S1-04  missing orderType",            "orderType"),
        ("FAIL-S1-05  missing orderSide",            "orderSide"),
        ("FAIL-S1-06  missing orderQuantity",        "orderQuantity"),
        ("FAIL-S1-07  missing limitPrice",           "limitPrice"),
        ("FAIL-S1-08  missing stopPrice",            "stopPrice"),
        ("FAIL-S1-09  missing algoName",             "algoName"),
        ("FAIL-S1-10  missing extraPercent",         "extraPercent"),
        ("FAIL-S1-11  missing algoTime",             "algoTime"),
    ]
    for label, field in missing_field_cases:
        sig = {k: v for k, v in good.items() if k != field}
        push(r, label, sig)
        total += 1

    bad_value_cases = [
        ("FAIL-S1-12  orderSide=HOLD  (invalid value)",
            {**good, "orderSide": "HOLD"}),
        ("FAIL-S1-13  orderSide=buy   (lowercase)",
            {**good, "orderSide": "buy"}),
        ("FAIL-S1-14  orderQuantity=0  (zero)",
            {**good, "orderQuantity": "0"}),
        ("FAIL-S1-15  orderQuantity=-30 (negative)",
            {**good, "orderQuantity": "-30"}),
        ("FAIL-S1-16  orderQuantity=abc (non-numeric)",
            {**good, "orderQuantity": "abc"}),
        ("FAIL-S1-17  limitPrice=-1 (negative)",
            {**good, "limitPrice": "-1"}),
        ("FAIL-S1-18  limitPrice=None string",
            {**good, "limitPrice": "None"}),
        ("FAIL-S1-19  algoName='' (empty string)",
            {**good, "algoName": ""}),
        ("FAIL-S1-20  algoName='X' (single char)",
            {**good, "algoName": "X"}),
        ("FAIL-S1-21  exchangeSegment=None string",
            {**good, "exchangeSegment": "None"}),
        ("FAIL-S1-22  orderQuantity=None string",
            {**good, "orderQuantity": "None"}),
        ("FAIL-S1-23  all fields empty strings",
            {k: "" for k in good}),
    ]
    for label, sig in bad_value_cases:
        push(r, label, sig)
        total += 1

    # ══════════════════════════════════════════════════════════════════════════
    # GROUP 3 — Stage 2 failures: algo / instrument / lot / value checks
    # ══════════════════════════════════════════════════════════════════════════
    print("\n── GROUP 3: Stage 2 failures (algo_check) ──")

    iid, seg, lot = NSEFO[0]
    cases = [
        ("FAIL-S2-01  unknown algo name",
            base(iid, seg, lot, algo=FAKE_ALGO)),
        ("FAIL-S2-02  instrument not in masterfile",
            base(FAKE[0][0], FAKE[0][1], 1)),
        ("FAIL-S2-03  instrument not in masterfile (NSECM fake)",
            base(FAKE[1][0], FAKE[1][1], 1)),
        ("FAIL-S2-04  qty < lot_size (qty=1, lot=30)",
            {**base(iid, seg, lot), "orderQuantity": "1"}),
        ("FAIL-S2-05  qty not multiple of lot_size (qty=31, lot=30)",
            {**base(iid, seg, lot), "orderQuantity": "31"}),
        ("FAIL-S2-06  qty not multiple of lot_size (qty=91, lot=30)",
            {**base(iid, seg, lot), "orderQuantity": "91"}),
        ("FAIL-S2-07  per_trade_lot_limit exceeded (>100 lots → qty=3030)",
            {**base(iid, seg, lot), "orderQuantity": str(lot * 101)}),
        ("FAIL-S2-08  max_order_value exceeded (qty=30 * price=40000)",
            {**base(iid, seg, lot), "orderQuantity": str(lot), "limitPrice": "40000"}),
        ("FAIL-S2-09  segment not in allowed_segments (MCXFO)",
            base(*MCXFO[0], "BUY", 1, 100.0)),
        ("FAIL-S2-10  segment not in allowed_segments (MCXFO instr-2)",
            base(*MCXFO[1], "BUY", 1, 100.0)),
    ]
    # Instrument exists but wrong segment claimed in signal
    iid_nsefo, _, lot_nsefo = NSEFO[0]
    iid_nsecm, _, _         = NSECM[0]
    cases += [
        ("FAIL-S2-11  signal says NSECM but instrument is NSEFO",
            {**base(iid_nsefo, "NSECM", lot_nsefo), "exchangeSegment": "NSECM"}),
        ("FAIL-S2-12  signal says NSEFO but instrument is NSECM",
            {**base(iid_nsecm, "NSEFO", 1), "exchangeSegment": "NSEFO"}),
        ("FAIL-S2-13  instrument-2 qty not multiple (qty=3751, lot=3750)",
            {**base(*NSEFO[1]), "orderQuantity": "3751"}),
        ("FAIL-S2-14  instrument-2 qty < lot_size (qty=100, lot=3750)",
            {**base(*NSEFO[1]), "orderQuantity": "100"}),
        ("FAIL-S2-15  instrument-3 max value exceeded",
            {**base(*NSEFO[2]), "orderQuantity": str(NSEFO[2][2]), "limitPrice": "9999"}),
    ]
    for label, sig in cases:
        push(r, label, sig)
        total += 1

    # ══════════════════════════════════════════════════════════════════════════
    # GROUP 4 — Boundary / exact-limit valid signals
    # ══════════════════════════════════════════════════════════════════════════
    print("\n── GROUP 4: Boundary values (expect pass) ──")

    iid, seg, lot = NSEFO[0]
    cases = [
        ("VALID-B-01  qty exactly = lot_size (1 lot)",
            base(iid, seg, lot, qty_lots=1, price=1.0)),
        ("VALID-B-02  qty = 100 lots (at per_trade_lot_limit)",
            base(iid, seg, lot, qty_lots=100, price=1.0)),
        ("VALID-B-03  limitPrice = 0.01 (near-zero positive)",
            {**base(iid, seg, lot), "limitPrice": "0.01"}),
        ("VALID-B-04  limitPrice exactly 0",
            {**base(iid, seg, lot), "limitPrice": "0"}),
        ("VALID-B-05  orderQuantity as float string '30.0'",
            {**base(iid, seg, lot), "orderQuantity": "30.0"}),
        ("VALID-B-06  limitPrice as float string '100.00'",
            {**base(iid, seg, lot), "limitPrice": "100.00"}),
        ("VALID-B-07  very large extraPercent",
            {**base(iid, seg, lot), "extraPercent": "99.99"}),
        ("VALID-B-08  NSECM qty=1000",
            {**base(*NSECM[0]), "orderQuantity": "1000"}),
        ("VALID-B-09  NSECM qty=1",
            base(*NSECM[0], qty_lots=1, price=50.0)),
        ("VALID-B-10  algoTime=0",
            {**base(iid, seg, lot), "algoTime": "0"}),
    ]
    for label, sig in cases:
        push(r, label, sig)
        total += 1

    # ══════════════════════════════════════════════════════════════════════════
    # GROUP 5 — Rapid-fire bursts (rate limiter stress)
    # ══════════════════════════════════════════════════════════════════════════
    print("\n── GROUP 5: Rapid-fire burst (rate limiter / day counter stress) ──")

    iid, seg, lot = NSEFO[0]
    for i in range(15):
        push(r, f"BURST-{i+1:02d}  rapid BUY NSEFO {i+1}",
             base(iid, seg, lot, "BUY", 1, 100.0 + i))
        total += 1

    # ══════════════════════════════════════════════════════════════════════════
    # GROUP 6 — Corrupt / malformed payloads
    # ══════════════════════════════════════════════════════════════════════════
    print("\n── GROUP 6: Corrupt payloads ──")

    # These are pushed as raw stream entries (no 'payload' wrapper key)
    # The pipeline decodes them as dict(fields) — tests the fallback path
    corrupt_cases = [
        ("CORRUPT-01  raw fields, no JSON wrapper",
            {"exchangeSegment": seg, "exchangeInstrumentID": iid,
             "orderSide": "BUY", "orderQuantity": str(lot),
             "limitPrice": "100", "algoName": ALGO,
             "productType": "NRML", "orderType": "LIMIT",
             "stopPrice": "0", "extraPercent": "0", "algoTime": "0"}),
    ]
    for label, raw_fields in corrupt_cases:
        msg_id = r.xadd(STREAM, raw_fields)
        print(f"  [{msg_id}]  {label}  (raw fields, no payload key)")
        total += 1

    # Truly malformed JSON in payload
    malformed = [
        ("CORRUPT-02  payload is empty string",          ""),
        ("CORRUPT-03  payload is plain text",            "not json at all"),
        ("CORRUPT-04  payload is truncated JSON",        '{"algoName": "TestAlgo", "exchang'),
        ("CORRUPT-05  payload is a JSON array not dict", json.dumps([1, 2, 3])),
        ("CORRUPT-06  payload is JSON null",             "null"),
        ("CORRUPT-07  payload is JSON number",           "42"),
        ("CORRUPT-08  valid JSON but all wrong keys",
            json.dumps({"foo": "bar", "baz": 123})),
        ("CORRUPT-09  numeric instrument ID as int",
            json.dumps({**base(iid, seg, lot), "exchangeInstrumentID": int(iid)})),
        ("CORRUPT-10  quantity as list",
            json.dumps({**base(iid, seg, lot), "orderQuantity": [30]})),
    ]
    for label, payload_str in malformed:
        msg_id = r.xadd(STREAM, {"payload": payload_str})
        print(f"  [{msg_id}]  {label}")
        total += 1

    # ══════════════════════════════════════════════════════════════════════════
    # GROUP 7 — Stress test: bulk valid signals to reach 1000 total
    # ══════════════════════════════════════════════════════════════════════════
    print("\n── GROUP 7: Stress bulk signals (valid, cycling instruments) ──")

    all_valid = NSEFO + NSECM
    stress_target = 1
    stress_count = stress_target - total
    for i in range(stress_count):
        iid_s, seg_s, lot_s = all_valid[i % len(all_valid)]
        side = "BUY" if i % 2 == 0 else "SELL"
        push(r, f"STRESS-{i+1:04d}  {side} {seg_s} instrument {iid_s}",
             base(iid_s, seg_s, lot_s, side, 1, 100.0 + (i % 500)))
        total += 1

    print(f"\n✓ Injected {total} signals into '{STREAM}'")
    print("  Check pipeline logs and Error_Stream for results.\n")


if __name__ == "__main__":
    run()
