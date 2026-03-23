# Firewall Test Suite — Documentation

**File:** `tests/test_firewall.py`  
**Total tests:** 112  
**Run command:** `pytest tests/test_firewall.py -v`

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Test Infrastructure](#2-test-infrastructure)
3. [Stage 1 — Signal Field Check](#3-stage-1--signal-field-check)
4. [Stage 2 — Algo Check](#4-stage-2--algo-check)
5. [Stage 3 — Segment Check](#5-stage-3--segment-check)
6. [Stage 4 — Client Check & Fanout](#6-stage-4--client-check--fanout)
7. [MCXFO Multiplier Adjustment](#7-mcxfo-multiplier-adjustment)
8. [Process Message Routing](#8-process-message-routing)
9. [ExecErrorProcessor (exec_error.py)](#9-execerrorprocessor-exec_errorpy)
10. [How to Validate Results](#10-how-to-validate-results)
11. [Common Failures & Fixes](#11-common-failures--fixes)
12. [Signal Field Reference](#12-signal-field-reference)

---

## 1. Architecture Overview

Every signal fired into the system passes through a 4-stage firewall inside `pipeline/firewall.py`.
Failures at any stage push a structured error to `Error_Stream` (Redis DB 14), consumed by `Executor_Error/exec_error.py`.

```
Redis Stream: Algo_Signal
        │
        ▼
┌───────────────────────────────────────────────────────────────┐
│  _process_message()                                           │
│                                                               │
│  [MCXFO path] ── Multiplier adjustment (before Stage 1)       │
│                                                               │
│  Stage 1 ── signal_field_check()   ← pure Python, no I/O     │
│  Stage 2 ── algo_check()           ← reads _MEMORY + Redis   │
│  Stage 3 ── segment_check()        ← reads _MEMORY           │
│  Stage 4 ── client_check_and_fanout() ← fan-out to clients   │
└───────────────────────────────────────────────────────────────┘
        │ (on failure at any stage)
        ▼
Redis Stream: Error_Stream
        │
        ▼
ExecErrorProcessor (exec_error.py)
  ├── order_data               → close gate + log + alarm
  ├── pending_order            → ack only
  ├── AlgoSignalValidationError → close gates for mapped clients
  └── ClientOrderValidationError → close gate + log + alarm
```

---

## 2. Test Infrastructure

### Helpers defined at module level

| Helper | Purpose |
|---|---|
| `valid_signal(...)` | Returns a complete, structurally valid signal dict. All fields can be overridden via keyword args. |
| `INSTR_NSEFO` | Realistic instrument dict for NSEFO (LotSize=30, Series=FUTIDX) |
| `INSTR_NSECM` | Realistic instrument dict for NSECM (LotSize=1, Series=EQ) |
| `make_algo_cfg(...)` | Returns a full algo config dict. Defaults represent a permissive live config. |
| `_make_full_memory(...)` | Returns a complete `_MEMORY` dict with TestAlgo config and one client (C1). |
| `_make_redis_cfg(...)` | Returns a MagicMock Redis that passes rate checks and returns `counter_result` for incrby. |
| `_SENTINEL` | Sentinel object used in helper signatures to distinguish "not provided" from `None`. |

### What gets mocked

All tests beyond Stage 1 mock:

| Mock target | What it replaces |
|---|---|
| `fw._MEMORY` | In-process config cache (algo configs, segment configs, client RMS) |
| `pipeline.firewall.get_instrument` | Two-level instrument lookup (in-memory + Redis DB 11) |
| `fw._proc_redis_cfg` | Redis DB 8 — rate counters |
| `fw._proc_redis_streams` | Redis DB 14 — error stream + xack |
| `fw._proc_redis_throttler` | Redis — order fanout stream |
| `pipeline.firewall._push_error` | Error publishing function (asserted on, not executed) |

---

## 3. Stage 1 — Signal Field Check

**Class:** `TestSignalFieldCheck`  
**Function:** `signal_field_check(signal: dict) -> None`  
**Type:** Pure Python — no Redis, no MongoDB, no mocks needed.

### What it validates

- All 11 required fields present and non-empty
- `orderQuantity` is a positive number (> 0)
- `limitPrice` is zero or a positive number (≥ 0)
- `orderSide` is exactly `"BUY"` or `"SELL"`
- `algoName` is at least 2 characters

### Test cases

| Test ID | Input | Expected |
|---|---|---|
| `test_valid_buy_passes` | Complete valid signal, side=BUY | No exception |
| `test_valid_sell_passes` | Complete valid signal, side=SELL | No exception |
| `test_valid_zero_limit_price_passes` | limitPrice="0" | No exception (market-style allowed) |
| `test_valid_float_quantity_passes` | orderQuantity="30.0" | No exception |
| `test_algo_name_exactly_two_chars_passes` | algoName="AB" | No exception (minimum = 2) |
| `test_missing_required_field_raises[<field>]` | Each of 11 required fields deleted | `FieldValidationError`, `.field == <field>` |
| `test_empty_string_field_raises[<field>]` | Each required field set to `""` | `FieldValidationError`, `.field == <field>` |
| `test_none_string_field_raises[<field>]` | Each required field set to `"None"` | `FieldValidationError`, `.field == <field>` |
| `test_invalid_order_side_raises[HOLD]` | orderSide="HOLD" | `FieldValidationError` field=orderSide |
| `test_invalid_order_side_raises[buy]` | orderSide="buy" (lowercase) | `FieldValidationError` field=orderSide |
| `test_invalid_order_side_raises[sell]` | orderSide="sell" (lowercase) | `FieldValidationError` field=orderSide |
| `test_invalid_order_side_raises[LONG]` | orderSide="LONG" | `FieldValidationError` field=orderSide |
| `test_invalid_order_side_raises[SHORT]` | orderSide="SHORT" | `FieldValidationError` field=orderSide |
| `test_invalid_order_side_raises[0]` | orderSide="0" | `FieldValidationError` field=orderSide |
| `test_invalid_order_side_raises[]` | orderSide="" | `FieldValidationError` field=orderSide |
| `test_invalid_order_quantity_raises[0]` | orderQuantity="0" | `FieldValidationError` field=orderQuantity |
| `test_invalid_order_quantity_raises[-30]` | orderQuantity="-30" | `FieldValidationError` field=orderQuantity |
| `test_invalid_order_quantity_raises[-1]` | orderQuantity="-1" | `FieldValidationError` field=orderQuantity |
| `test_invalid_order_quantity_raises[abc]` | orderQuantity="abc" | `FieldValidationError` field=orderQuantity |
| `test_invalid_order_quantity_raises[0.0]` | orderQuantity="0.0" | `FieldValidationError` field=orderQuantity |
| `test_invalid_limit_price_raises[-1]` | limitPrice="-1" | `FieldValidationError` field=limitPrice |
| `test_invalid_limit_price_raises[-0.01]` | limitPrice="-0.01" | `FieldValidationError` field=limitPrice |
| `test_invalid_limit_price_raises[bad_price]` | limitPrice="bad_price" | `FieldValidationError` field=limitPrice |
| `test_invalid_algo_name_raises[]` | algoName="" | `FieldValidationError` field=algoName |
| `test_invalid_algo_name_raises[X]` | algoName="X" (single char) | `FieldValidationError` field=algoName |
| `test_invalid_algo_name_raises[ ]` | algoName=" " (whitespace only) | `FieldValidationError` field=algoName |

**Total: 56 tests** (11 fields × 3 parametrized groups + 12 explicit cases)

---

## 4. Stage 2 — Algo Check

**Class:** `TestAlgoCheck`  
**Function:** `algo_check(signal: dict) -> dict`  
**Mocks:** `_MEMORY`, `get_instrument`, `_proc_redis_cfg`

### What it validates

- Instrument exists in masterfile
- `ExchangeSegment` in signal matches masterfile value
- Algo exists in config and has `status=True`
- Signal's segment is inside `allowed_segments` (if configured)
- `orderQuantity >= lot_size` and `orderQuantity % lot_size == 0`
- Lots per trade ≤ `per_trade_lot_limit`
- Order value ≤ `max_value_of_symbol_check` (if set)
- Per-second rate limit passes (Lua atomic check)
- Daily trade count ≤ `trade_limit_per_day` (if set)
- Daily lot count ≤ `daily_lot_limit` (if set)

### Test cases

| Test ID | Setup | Expected |
|---|---|---|
| `test_valid_signal_returns_algo_cfg` | All defaults | Returns algo config dict with `status=True` |
| `test_instrument_not_found_raises` | `get_instrument` → `None` | `RuntimeError` "not found" |
| `test_segment_mismatch_raises` | Instrument has NSECM, signal says NSEFO | `ValueError` "segment mismatch" |
| `test_unknown_algo_raises` | algoName="GhostAlgo" not in memory | `KeyError` "no entry" |
| `test_inactive_algo_raises` | `status=False` | `RuntimeError` "inactive" |
| `test_segment_not_in_allowed_segments_raises` | allowed=["NSECM"], signal=NSEFO | `RuntimeError` "allowed_segments" |
| `test_segment_in_allowed_segments_passes` | allowed=["NSEFO"], signal=NSEFO | Returns algo config |
| `test_qty_less_than_lot_size_raises` | qty=1, lot_size=30 | `RuntimeError` "lot_size" |
| `test_qty_not_multiple_of_lot_size_raises` | qty=31, lot_size=30 | `RuntimeError` "not a multiple" |
| `test_qty_exact_lot_size_passes` | qty=30, lot_size=30 | No exception |
| `test_qty_multiple_lots_passes` | qty=90 (3 lots) | No exception |
| `test_per_trade_lot_limit_exceeded_raises` | limit=2, qty=90 (3 lots) | `RuntimeError` "per_trade_lot_limit" |
| `test_per_trade_lot_limit_at_boundary_passes` | limit=3, qty=90 (3 lots exactly) | No exception |
| `test_max_order_value_exceeded_raises` | max=100, qty=30, price=10 → 300 > 100 | `RuntimeError` "max_value_of_symbol_check" |
| `test_max_order_value_zero_means_unlimited` | max=0 (disabled), price=99999 | No exception |
| `test_per_second_rate_limit_exceeded_raises` | `redis.eval` returns -1 | `RuntimeError` "Rate limit exceeded" |
| `test_daily_trade_limit_exceeded_raises` | Pipeline incrby=999, limit=5 | `RuntimeError` "Daily trade limit" |
| `test_daily_lot_limit_exceeded_raises` | Second pipeline call=999, limit=10 | `RuntimeError` "Daily lot limit" |
| `test_daily_lot_limit_zero_means_unlimited` | limit=0 (disabled), counter=9999 | No exception |

**Total: 19 tests**

> **Redis mock note:** `_proc_rate_limit_sha` is `None` in tests (worker never initialised).  
> The code falls back to `redis.eval()` instead of `redis.evalsha()`. Both are mocked to `-1` for the rate-limit test.

---

## 5. Stage 3 — Segment Check

**Class:** `TestSegmentCheck`  
**Function:** `segment_check(signal: dict) -> None`  
**Mocks:** `_MEMORY["segment_rms_check"]`, `get_instrument`

> **Note:** OI and bid/ask spread checks (`_derivatives_check`, `_equity_check`) are currently commented out in production code. Tests cover only the active gateway checks.

### What it validates

- Instrument exists in masterfile
- Segment has a config entry in `segment_rms_check`

### Test cases

| Test ID | Setup | Expected |
|---|---|---|
| `test_valid_signal_passes` | NSEFO instrument + NSEFO segment config | No exception |
| `test_instrument_not_found_raises` | `get_instrument` → `None` | `RuntimeError` "not found" |
| `test_segment_config_not_found_raises` | `segment_rms_check = {}` | `RuntimeError` "no config for segment" |
| `test_unknown_segment_in_signal_raises` | Signal says BSEFO, only NSEFO in config | `RuntimeError` "no config for segment" |

**Total: 4 tests**

---

## 6. Stage 4 — Client Check & Fanout

**Class:** `TestClientCheckAndFanout`  
**Function:** `client_check_and_fanout(signal: dict, algo_cfg: dict) -> int`  
**Mocks:** `_MEMORY`, `_proc_redis_throttler`, `_proc_redis_streams`, `_push_error`

### What it validates

- Algo has at least one client mapped in `algo_clients`
- `quantity_multiplier × original_qty > 0` per client
- Broker payload is correctly built and pushed to `Client_API_Place_Order` stream
- Returns count of successfully fanned-out clients

### Test cases

| Test ID | Setup | Expected |
|---|---|---|
| `test_no_clients_returns_zero_and_no_xadd` | No entry for TestAlgo in `algo_clients` | Returns 0, `throttler.xadd` never called |
| `test_single_client_fanout_success` | One client C1, multiplier=1.0 | Returns 1, `throttler.xadd` called once |
| `test_multiple_clients_fanout_success` | Two clients C1 (×1.0), C2 (×2.0) | Returns 2, `throttler.xadd` called twice |
| `test_zero_multiplier_pushes_client_error` | multiplier=0.0 → qty becomes 0 | Returns 0, `_push_error` called with `error_type="ClientOrderValidationError"` |
| `test_negative_multiplier_pushes_client_error` | multiplier=-1.0 → negative qty | Returns 0, `_push_error` called once |
| `test_broker_payload_contains_client_id` | Single client C1 | Payload JSON has `clientID="C1"` and `algoName="TestAlgo"` |
| `test_quantity_multiplier_applied` | multiplier=2.0, qty=30 → 60 | Payload JSON has `orderQuantity=60` |

**Total: 7 tests**

---

## 7. MCXFO Multiplier Adjustment

**Class:** `TestMCXFOMultiplier`  
**Code path:** Inside `_process_message()`, before Stage 1, only when `exchangeSegment == "MCXFO"`

### What it does

MCX futures quantities are divided by the instrument's `Multiplier` field from the masterfile before the rest of the pipeline sees them.

```
adjusted_qty = float(signal["orderQuantity"]) / float(instrument["Multiplier"])
```

### Test cases

| Test ID | Instrument returned | Expected |
|---|---|---|
| `test_instrument_not_found_pushes_error` | `None` | `_push_error` called, reason contains "not found in masterfile" |
| `test_multiplier_field_missing_pushes_error` | No `Multiplier` key in instrument dict | `_push_error` called, reason contains "missing" |
| `test_multiplier_zero_pushes_error` | `Multiplier="0"` | `_push_error` called, reason contains "must be > 0" |
| `test_multiplier_negative_pushes_error` | `Multiplier="-2"` | `_push_error` called, reason contains "must be > 0" |
| `test_valid_multiplier_adjusts_qty_no_multiplier_error` | `Multiplier="10"`, qty=100 → adjusted=10 | `_push_error` NOT called about multiplier |

**Total: 5 tests**

---

## 8. Process Message Routing

**Class:** `TestProcessMessage`  
**Function:** `_process_message(msg_id: str, fields: dict) -> None`  
**Purpose:** End-to-end routing — verifies failures at each stage produce the correct `error_type` and `stage` in the pushed error.

### Signal entry formats

| `fields` dict shape | How it's handled |
|---|---|
| `{"payload": "<json string>"}` | JSON is parsed; signal is the resulting dict |
| Any other dict (no `payload` key) | Used directly as `signal = dict(fields)` |

### Test cases

| Test ID | Input | Expected `_push_error` call |
|---|---|---|
| `test_invalid_json_pushes_error` | `payload="not-json{"` | `error_type="AlgoSignalValidationError"`, reason="malformed..." |
| `test_json_array_payload_pushes_error` | `payload="[1,2,3]"` | reason contains "JSON object" |
| `test_empty_string_payload_pushes_error` | `payload=""` | `_push_error` called once |
| `test_json_null_payload_pushes_error` | `payload="null"` | `_push_error` called once |
| `test_stage1_invalid_side_pushes_field_error` | orderSide="HOLD" | `error_type="AlgoSignalValidationError"`, `stage="signal_field_check"`, reason contains "orderSide" |
| `test_stage1_zero_quantity_pushes_field_error` | orderQuantity="0" | `stage="signal_field_check"` |
| `test_stage1_missing_algo_name_pushes_field_error` | No `algoName` key | `stage="signal_field_check"` |
| `test_stage2_unknown_algo_pushes_algo_check_error` | algoName="GhostAlgo" | `error_type="AlgoSignalValidationError"`, `stage="algo_check"` |
| `test_stage2_instrument_not_found_pushes_algo_check_error` | `get_instrument` → None | `stage="algo_check"` |
| `test_stage3_no_segment_config_pushes_segment_check_error` | `segment_rms_check={}` | `stage="segment_check"` |
| `test_raw_fields_dict_is_processed` | Valid raw dict, no `payload` key | `_push_error` NOT called |
| `test_full_pipeline_success_no_error_pushed` | Valid signal, all mocks passing | `_push_error` NOT called |
| `test_full_pipeline_fanout_writes_to_throttler` | Valid signal, all mocks passing | `throttler.xadd` called once |

**Total: 13 tests**

---

## 9. ExecErrorProcessor (exec_error.py)

**Class:** `TestExecErrorProcessor`  
**File:** `Executor_Error/exec_error.py`  
**Type:** Async tests (`pytest-asyncio`, `asyncio_mode = auto`)

### Setup — `_make_processor()`

Builds an `ExecErrorProcessor` without touching real connections:
- Injects `AsyncMock` for `stream_redis`, `ping_redis`, `sym_redis`, `algo_redis`
- Injects `MagicMock` for all MongoDB collections
- Sets `is_running = True`

### 9.1 `error_type = "order_data"`

Triggered when Executor_Client reports a bad order response.

| Step | What happens |
|---|---|
| 1 | `close_exec_gate(order_data)` — writes `Stop` to `algo_redis`, updates MongoDB |
| 2 | `log_error_order(order_data, reason)` — inserts cancelled order to `final_response` + message to `algo_message` |
| 3 | `send_alarm(order_data, reason)` |
| 4 | `stream_redis.xack(error_stream, group, message_id)` |

**Test:** `test_order_data_error_closes_gate_and_acks`  
Asserts `algo_redis.set` called once + `stream_redis.xack` called once with correct args.

### 9.2 `error_type = "pending_order"`

Triggered for orders that timed out in pending state.

| Step | What happens |
|---|---|
| 1 | Logs the event |
| 2 | Acks only — **no gate close, no MongoDB write** |

**Test:** `test_pending_order_error_acks_only`  
Asserts `stream_redis.xack` called, `algo_redis.set` NOT called.

### 9.3 `error_type = "AlgoSignalValidationError"` — no `algoName` in signal

Triggered when Stage 1/2 failed before `algoName` was validated.

| Step | What happens |
|---|---|
| 1 | Sets `algoName = "UnknownAlgo"`, `clientID = "UnknownClient"` |
| 2 | Logs error order to MongoDB |
| 3 | Sends alarm with "AlgoName not found" |
| 4 | Acks |

**Test:** `test_algo_validation_error_unknown_algo_sends_alarm`  
Asserts `ping_redis.rpush` called + `stream_redis.xack` called once.

### 9.4 `error_type = "AlgoSignalValidationError"` — `algoName` present

Triggered when algo/segment RMS checks failed but the signal was structurally valid.

| Step | What happens |
|---|---|
| 1 | Reads `client_action_map: {algoName}` from Redis (hash: clientID → "0"/"1") |
| 2 | For each client with status `"1"`: calls `close_exec_gate` + `log_error_order` |
| 3 | If map is empty: falls back to MongoDB `client_rms_params` |
| 4 | Sends alarm, acks |

**Test:** `test_algo_validation_error_with_algo_name_closes_mapped_clients`  
- `client_action_map = {"C1": "1", "C2": "0"}` → only C1 is active  
- Asserts `algo_redis.set` called exactly once for key `"TestAlgo_C1"`  
- Asserts `stream_redis.xack` called once

### 9.5 `error_type = "ClientOrderValidationError"`

Triggered when per-client fanout check fails (e.g. zero quantity after multiplier).

| Step | What happens |
|---|---|
| 1 | `close_exec_gate(order_data)` |
| 2 | `log_error_order(order_data, reason)` |
| 3 | Sends alarm |
| 4 | Acks |

**Test:** `test_client_order_validation_error_closes_gate`  
Asserts `algo_redis.set` called once + `stream_redis.xack` called once.

### 9.6 Helper method tests

| Test | Function | Asserts |
|---|---|---|
| `test_close_exec_gate_sets_redis_and_mongo` | `close_exec_gate` | `algo_redis.set("TestAlgo_C1", '{"action": "Stop"}')` awaited; `update_one` called on `client_rms_params` |
| `test_log_error_order_inserts_to_response_and_monitor` | `log_error_order` | `response_db.insert_one` called with `orderStatus="Cancelled"` and `cancelrejectreason="M&M RMS: test reason"`; `monitor_db.insert_one` called |
| `test_send_alarm_pushes_to_ping_redis` | `send_alarm` | `ping_redis.rpush` called with `"alarms-0"` and `"alarms-v2"` |

**Total: 8 tests**

---

## 10. How to Validate Results

### Run all tests

```bash
cd /home/ubuntu/gokul_oms
pytest tests/test_firewall.py -v
```

Expected final line:
```
112 passed in ~0.3s
```

### Run one class at a time

```bash
pytest tests/test_firewall.py::TestSignalFieldCheck -v       # Stage 1  (56 tests)
pytest tests/test_firewall.py::TestAlgoCheck -v              # Stage 2  (19 tests)
pytest tests/test_firewall.py::TestSegmentCheck -v           # Stage 3  (4 tests)
pytest tests/test_firewall.py::TestClientCheckAndFanout -v   # Stage 4  (7 tests)
pytest tests/test_firewall.py::TestMCXFOMultiplier -v        # MCXFO    (5 tests)
pytest tests/test_firewall.py::TestProcessMessage -v         # Routing  (13 tests)
pytest tests/test_firewall.py::TestExecErrorProcessor -v     # ExecError (8 tests)
```

### What each outcome means

| Outcome | Meaning |
|---|---|
| `PASSED` | Function behaved exactly as documented |
| `FAILED: DID NOT RAISE` | Firewall did NOT reject a bad signal — validation gap in production |
| `FAILED: AssertionError on error_type` | Wrong error type pushed — routing is broken |
| `FAILED: AssertionError on stage` | Error attributed to the wrong stage |
| `FAILED: assert mock.call_count == 1` | Function was called 0 or 2+ times instead of once |
| `FAILED: ModuleNotFoundError` | Import path issue — see Section 11 |

### Live end-to-end check (real Redis)

```bash
# Fire one correct signal
cd /home/ubuntu/gokul_oms/tests
bash signal.sh

# Inject full test battery (valid + failing cases)
python3 inject_test_signals.py
```

Then inspect in Redis:
```bash
# How many errors landed in Error_Stream
redis-cli -n 14 XLEN Error_Stream

# Read the last 5 errors
redis-cli -n 14 XREVRANGE Error_Stream + - COUNT 5

# Check if Algo_Signal has been drained
redis-cli -n 14 XLEN Algo_Signal
```

---

## 11. Common Failures & Fixes

| Error | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: pipeline` | Not running from project root | `cd /home/ubuntu/gokul_oms` before `pytest` |
| `ModuleNotFoundError: stream_logger` | Executor_Error path not in sys.path | Already handled inside `_make_processor()` |
| `ModuleNotFoundError: pytest` | pytest not installed in active venv | `pip install pytest pytest-asyncio` |
| `DID NOT RAISE` on rate limit test | `_proc_rate_limit_sha` is None → code uses `eval` not `evalsha` | Mock both: `redis.evalsha.return_value = -1` and `redis.eval.return_value = -1` |
| `IndexError: tuple index out of range` on `call_args[0]` | `_push_error` uses keyword args | Use `mock.call_args.kwargs["error_type"]` not `mock.call_args[0][0]` |
| `RuntimeError` in `finally` block | `_proc_redis_streams` not mocked for that test | Add `patch.object(fw, "_proc_redis_streams", MagicMock())` |
| Async tests not collected or fail with `no event loop` | asyncio mode not set | `pytest.ini` must have `asyncio_mode = auto` |
| `PytestUnraisableExceptionWarning` on close | Async mock not properly closed | Use `AsyncMock` for all async Redis/Mongo handles |

---

## 12. Signal Field Reference

The 11 required fields validated in Stage 1:

| Field | Expected type | Rules |
|---|---|---|
| `exchangeSegment` | string | Non-empty, not `"None"` |
| `exchangeInstrumentID` | string | Non-empty, not `"None"` |
| `productType` | string | Non-empty, not `"None"` |
| `orderType` | string | Non-empty, not `"None"` |
| `orderSide` | string | Exactly `"BUY"` or `"SELL"` |
| `orderQuantity` | numeric string | Parses to `float > 0` |
| `limitPrice` | numeric string | Parses to `float >= 0` |
| `stopPrice` | string | Non-empty, not `"None"` |
| `algoName` | string | Non-empty, length >= 2 after strip |
| `extraPercent` | string | Non-empty, not `"None"` |
| `algoTime` | string | Non-empty, not `"None"` |
