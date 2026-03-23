"""
test_firewall.py
----------------
Unit tests for pipeline/firewall.py — all 4 stages, MCXFO multiplier,
_process_message routing, and ExecErrorProcessor (exec_error.py).

Run:
    pytest tests/test_firewall.py -v

Validation guide (at bottom of file).
"""

import json
import time
import sys
import os
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.firewall import (
    signal_field_check,
    FieldValidationError,
    algo_check,
    segment_check,
    client_check_and_fanout,
    _process_message,
    REQUIRED_SIGNAL_FIELDS,
)
import pipeline.firewall as fw


# ─────────────────────────────────────────────────────────────────────────────
# Shared fixtures / helpers
# ─────────────────────────────────────────────────────────────────────────────

_SENTINEL = object()   # used instead of None for "not provided" args


def valid_signal(
    seg="NSEFO",
    iid="66647",
    side="BUY",
    qty="30",
    price="150.0",
    algo="TestAlgo",
    product="NRML",
    order_type="LIMIT",
):
    """Return a complete, structurally valid signal dict."""
    return {
        "exchangeSegment":      seg,
        "exchangeInstrumentID": iid,
        "productType":          product,
        "orderType":            order_type,
        "orderSide":            side,
        "orderQuantity":        qty,
        "limitPrice":           price,
        "stopPrice":            "0",
        "algoName":             algo,
        "extraPercent":         "0.5",
        "algoTime":             str(int(time.time())),
    }


# Realistic instrument dict (mirrors what comes out of Redis HGETALL)
INSTR_NSEFO = {
    "ExchangeSegment": "NSEFO",
    "LotSize":         "30",
    "Series":          "FUTIDX",
    "Multiplier":      "1",
}

INSTR_NSECM = {
    "ExchangeSegment": "NSECM",
    "LotSize":         "1",
    "Series":          "EQ",
    "Multiplier":      "1",
}


def make_algo_cfg(
    status=True,
    trade_limit_per_second=0,
    trade_limit_per_day=0,
    allowed_segments=None,
    lot_size=30,
    per_trade_lot_limit=100,
    daily_lot_limit=0,
    max_value_of_symbol_check=0,
):
    return {
        "status":                    status,
        "trade_limit_per_second":    trade_limit_per_second,
        "trade_limit_per_day":       trade_limit_per_day,
        "allowed_segments":          allowed_segments or [],
        "lot_size":                  lot_size,
        "lot_size_multiple":         1,
        "per_trade_lot_limit":       per_trade_lot_limit,
        "daily_lot_limit":           daily_lot_limit,
        "max_value_of_symbol_check": max_value_of_symbol_check,
    }


def _make_full_memory(algo_cfg=None, clients=None, client_rms=None, seg_cfg=None):
    return {
        "algo_configs":      {"TestAlgo": algo_cfg or make_algo_cfg()},
        "segment_rms_check": {"NSEFO": seg_cfg or {"oi_threshold": 0, "max_spread_pct": 100}},
        "instruments":       {},
        "client_rms":        client_rms or {"C1:TestAlgo": {"quantity_multiplier": 1.0, "broker": "XTSB"}},
        "algo_clients":      {"TestAlgo": clients or ["C1"]},
    }


def _make_redis_cfg(rate_result=1, counter_result=1):
    """Mock redis that passes rate checks and returns *counter_result* for incrby."""
    r = MagicMock()
    r.evalsha.return_value = rate_result
    pipe = MagicMock()
    pipe.execute.return_value = [counter_result, True]
    r.pipeline.return_value = pipe
    return r


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 1 — signal_field_check  (pure Python, zero I/O)
# ═════════════════════════════════════════════════════════════════════════════

class TestSignalFieldCheck:
    """
    VALIDATION: each test calls signal_field_check() directly (no Redis).
    Pass = no exception raised.  Fail = FieldValidationError with .field set.
    """

    def test_valid_buy_passes(self, log_signal):
        sig = valid_signal()
        log_signal(sig)
        signal_field_check(sig)

    def test_valid_sell_passes(self, log_signal):
        sig = valid_signal(side="SELL")
        log_signal(sig)
        signal_field_check(sig)

    def test_valid_zero_limit_price_passes(self, log_signal):
        """limitPrice=0 is allowed (market-style order)."""
        sig = valid_signal(price="0")
        log_signal(sig)
        signal_field_check(sig)

    def test_valid_float_quantity_passes(self, log_signal):
        """'30.0' should be accepted and treated as 30."""
        sig = valid_signal(qty="30.0")
        log_signal(sig)
        signal_field_check(sig)

    def test_algo_name_exactly_two_chars_passes(self, log_signal):
        sig = valid_signal(algo="AB")
        log_signal(sig)
        signal_field_check(sig)

    # ── Missing required fields ───────────────────────────────────────────────

    @pytest.mark.parametrize("field", REQUIRED_SIGNAL_FIELDS)
    def test_missing_required_field_raises(self, field, log_signal):
        sig = {k: v for k, v in valid_signal().items() if k != field}
        log_signal(sig)
        with pytest.raises(FieldValidationError) as exc_info:
            signal_field_check(sig)
        assert exc_info.value.field == field

    # ── Empty / "None" string values ─────────────────────────────────────────

    @pytest.mark.parametrize("field", REQUIRED_SIGNAL_FIELDS)
    def test_empty_string_field_raises(self, field, log_signal):
        sig = {**valid_signal(), field: ""}
        log_signal(sig)
        with pytest.raises(FieldValidationError) as exc_info:
            signal_field_check(sig)
        assert exc_info.value.field == field

    @pytest.mark.parametrize("field", REQUIRED_SIGNAL_FIELDS)
    def test_none_string_field_raises(self, field, log_signal):
        sig = {**valid_signal(), field: "None"}
        log_signal(sig)
        with pytest.raises(FieldValidationError) as exc_info:
            signal_field_check(sig)
        assert exc_info.value.field == field

    # ── orderSide ─────────────────────────────────────────────────────────────

    @pytest.mark.parametrize("bad_side", ["HOLD", "buy", "sell", "LONG", "SHORT", "0", ""])
    def test_invalid_order_side_raises(self, bad_side, log_signal):
        sig = {**valid_signal(), "orderSide": bad_side}
        log_signal(sig)
        with pytest.raises(FieldValidationError) as exc_info:
            signal_field_check(sig)
        assert exc_info.value.field == "orderSide"

    # ── orderQuantity ─────────────────────────────────────────────────────────

    @pytest.mark.parametrize("bad_qty", ["0", "-30", "-1", "abc", "0.0"])
    def test_invalid_order_quantity_raises(self, bad_qty, log_signal):
        sig = {**valid_signal(), "orderQuantity": bad_qty}
        log_signal(sig)
        with pytest.raises(FieldValidationError) as exc_info:
            signal_field_check(sig)
        assert exc_info.value.field == "orderQuantity"

    # ── limitPrice ────────────────────────────────────────────────────────────

    @pytest.mark.parametrize("bad_price", ["-1", "-0.01", "bad_price"])
    def test_invalid_limit_price_raises(self, bad_price, log_signal):
        sig = {**valid_signal(), "limitPrice": bad_price}
        log_signal(sig)
        with pytest.raises(FieldValidationError) as exc_info:
            signal_field_check(sig)
        assert exc_info.value.field == "limitPrice"

    # ── algoName ─────────────────────────────────────────────────────────────

    @pytest.mark.parametrize("bad_algo", ["", "X", " "])
    def test_invalid_algo_name_raises(self, bad_algo, log_signal):
        sig = {**valid_signal(), "algoName": bad_algo}
        log_signal(sig)
        with pytest.raises(FieldValidationError) as exc_info:
            signal_field_check(sig)
        assert exc_info.value.field == "algoName"


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 2 — algo_check
# ═════════════════════════════════════════════════════════════════════════════

class TestAlgoCheck:
    """
    VALIDATION: patches _MEMORY, get_instrument, and _proc_redis_cfg.
    algo_check() should return the algo_cfg dict on success.
    """

    def _run(self, sig, memory=None, instrument=_SENTINEL, redis_cfg=None):
        mem  = memory    if memory    is not None else _make_full_memory()
        inst = INSTR_NSEFO if instrument is _SENTINEL else instrument
        rcfg = redis_cfg if redis_cfg is not None else _make_redis_cfg()
        with patch.object(fw, "_MEMORY", mem), \
             patch.object(fw, "_proc_redis_cfg", rcfg), \
             patch("pipeline.firewall.get_instrument", return_value=inst):
            return algo_check(sig)

    # ── Happy path ────────────────────────────────────────────────────────────

    def test_valid_signal_returns_algo_cfg(self, log_signal):
        sig = valid_signal()
        log_signal(sig)
        result = self._run(sig)
        assert result["status"] is True

    # ── Instrument checks ─────────────────────────────────────────────────────

    def test_instrument_not_found_raises(self, log_signal):
        sig = valid_signal()
        log_signal({"signal": sig, "instrument": None})
        with pytest.raises(RuntimeError, match="not found"):
            self._run(sig, instrument=None)

    def test_segment_mismatch_raises(self, log_signal):
        sig  = valid_signal(seg="NSEFO")
        wrong = {**INSTR_NSEFO, "ExchangeSegment": "NSECM"}
        log_signal({"signal": sig, "instrument_segment": "NSECM"})
        with pytest.raises(ValueError, match="segment mismatch"):
            self._run(sig, instrument=wrong)

    # ── Algo config checks ────────────────────────────────────────────────────

    def test_unknown_algo_raises(self, log_signal):
        sig = valid_signal(algo="GhostAlgo")
        log_signal(sig)
        with pytest.raises(KeyError, match="no entry"):
            self._run(sig)

    def test_inactive_algo_raises(self, log_signal):
        sig = valid_signal()
        log_signal({"signal": sig, "algo_status": False})
        mem = _make_full_memory(algo_cfg=make_algo_cfg(status=False))
        with pytest.raises(RuntimeError, match="inactive"):
            self._run(sig, memory=mem)

    def test_segment_not_in_allowed_segments_raises(self, log_signal):
        sig = valid_signal(seg="NSEFO")
        log_signal({"signal": sig, "allowed_segments": ["NSECM"]})
        mem = _make_full_memory(algo_cfg=make_algo_cfg(allowed_segments=["NSECM"]))
        with pytest.raises(RuntimeError, match="allowed_segments"):
            self._run(sig, memory=mem)

    def test_segment_in_allowed_segments_passes(self, log_signal):
        sig = valid_signal(seg="NSEFO")
        log_signal({"signal": sig, "allowed_segments": ["NSEFO"]})
        mem = _make_full_memory(algo_cfg=make_algo_cfg(allowed_segments=["NSEFO"]))
        result = self._run(sig, memory=mem)
        assert result is not None

    # ── Lot / quantity checks ─────────────────────────────────────────────────

    def test_qty_less_than_lot_size_raises(self, log_signal):
        sig = valid_signal(qty="1")
        log_signal({"signal": sig, "lot_size": 30})
        with pytest.raises(RuntimeError, match="lot_size"):
            self._run(sig)

    def test_qty_not_multiple_of_lot_size_raises(self, log_signal):
        sig = valid_signal(qty="31")
        log_signal({"signal": sig, "lot_size": 30})
        with pytest.raises(RuntimeError, match="not a multiple"):
            self._run(sig)

    def test_qty_exact_lot_size_passes(self, log_signal):
        sig = valid_signal(qty="30")
        log_signal({"signal": sig, "lot_size": 30})
        self._run(sig)

    def test_qty_multiple_lots_passes(self, log_signal):
        sig = valid_signal(qty="90")
        log_signal({"signal": sig, "lots": 3, "lot_size": 30})
        self._run(sig)

    def test_per_trade_lot_limit_exceeded_raises(self, log_signal):
        sig = valid_signal(qty="90")
        log_signal({"signal": sig, "per_trade_lot_limit": 2, "lots_in_signal": 3})
        mem = _make_full_memory(algo_cfg=make_algo_cfg(per_trade_lot_limit=2))
        with pytest.raises(RuntimeError, match="per_trade_lot_limit"):
            self._run(sig, memory=mem)

    def test_per_trade_lot_limit_at_boundary_passes(self, log_signal):
        sig = valid_signal(qty="90")
        log_signal({"signal": sig, "per_trade_lot_limit": 3, "lots_in_signal": 3})
        mem = _make_full_memory(algo_cfg=make_algo_cfg(per_trade_lot_limit=3))
        self._run(sig, memory=mem)

    # ── Order value checks ────────────────────────────────────────────────────

    def test_max_order_value_exceeded_raises(self, log_signal):
        sig = valid_signal(qty="30", price="10.0")
        log_signal({"signal": sig, "order_value": 300, "max_value": 100})
        mem = _make_full_memory(algo_cfg=make_algo_cfg(max_value_of_symbol_check=100.0))
        with pytest.raises(RuntimeError, match="max_value_of_symbol_check"):
            self._run(sig, memory=mem)

    def test_max_order_value_zero_means_unlimited(self, log_signal):
        """max_value=0 disables the check — any order value should pass."""
        sig = valid_signal(qty="30", price="99999.0")
        log_signal({"signal": sig, "max_value": 0, "note": "disabled"})
        mem = _make_full_memory(algo_cfg=make_algo_cfg(max_value_of_symbol_check=0))
        self._run(sig, memory=mem)

    # ── Rate / counter checks ─────────────────────────────────────────────────

    def test_per_second_rate_limit_exceeded_raises(self, log_signal):
        """eval/evalsha returns -1 → Lua script rejected the increment."""
        sig  = valid_signal()
        log_signal({"signal": sig, "trade_limit_per_second": 5, "redis_eval_result": -1})
        rcfg = _make_redis_cfg(rate_result=-1)
        rcfg.eval.return_value = -1
        mem  = _make_full_memory(algo_cfg=make_algo_cfg(trade_limit_per_second=5))
        with pytest.raises(RuntimeError, match="Rate limit exceeded"):
            self._run(sig, memory=mem, redis_cfg=rcfg)

    def test_daily_trade_limit_exceeded_raises(self, log_signal):
        """Pipeline incrby returns 999 → daily trade counter is over limit."""
        sig  = valid_signal()
        log_signal({"signal": sig, "trade_limit_per_day": 5, "redis_counter": 999})
        rcfg = _make_redis_cfg(rate_result=1, counter_result=999)
        mem  = _make_full_memory(algo_cfg=make_algo_cfg(trade_limit_per_day=5))
        with pytest.raises(RuntimeError, match="Daily trade limit"):
            self._run(sig, memory=mem, redis_cfg=rcfg)

    def test_daily_lot_limit_exceeded_raises(self, log_signal):
        """First pipeline call (trade_count) is OK; second (lot_count) is over limit."""
        sig  = valid_signal()
        log_signal({"signal": sig, "daily_lot_limit": 10, "lot_counter": 999})
        rcfg = _make_redis_cfg(rate_result=1)
        pipe = MagicMock()
        pipe.execute.side_effect = [[1, True], [999, True]]
        rcfg.pipeline.return_value = pipe
        mem = _make_full_memory(algo_cfg=make_algo_cfg(daily_lot_limit=10))
        with pytest.raises(RuntimeError, match="Daily lot limit"):
            self._run(sig, memory=mem, redis_cfg=rcfg)

    def test_daily_lot_limit_zero_means_unlimited(self, log_signal):
        sig  = valid_signal()
        log_signal({"signal": sig, "daily_lot_limit": 0, "lot_counter": 9999, "note": "disabled"})
        mem = _make_full_memory(algo_cfg=make_algo_cfg(daily_lot_limit=0))
        pipe = MagicMock()
        pipe.execute.return_value = [9999, True]
        rcfg = _make_redis_cfg()
        rcfg.pipeline.return_value = pipe
        self._run(sig, memory=mem, redis_cfg=rcfg)


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 3 — segment_check
# ═════════════════════════════════════════════════════════════════════════════

class TestSegmentCheck:
    """
    VALIDATION: currently segment_check only validates that the instrument
    and segment config exist; OI/spread checks are commented out.
    Tests verify gating behaviour for those two lookups.
    """

    def _run(self, sig, instrument=_SENTINEL, seg_cfg=_SENTINEL):
        inst = INSTR_NSEFO if instrument is _SENTINEL else instrument
        scfg = {"oi_threshold": 0, "max_spread_pct": 100} if seg_cfg is _SENTINEL else seg_cfg
        memory = {
            "algo_configs":      {},
            "segment_rms_check": {"NSEFO": scfg} if scfg is not None else {},
            "instruments":       {},
            "client_rms":        {},
            "algo_clients":      {},
        }
        with patch.object(fw, "_MEMORY", memory), \
             patch("pipeline.firewall.get_instrument", return_value=inst):
            segment_check(sig)

    def test_valid_signal_passes(self, log_signal):
        sig = valid_signal()
        log_signal(sig)
        self._run(sig)

    def test_instrument_not_found_raises(self, log_signal):
        sig = valid_signal()
        log_signal({"signal": sig, "instrument": None})
        with pytest.raises(RuntimeError, match="not found"):
            self._run(sig, instrument=None)

    def test_segment_config_not_found_raises(self, log_signal):
        sig = valid_signal()
        log_signal({"signal": sig, "segment_rms_check": {}})
        with pytest.raises(RuntimeError, match="no config for segment"):
            self._run(sig, seg_cfg=None)

    def test_unknown_segment_in_signal_raises(self, log_signal):
        """Signal references a segment that has no config entry."""
        sig = valid_signal(seg="BSEFO")
        log_signal({"signal": sig, "note": "BSEFO not in segment_rms_check"})
        with pytest.raises(RuntimeError, match="no config for segment"):
            self._run(sig)


# ═════════════════════════════════════════════════════════════════════════════
# STAGE 4 — client_check_and_fanout
# ═════════════════════════════════════════════════════════════════════════════

class TestClientCheckAndFanout:
    """
    VALIDATION: patches _MEMORY, _proc_redis_throttler, and _push_error.
    Asserts fanout count and whether errors were pushed.
    """

    def _run(self, sig, memory=None, expect_errors=False):
        mem       = memory or _make_full_memory()
        throttler = MagicMock()
        streams   = MagicMock()
        with patch.object(fw, "_MEMORY", mem), \
             patch.object(fw, "_proc_redis_throttler", throttler), \
             patch.object(fw, "_proc_redis_streams", streams), \
             patch("pipeline.firewall._push_error") as mock_push:
            count = client_check_and_fanout(sig, make_algo_cfg())
        if not expect_errors:
            mock_push.assert_not_called()
        return count, throttler, mock_push

    def test_no_clients_returns_zero_and_no_xadd(self, log_signal):
        sig = valid_signal()
        log_signal({"signal": sig, "algo_clients": {}})
        mem = _make_full_memory()
        mem["algo_clients"] = {}
        count, throttler, _ = self._run(sig, memory=mem)
        assert count == 0
        throttler.xadd.assert_not_called()

    def test_single_client_fanout_success(self, log_signal):
        sig = valid_signal()
        log_signal({"signal": sig, "clients": ["C1"], "multiplier_C1": 1.0})
        count, throttler, _ = self._run(sig)
        assert count == 1
        throttler.xadd.assert_called_once()

    def test_multiple_clients_fanout_success(self, log_signal):
        sig = valid_signal()
        client_rms = {
            "C1:TestAlgo": {"quantity_multiplier": 1.0, "broker": "XTSB"},
            "C2:TestAlgo": {"quantity_multiplier": 2.0, "broker": "XTSB"},
        }
        log_signal({"signal": sig, "clients": ["C1", "C2"], "multiplier_C1": 1.0, "multiplier_C2": 2.0})
        mem = _make_full_memory(clients=["C1", "C2"], client_rms=client_rms)
        count, throttler, _ = self._run(sig, memory=mem)
        assert count == 2
        assert throttler.xadd.call_count == 2

    def test_zero_multiplier_pushes_client_error(self, log_signal):
        """quantity_multiplier=0 makes qty → 0 which must be rejected."""
        sig = valid_signal()
        client_rms = {"C1:TestAlgo": {"quantity_multiplier": 0.0, "broker": ""}}
        log_signal({"signal": sig, "multiplier_C1": 0.0, "expected_qty": 0})
        mem = _make_full_memory(client_rms=client_rms)
        count, _, mock_push = self._run(sig, memory=mem, expect_errors=True)
        assert count == 0
        mock_push.assert_called_once()
        assert mock_push.call_args.kwargs["error_type"] == "ClientOrderValidationError"

    def test_negative_multiplier_pushes_client_error(self, log_signal):
        """Negative multiplier → negative qty → should be rejected."""
        sig = valid_signal()
        client_rms = {"C1:TestAlgo": {"quantity_multiplier": -1.0, "broker": ""}}
        log_signal({"signal": sig, "multiplier_C1": -1.0, "expected_qty": -30})
        mem = _make_full_memory(client_rms=client_rms)
        count, _, mock_push = self._run(sig, memory=mem, expect_errors=True)
        assert count == 0
        mock_push.assert_called_once()

    def test_broker_payload_contains_client_id(self, log_signal):
        """The payload pushed to the throttler stream must contain clientID."""
        sig = valid_signal()
        log_signal({"signal": sig, "assert_payload_fields": ["clientID", "algoName"]})
        count, throttler, _ = self._run(sig)
        assert count == 1
        payload_str = throttler.xadd.call_args[0][1]["payload"]
        payload = json.loads(payload_str)
        assert payload["clientID"] == "C1"
        assert payload["algoName"] == "TestAlgo"

    def test_quantity_multiplier_applied(self, log_signal):
        """multiplier=2 → orderQuantity doubles in the broker payload."""
        sig = valid_signal(qty="30")
        client_rms = {"C1:TestAlgo": {"quantity_multiplier": 2.0, "broker": ""}}
        log_signal({"signal": sig, "multiplier": 2.0, "expected_qty_in_payload": 60})
        mem = _make_full_memory(client_rms=client_rms)
        _, throttler, _ = self._run(sig, memory=mem)
        payload = json.loads(throttler.xadd.call_args[0][1]["payload"])
        assert payload["orderQuantity"] == 60


# ═════════════════════════════════════════════════════════════════════════════
# MCXFO Multiplier Adjustment
# ═════════════════════════════════════════════════════════════════════════════

class TestMCXFOMultiplier:
    """
    VALIDATION: calls _process_message() directly with an MCXFO signal.
    All Redis handles are mocked; we assert on _push_error behaviour.
    """

    def _mcxfo_signal(self, iid="486502", qty="100"):
        return {
            "exchangeSegment":      "MCXFO",
            "exchangeInstrumentID": iid,
            "productType":          "NRML",
            "orderType":            "LIMIT",
            "orderSide":            "BUY",
            "orderQuantity":        qty,
            "limitPrice":           "9100",
            "stopPrice":            "0",
            "algoName":             "TestAlgo",
            "extraPercent":         "0.5",
            "algoTime":             str(int(time.time())),
        }

    def _run_process(self, sig, instrument, memory_extra=None):
        mem = {
            "algo_configs":      {"TestAlgo": make_algo_cfg(lot_size=1, allowed_segments=["MCXFO"])},
            "segment_rms_check": {"MCXFO": {"oi_threshold": 0, "max_spread_pct": 100}},
            "instruments":       {},
            "client_rms":        {"C1:TestAlgo": {"quantity_multiplier": 1.0, "broker": ""}},
            "algo_clients":      {"TestAlgo": ["C1"]},
        }
        if memory_extra:
            mem.update(memory_extra)
        rcfg = _make_redis_cfg()
        with patch("pipeline.firewall.get_instrument", return_value=instrument), \
             patch.object(fw, "_MEMORY", mem), \
             patch.object(fw, "_proc_redis_cfg",    rcfg), \
             patch.object(fw, "_proc_redis_streams", MagicMock()), \
             patch.object(fw, "_proc_redis_throttler", MagicMock()), \
             patch("pipeline.firewall._push_error") as mock_push:
            _process_message("mcxfo-msg", {"payload": json.dumps(sig)})
        return mock_push

    def test_instrument_not_found_pushes_error(self, log_signal):
        sig = self._mcxfo_signal()
        log_signal({"signal": sig, "instrument": None})
        mock_push = self._run_process(sig, instrument=None)
        mock_push.assert_called_once()
        assert "not found in masterfile" in mock_push.call_args[0][2]

    def test_multiplier_field_missing_pushes_error(self, log_signal):
        sig   = self._mcxfo_signal()
        instr = {"ExchangeSegment": "MCXFO", "LotSize": "1", "Series": "FUTCOM"}
        log_signal({"signal": sig, "instrument": instr, "note": "no Multiplier key"})
        mock_push = self._run_process(sig, instrument=instr)
        mock_push.assert_called_once()
        assert "missing" in mock_push.call_args[0][2]

    def test_multiplier_zero_pushes_error(self, log_signal):
        sig   = self._mcxfo_signal()
        instr = {"ExchangeSegment": "MCXFO", "LotSize": "1", "Series": "FUTCOM", "Multiplier": "0"}
        log_signal({"signal": sig, "instrument_Multiplier": "0"})
        mock_push = self._run_process(sig, instrument=instr)
        mock_push.assert_called_once()
        assert "must be > 0" in mock_push.call_args[0][2]

    def test_multiplier_negative_pushes_error(self, log_signal):
        sig   = self._mcxfo_signal()
        instr = {"ExchangeSegment": "MCXFO", "LotSize": "1", "Series": "FUTCOM", "Multiplier": "-2"}
        log_signal({"signal": sig, "instrument_Multiplier": "-2"})
        mock_push = self._run_process(sig, instrument=instr)
        mock_push.assert_called_once()
        assert "must be > 0" in mock_push.call_args[0][2]

    def test_valid_multiplier_adjusts_qty_no_multiplier_error(self, log_signal):
        """qty=100 / multiplier=10 = 10. Should not error at the multiplier stage."""
        sig   = self._mcxfo_signal(qty="100")
        instr = {"ExchangeSegment": "MCXFO", "LotSize": "1", "Series": "FUTCOM", "Multiplier": "10"}
        log_signal({"signal": sig, "instrument_Multiplier": "10", "adjusted_qty": 10})
        mock_push = self._run_process(sig, instrument=instr)
        if mock_push.called:
            reason = mock_push.call_args[0][2]
            assert "multiplier" not in reason.lower(), f"Unexpected multiplier error: {reason}"


# ═════════════════════════════════════════════════════════════════════════════
# _process_message — routing / integration
# ═════════════════════════════════════════════════════════════════════════════

class TestProcessMessage:
    """
    VALIDATION: calls _process_message() end-to-end with all handles mocked.
    Checks which error type + stage is pushed, or that no error is pushed.
    """

    def _ctx(self, memory=None, instrument=_SENTINEL):
        """Context manager stack that patches all Redis handles."""
        mem  = memory    if memory    is not None else _make_full_memory()
        inst = INSTR_NSEFO if instrument is _SENTINEL else instrument
        rcfg = _make_redis_cfg()
        return (
            patch.object(fw, "_MEMORY", mem),
            patch.object(fw, "_proc_redis_cfg",      rcfg),
            patch.object(fw, "_proc_redis_streams",  MagicMock()),
            patch.object(fw, "_proc_redis_throttler", MagicMock()),
            patch("pipeline.firewall.get_instrument", return_value=inst),
        )

    def _run(self, msg_fields, memory=None, instrument=_SENTINEL):
        patches = self._ctx(memory, instrument)
        with patches[0], patches[1], patches[2], patches[3], patches[4], \
             patch("pipeline.firewall._push_error") as mock_push:
            _process_message("test-msg", msg_fields)
        return mock_push

    # ── Malformed payload ─────────────────────────────────────────────────────

    def test_invalid_json_pushes_error(self, log_signal):
        fields = {"payload": "not-json{"}
        log_signal(fields)
        mock_push = self._run(fields)
        mock_push.assert_called_once()
        assert mock_push.call_args[0][0] == "AlgoSignalValidationError"
        assert "malformed" in mock_push.call_args[0][2]

    def test_json_array_payload_pushes_error(self, log_signal):
        fields = {"payload": json.dumps([1, 2, 3])}
        log_signal(fields)
        mock_push = self._run(fields)
        mock_push.assert_called_once()
        assert "JSON object" in mock_push.call_args[0][2]

    def test_empty_string_payload_pushes_error(self, log_signal):
        fields = {"payload": ""}
        log_signal(fields)
        mock_push = self._run(fields)
        mock_push.assert_called_once()

    def test_json_null_payload_pushes_error(self, log_signal):
        fields = {"payload": "null"}
        log_signal(fields)
        mock_push = self._run(fields)
        mock_push.assert_called_once()

    # ── Stage 1 failure routing ───────────────────────────────────────────────

    def test_stage1_invalid_side_pushes_field_error(self, log_signal):
        sig = valid_signal(side="HOLD")
        log_signal(sig)
        mock_push = self._run({"payload": json.dumps(sig)})
        mock_push.assert_called_once()
        error_type, _, reason, stage = mock_push.call_args[0]
        assert error_type == "AlgoSignalValidationError"
        assert stage == "signal_field_check"
        assert "orderSide" in reason

    def test_stage1_zero_quantity_pushes_field_error(self, log_signal):
        sig = valid_signal(qty="0")
        log_signal(sig)
        mock_push = self._run({"payload": json.dumps(sig)})
        mock_push.assert_called_once()
        assert mock_push.call_args[0][3] == "signal_field_check"

    def test_stage1_missing_algo_name_pushes_field_error(self, log_signal):
        sig = {k: v for k, v in valid_signal().items() if k != "algoName"}
        log_signal(sig)
        mock_push = self._run({"payload": json.dumps(sig)})
        mock_push.assert_called_once()
        assert mock_push.call_args[0][3] == "signal_field_check"

    # ── Stage 2 failure routing ───────────────────────────────────────────────

    def test_stage2_unknown_algo_pushes_algo_check_error(self, log_signal):
        sig = valid_signal(algo="GhostAlgo")
        log_signal(sig)
        mock_push = self._run({"payload": json.dumps(sig)})
        mock_push.assert_called_once()
        error_type, _, _reason, stage = mock_push.call_args[0]
        assert error_type == "AlgoSignalValidationError"
        assert stage == "algo_check"

    def test_stage2_instrument_not_found_pushes_algo_check_error(self, log_signal):
        sig = valid_signal()
        log_signal({"signal": sig, "instrument": None})
        mock_push = self._run({"payload": json.dumps(sig)}, instrument=None)
        mock_push.assert_called_once()
        assert mock_push.call_args[0][3] == "algo_check"

    # ── Stage 3 failure routing ───────────────────────────────────────────────

    def test_stage3_no_segment_config_pushes_segment_check_error(self, log_signal):
        sig = valid_signal()
        log_signal({"signal": sig, "segment_rms_check": {}})
        mem = _make_full_memory()
        mem["segment_rms_check"] = {}
        mock_push = self._run({"payload": json.dumps(sig)}, memory=mem)
        mock_push.assert_called_once()
        assert mock_push.call_args[0][3] == "segment_check"

    # ── Raw fields (no 'payload' wrapper) ────────────────────────────────────

    def test_raw_fields_dict_is_processed(self, log_signal):
        """Stream entries without a 'payload' key are read as dict(fields) directly."""
        raw = valid_signal()
        log_signal({"raw_fields": raw, "note": "no payload wrapper"})
        mock_push = self._run(raw)
        mock_push.assert_not_called()

    # ── Full success ──────────────────────────────────────────────────────────

    def test_full_pipeline_success_no_error_pushed(self, log_signal):
        sig = valid_signal()
        log_signal(sig)
        mock_push = self._run({"payload": json.dumps(sig)})
        mock_push.assert_not_called()

    def test_full_pipeline_fanout_writes_to_throttler(self, log_signal):
        sig = valid_signal()
        log_signal(sig)
        throttler = MagicMock()
        mem   = _make_full_memory()
        rcfg  = _make_redis_cfg()
        with patch.object(fw, "_MEMORY", mem), \
             patch.object(fw, "_proc_redis_cfg",      rcfg), \
             patch.object(fw, "_proc_redis_streams",  MagicMock()), \
             patch.object(fw, "_proc_redis_throttler", throttler), \
             patch("pipeline.firewall.get_instrument", return_value=INSTR_NSEFO):
            _process_message("ok-msg", {"payload": json.dumps(sig)})
        throttler.xadd.assert_called_once()


# ═════════════════════════════════════════════════════════════════════════════
# ExecErrorProcessor — exec_error.py
# ═════════════════════════════════════════════════════════════════════════════

class TestExecErrorProcessor:
    """
    VALIDATION: builds an ExecErrorProcessor with all async connections mocked.
    Tests process_error() for each error_type branch, plus close_exec_gate
    and log_error_order helpers.

    Requires: pytest-asyncio  (pip install pytest-asyncio)
    """

    def _make_processor(self):
        """
        Build a minimally-initialised ExecErrorProcessor without touching
        real connections or sys.argv.
        """
        sys.argv = ["exec_error.py", "group1", "consumer_1"]

        # exec_error.py uses a local 'stream_logger' module inside Executor_Error/
        _exec_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "Executor_Error",
        )
        if _exec_dir not in sys.path:
            sys.path.insert(0, _exec_dir)

        from configparser import ConfigParser
        cfg = ConfigParser()
        cfg.read_dict({
            "params": {
                "error_stream":           "Error_Stream",
                "logging_stream":         "Logging_Stream",
                "processing_concurrency": "5",
            },
            "dbParams": {
                "redisHost": "localhost",
                "redisPort": "6379",
                "redisPass": "test",
            },
            "infraParams": {
                "redisHost":  "localhost",
                "redisPort":  "6379",
                "redisPass":  "test",
                "mongoHost":  "localhost",
                "mongoPort":  "27017",
                "mongoUser":  "user",
                "mongoPass":  "pass",
            },
        })

        from Executor_Error.exec_error import ExecErrorProcessor
        processor = ExecErrorProcessor(
            config=cfg,
            error_stream="Error_Stream",
            logging_stream="Logging_Stream",
            processing_concurrency=5,
        )

        # Inject mock Redis / Mongo handles
        processor.stream_redis = AsyncMock()
        processor.ping_redis   = AsyncMock()
        processor.sym_redis    = AsyncMock()
        processor.algo_redis   = AsyncMock()
        processor.db_mongo_client = MagicMock()

        # Mock the stream logger
        mock_logger = AsyncMock()
        mock_logger.added_packets = {}
        processor.logger = mock_logger

        # Mock MongoDB collections
        processor.orders_db    = AsyncMock()
        processor.response_db  = AsyncMock()
        processor.monitor_db   = AsyncMock()
        processor.firewalldb   = AsyncMock()

        processor.is_running = True
        return processor

    # ── process_error: order_data ─────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_order_data_error_closes_gate_and_acks(self, log_signal):
        proc = self._make_processor()
        order = {
            "clientID": "C1", "algoName": "TestAlgo",
            "exchangeInstrumentID": "66647", "exchangeSegment": "NSEFO",
            "orderQuantity": "30", "orderSide": "BUY", "orderType": "LIMIT",
            "productType": "NRML",
        }
        error_data = {
            "error_type": "order_data",
            "reason":     "test order error",
            "signal":     json.dumps(order),
        }
        log_signal(error_data)
        proc.sym_redis.get = AsyncMock(return_value="TESTFUT")
        proc.response_db.insert_one  = AsyncMock()
        proc.monitor_db.insert_one   = AsyncMock()
        proc.algo_redis.set          = AsyncMock()
        proc.db_mongo_client["Info"]["client_rms_params"].update_one = AsyncMock()

        await proc.process_error("msg-1", error_data)

        proc.algo_redis.set.assert_called_once()
        proc.stream_redis.xack.assert_called_once_with(
            "Error_Stream", "group1", "msg-1"
        )

    # ── process_error: pending_order ──────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_pending_order_error_acks_only(self, log_signal):
        proc = self._make_processor()
        pending = {
            "clientID": "C1", "algoName": "TestAlgo",
            "exchangeInstrumentID": "66647", "exchangeSegment": "NSEFO",
        }
        error_data = {
            "error_type": "pending_order",
            "reason":     "order timed out",
            "signal":     json.dumps(pending),
        }
        log_signal(error_data)

        await proc.process_error("msg-2", error_data)

        proc.stream_redis.xack.assert_called_once_with(
            "Error_Stream", "group1", "msg-2"
        )
        # Gate must NOT be closed for pending_order
        proc.algo_redis.set.assert_not_called()

    # ── process_error: AlgoSignalValidationError — unknown algo ───────────────

    @pytest.mark.asyncio
    async def test_algo_validation_error_unknown_algo_sends_alarm(self, log_signal):
        proc = self._make_processor()
        order = {
            "exchangeInstrumentID": "66647", "exchangeSegment": "NSEFO",
            "orderQuantity": "30", "orderSide": "BUY", "orderType": "LIMIT",
            "productType": "NRML",
        }
        error_data = {
            "error_type": "AlgoSignalValidationError",
            "reason":     "algo_configs has no entry for 'UnknownAlgo'",
            "signal":     json.dumps(order),
        }
        log_signal(error_data)
        proc.sym_redis.get        = AsyncMock(return_value="X")
        proc.response_db.insert_one = AsyncMock()
        proc.monitor_db.insert_one  = AsyncMock()

        await proc.process_error("msg-3", error_data)

        # alarm must be sent
        proc.ping_redis.rpush.assert_called()
        proc.stream_redis.xack.assert_called_once()

    # ── process_error: AlgoSignalValidationError — with algoName ─────────────

    @pytest.mark.asyncio
    async def test_algo_validation_error_with_algo_name_closes_mapped_clients(self, log_signal):
        proc = self._make_processor()
        order = {
            "algoName": "TestAlgo",
            "exchangeInstrumentID": "66647", "exchangeSegment": "NSEFO",
            "orderQuantity": "30", "orderSide": "BUY", "orderType": "LIMIT",
            "productType": "NRML",
        }
        error_data = {
            "error_type": "AlgoSignalValidationError",
            "reason":     "Daily trade limit exceeded",
            "signal":     json.dumps(order),
        }
        log_signal(error_data)

        # Simulate client_action_map with 2 clients, 1 active
        proc.stream_redis.hgetall = AsyncMock(
            return_value={"C1": "1", "C2": "0"}
        )
        proc.algo_redis.set      = AsyncMock()
        proc.response_db.insert_one = AsyncMock()
        proc.monitor_db.insert_one  = AsyncMock()
        proc.sym_redis.get          = AsyncMock(return_value="TESTFUT")
        proc.db_mongo_client["Info"]["client_rms_params"].update_one = AsyncMock()

        await proc.process_error("msg-4", error_data)

        # Only C1 (status='1') should have its gate closed
        proc.algo_redis.set.assert_called_once()
        call_args = proc.algo_redis.set.call_args[0]
        assert "TestAlgo_C1" == call_args[0]
        proc.stream_redis.xack.assert_called_once()

    # ── process_error: ClientOrderValidationError ─────────────────────────────

    @pytest.mark.asyncio
    async def test_client_order_validation_error_closes_gate(self, log_signal):
        proc = self._make_processor()
        order = {
            "clientID": "C1", "algoName": "TestAlgo",
            "exchangeInstrumentID": "66647", "exchangeSegment": "NSEFO",
            "orderQuantity": "30", "orderSide": "BUY", "orderType": "LIMIT",
            "productType": "NRML",
        }
        error_data = {
            "error_type": "ClientOrderValidationError",
            "reason":     "Computed qty 0 <= 0 after multiplier 0",
            "signal":     json.dumps(order),
        }
        log_signal(error_data)
        proc.sym_redis.get           = AsyncMock(return_value="TESTFUT")
        proc.response_db.insert_one  = AsyncMock()
        proc.monitor_db.insert_one   = AsyncMock()
        proc.algo_redis.set          = AsyncMock()
        proc.db_mongo_client["Info"]["client_rms_params"].update_one = AsyncMock()

        await proc.process_error("msg-5", error_data)

        proc.algo_redis.set.assert_called_once()
        proc.stream_redis.xack.assert_called_once()

    # ── close_exec_gate ───────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_close_exec_gate_sets_redis_and_mongo(self, log_signal):
        proc = self._make_processor()
        proc.algo_redis.set = AsyncMock()
        proc.db_mongo_client["Info"]["client_rms_params"].update_one = AsyncMock()

        order = {"clientID": "C1", "algoName": "TestAlgo"}
        log_signal(order)
        await proc.close_exec_gate(order)

        proc.algo_redis.set.assert_awaited_once_with(
            "TestAlgo_C1", json.dumps({"action": "Stop"})
        )
        proc.db_mongo_client["Info"]["client_rms_params"].update_one.assert_called_once()

    # ── log_error_order ───────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_log_error_order_inserts_to_response_and_monitor(self, log_signal):
        proc = self._make_processor()
        proc.sym_redis.get          = AsyncMock(return_value="TESTFUT")
        proc.response_db.insert_one = AsyncMock()
        proc.monitor_db.insert_one  = AsyncMock()

        order = {
            "clientID": "C1", "algoName": "TestAlgo",
            "exchangeInstrumentID": "66647", "exchangeSegment": "NSEFO",
            "orderQuantity": "30", "orderSide": "BUY", "orderType": "LIMIT",
            "productType": "NRML",
        }
        log_signal(order)
        await proc.log_error_order(order, "test reason")

        proc.response_db.insert_one.assert_called_once()
        proc.monitor_db.insert_one.assert_called_once()
        doc = proc.response_db.insert_one.call_args[0][0]
        assert doc["orderStatus"] == "Cancelled"
        assert doc["clientID"] == "C1"
        assert "M&M RMS: test reason" in doc["cancelrejectreason"]

    # ── send_alarm ────────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_send_alarm_pushes_to_ping_redis(self, log_signal):
        proc = self._make_processor()
        proc.ping_redis.rpush = AsyncMock()

        log_signal({"algoName": "TestAlgo", "message": "some error"})
        await proc.send_alarm({"algoName": "TestAlgo"}, "some error")

        calls = [c[0][0] for c in proc.ping_redis.rpush.call_args_list]
        assert "alarms-0" in calls
        assert "alarms-v2" in calls


# ═════════════════════════════════════════════════════════════════════════════
# HOW TO VALIDATE YOUR TEST RESULTS
# ═════════════════════════════════════════════════════════════════════════════
#
# 1. INSTALL DEPENDENCIES (if not already present)
#       pip install pytest pytest-asyncio
#
# 2. RUN ALL TESTS
#       cd /home/ubuntu/gokul_oms
#       pytest tests/test_firewall.py -v
#
# 3. WHAT "PASSED" MEANS FOR EACH GROUP
#
#    Stage 1 (TestSignalFieldCheck)
#      • test_valid_*          → signal_field_check raises NOTHING
#      • test_missing_*        → FieldValidationError.field == the missing field
#      • test_invalid_*        → FieldValidationError.field == the bad field
#
#    Stage 2 (TestAlgoCheck)
#      • test_valid_*          → algo_check() returns the algo config dict
#      • test_*_raises         → correct RuntimeError / ValueError / KeyError
#      • counter tests         → mocked Redis pipeline side-effects trigger limits
#
#    Stage 3 (TestSegmentCheck)
#      • test_valid_*          → segment_check() returns without exception
#      • test_*_raises         → RuntimeError about missing instrument or config
#
#    Stage 4 (TestClientCheckAndFanout)
#      • fanout tests          → returns correct count; throttler.xadd called N times
#      • error tests           → _push_error called once; error_type is correct
#      • payload tests         → JSON payload contains expected clientID, qty
#
#    MCXFO (TestMCXFOMultiplier)
#      • error tests           → _push_error called with message about multiplier
#      • valid test            → _push_error NOT called for multiplier step
#
#    _process_message (TestProcessMessage)
#      • routing tests         → error_type + stage fields match expected values
#      • success test          → _push_error never called; throttler.xadd called
#
#    ExecErrorProcessor (TestExecErrorProcessor)
#      • process_error tests   → correct mocked calls on algo_redis, stream_redis
#      • close_exec_gate       → algo_redis.set + mongo update_one called
#      • log_error_order       → response_db + monitor_db inserts correct doc
#      • send_alarm            → ping_redis.rpush to correct keys
#
# 4. COMMON FAILURES AND FIXES
#
#    ModuleNotFoundError       → ensure you run from /home/ubuntu/gokul_oms
#    AttributeError on mock    → check the patch path matches actual import path
#    RuntimeError in finally   → _proc_redis_streams.xack is not mocked in the test
#    pytest-asyncio not found  → pip install pytest-asyncio
#    asyncio fixture warning   → add asyncio_mode = "auto" to pytest.ini / pyproject
#
# 5. QUICK SMOKE TEST (live, touches real Redis)
#
#    To confirm the whole pipeline end-to-end, run:
#       cd /home/ubuntu/gokul_oms/tests
#       bash signal.sh          ← fires one good signal into Redis
#       python3 inject_test_signals.py  ← injects the full test suite
#
#    Then inspect:
#       redis-cli -n 14 XLEN Error_Stream   ← should be > 0 for failing cases
#       redis-cli -n 14 XLEN Algo_Signal    ← should drain as workers consume
# =============================================================================
