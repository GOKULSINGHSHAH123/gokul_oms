"""
conftest.py
-----------
Pytest configuration for the firewall test suite.

Hooks:
  - pytest_runtest_logreport  : logs PASS / FAIL / ERROR for every test
  - pytest_sessionstart       : logs session header
  - pytest_sessionfinish      : logs summary totals

Log file: tests/logs/test_firewall_validation.log
Format  : same as setup_logging.py  →  timestamp | level | name | message
"""

import json
import os
import sys
import logging
import time
from logging.handlers import RotatingFileHandler

import pytest

# ── ensure project root is importable ────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# ── log file location ─────────────────────────────────────────────────────────
_LOG_DIR  = os.path.join(os.path.dirname(__file__), "logs")
_LOG_FILE = os.path.join(_LOG_DIR, "test_firewall_validation.log")

_FORMAT      = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# ── build a dedicated logger (file + console) ─────────────────────────────────
os.makedirs(_LOG_DIR, exist_ok=True)

_logger = logging.getLogger("test_firewall")
_logger.setLevel(logging.DEBUG)
_logger.propagate = False

if not _logger.handlers:
    _fmt = logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT)

    _fh = RotatingFileHandler(
        _LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    _fh.setFormatter(_fmt)
    _logger.addHandler(_fh)

    _ch = logging.StreamHandler()
    _ch.setFormatter(_fmt)
    _logger.addHandler(_ch)


# ── helpers ───────────────────────────────────────────────────────────────────

# Maps test class prefix → stage label shown in logs
_STAGE_LABELS = {
    "TestSignalFieldCheck":      "Stage 1 | signal_field_check",
    "TestAlgoCheck":             "Stage 2 | algo_check",
    "TestSegmentCheck":          "Stage 3 | segment_check",
    "TestClientCheckAndFanout":  "Stage 4 | client_check_and_fanout",
    "TestMCXFOMultiplier":       "MCXFO   | multiplier_adjustment",
    "TestProcessMessage":        "Routing | _process_message",
    "TestExecErrorProcessor":    "ExecErr | exec_error.py",
}

# Expected outcome tag inferred from test name prefix
def _expected_tag(test_name: str) -> str:
    n = test_name.lower()
    # Signal / operation should be ACCEPTED by the firewall
    _pass_words = ("valid", "passes", "success", "unlimited", "no_xadd",
                   "no_error", "boundary", "applied", "contains", "writes_to",
                   "sets_redis", "inserts", "sends_alarm", "acks_only",
                   "closes_gate", "fanout", "returns_algo", "returns_zero")
    if any(w in n for w in _pass_words):
        return "EXPECT-PASS"

    # Signal / operation should be REJECTED by the firewall
    _fail_words = ("raises", "pushes_error", "invalid", "missing", "unknown",
                   "mismatch", "exceeded", "inactive", "negative", "zero_mult",
                   "not_found", "no_config", "no_segment")
    if any(w in n for w in _fail_words):
        return "EXPECT-FAIL"

    return "EXPECT-?"


def _stage(nodeid: str) -> str:
    for class_name, label in _STAGE_LABELS.items():
        if class_name in nodeid:
            return label
    return "unknown"


# ── session hooks ─────────────────────────────────────────────────────────────

def pytest_sessionstart(session):
    _logger.info("=" * 72)
    _logger.info("TEST SESSION START")
    _logger.info("=" * 72)


def pytest_sessionfinish(session, exitstatus):
    passed  = _counts.get("passed",  0)
    failed  = _counts.get("failed",  0)
    errored = _counts.get("errored", 0)
    total   = passed + failed + errored

    _logger.info("=" * 72)
    _logger.info(
        "TEST SESSION FINISH  |  total=%d  passed=%d  failed=%d  errored=%d",
        total, passed, failed, errored,
    )
    if failed == 0 and errored == 0:
        _logger.info("RESULT: ALL TESTS PASSED")
    else:
        _logger.warning("RESULT: %d TEST(S) FAILED / ERRORED", failed + errored)
    _logger.info("Log written to: %s", _LOG_FILE)
    _logger.info("=" * 72)


# ── per-test hook ─────────────────────────────────────────────────────────────

def pytest_runtest_logreport(report):
    """
    Called 3 times per test: setup / call / teardown.
    We only log on the 'call' phase (the actual test body).
    For setup/teardown errors we log those separately.
    """
    session = report  # just for clarity — report IS the object

    if report.when == "call":
        _log_call_result(report)

    elif report.when in ("setup", "teardown") and report.failed:
        _logger.error(
            "%-8s | %s | phase=%s | %s",
            "ERROR",
            report.nodeid,
            report.when,
            _short_longrepr(report),
        )


def _log_call_result(report):
    """Log a single test call result with stage, expected outcome, and duration."""
    nodeid    = report.nodeid                  # e.g. tests/test_firewall.py::TestAlgoCheck::test_valid...
    test_name = nodeid.split("::")[-1]         # last segment, e.g. test_valid_signal_returns_algo_cfg
    stage     = _stage(nodeid)
    expected  = _expected_tag(test_name)
    duration  = f"{report.duration * 1000:.1f}ms"

    # Detect validation correctness:
    #   EXPECT-PASS + PASSED  → correct
    #   EXPECT-FAIL + PASSED  → correct (exception raised as expected)
    #   any + FAILED          → problem

    if report.passed:
        _logger.info(
            "PASS | %-42s | %-12s | %-12s | %s",
            stage, expected, "ACTUAL-PASS", duration,
        )
        _logger.debug("     nodeid: %s", nodeid)
        _increment(report, "_fw_passed")

    elif report.failed:
        reason = _short_longrepr(report)
        _logger.error(
            "FAIL | %-42s | %-12s | %-12s | %s",
            stage, expected, "ACTUAL-FAIL", duration,
        )
        _logger.error("     nodeid  : %s", nodeid)
        _logger.error("     reason  : %s", reason)
        _increment(report, "_fw_failed")

    elif report.skipped:
        _logger.warning(
            "SKIP | %-42s | %-12s | %-12s | %s",
            stage, expected, "SKIPPED", duration,
        )
        _logger.warning("     nodeid: %s", nodeid)


def _short_longrepr(report) -> str:
    """Return a compact single-line failure reason from the report."""
    if not report.longrepr:
        return "no details"
    text = str(report.longrepr)
    # Take the last meaningful line (usually the AssertionError or ExceptionInfo)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return lines[-1] if lines else text[:200]


_counts = {"passed": 0, "failed": 0, "errored": 0}

def _increment(report, attr: str):
    key = attr.replace("_fw_", "")
    _counts[key] = _counts.get(key, 0) + 1


# ── signal recording store ────────────────────────────────────────────────────
# tests call log_signal(data) to store their input; _log_test_context reads it.

_signal_records: dict = {}   # nodeid → recorded data dict


@pytest.fixture
def log_signal(request):
    """
    Fixture that tests call to record their input signal for logging.

    Usage (inside any test method):
        def test_something(self, log_signal):
            sig = valid_signal(...)
            log_signal(sig)            # or log_signal({"signal": sig, "instrument": instr})
            ...
    """
    def _record(data):
        _signal_records[request.node.nodeid] = (
            data if isinstance(data, dict) else {"value": str(data)}
        )
    return _record


def _compact_signal(data: dict) -> str:
    """Serialize a signal dict compactly — truncate long values for readability."""
    short = {}
    for k, v in data.items():
        s = str(v)
        short[k] = s if len(s) <= 60 else s[:57] + "..."
    return json.dumps(short)


# ── per-test validation fixture ───────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _log_test_context(request):
    """
    Runs before and after every test.
    Logs the test start + validation intent + signal input, then the outcome.
    """
    test_name = request.node.name
    nodeid    = request.node.nodeid
    stage     = _stage(nodeid)
    expected  = _expected_tag(test_name)

    # Log parametrize params (visible in node name already, but explicit here)
    params_str = ""
    if hasattr(request.node, "callspec"):
        params_str = "  params=" + json.dumps(
            {k: str(v) for k, v in request.node.callspec.params.items()}
        )

    _logger.debug(
        "START | %-42s | %-12s | test=%s%s",
        stage, expected, test_name, params_str,
    )

    start = time.monotonic()
    yield
    elapsed = (time.monotonic() - start) * 1000

    # Log the recorded signal (if the test called log_signal)
    recorded = _signal_records.pop(nodeid, None)
    if recorded is not None:
        _logger.debug("     signal  : %s", _compact_signal(recorded))

    _logger.debug(
        "END   | %-42s | elapsed=%.1fms",
        stage, elapsed,
    )
