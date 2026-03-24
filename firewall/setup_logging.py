"""
setup_logging.py
----------------
Logger factory — stdout-only for containerised deployment.
All pipeline modules call get_logger(__name__) to get a process-scoped
logger with a consistent format.
"""

import logging
import os
import sys

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_configured: set = set()


def get_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    Return a named logger with a console (stdout) handler.

    Args:
        name:     Logger name — use __name__ in every module.
        log_file: Ignored in container mode. Kept for backward compatibility.
    """
    if name in _configured:
        return logging.getLogger(name)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    logger.propagate = False

    formatter = logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    _configured.add(name)
    return logger