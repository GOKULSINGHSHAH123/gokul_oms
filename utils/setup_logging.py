"""
setup_logging.py
----------------
Rotating file logger factory.
All pipeline modules call get_logger(__name__) to get a process-scoped,
rotating-file logger with a consistent format.
"""

import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR: str = os.getenv("LOG_DIR", "logs")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# 10 MB per file, keep 5 backups
MAX_BYTES: int = 10 * 1024 * 1024
BACKUP_COUNT: int = 5

_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Track which loggers have already been configured to avoid duplicate handlers
_configured: set = set()


def get_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    Return a named logger with both console and rotating-file handlers.

    Args:
        name:     Logger name — use __name__ in every module.
        log_file: Override log file path. Defaults to logs/<name>.log.
    """
    if name in _configured:
        return logging.getLogger(name)

    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    logger.propagate = False  # Don't double-log to root

    formatter = logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Rotating file handler
    if log_file is None:
        safe_name = name.replace(".", "_")
        log_file = os.path.join(LOG_DIR, f"{safe_name}.log")

    fh = RotatingFileHandler(
        log_file,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    _configured.add(name)
    return logger
