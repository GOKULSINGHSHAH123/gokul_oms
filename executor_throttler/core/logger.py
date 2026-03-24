"""
Logging configuration and setup module.

Provides centralized logging configuration for containerised deployment.
All output goes to stdout — Docker handles rotation and persistence.
"""

import logging
import sys


def setup_logging(
    logger_name: str,
    log_dir: str = 'logs',
    log_level: int = logging.DEBUG,
    file_name: str = 'main',
) -> logging.Logger:
    """
    Set up a logger with stdout handler.

    Args:
        logger_name: Unique identifier for the logger (used in log messages)
        log_dir: Ignored — kept for backward compatibility
        log_level: Logging verbosity level (default: logging.DEBUG)
        file_name: Ignored — kept for backward compatibility

    Returns:
        logging.Logger: Configured logger instance ready for use
    """
    log_format = f'%(asctime)s - {logger_name} - %(levelname)s - %(message)s'

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(log_format)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    ch.setLevel(log_level)
    logger.addHandler(ch)

    return logger