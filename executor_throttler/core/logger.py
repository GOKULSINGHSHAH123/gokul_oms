"""
Logging configuration and setup module.

Provides centralized logging configuration with rotating file handlers, supporting
multi-process environments. Logs are organized by component with daily rotation.
"""

import logging
import logging.handlers
from pathlib import Path
from datetime import datetime


def setup_logging(
    logger_name: str,
    log_dir: str = 'logs',
    log_level: int = logging.DEBUG,
    file_name: str = 'main',
) -> logging.Logger:
    """
    Set up a logger with rotating file handler and multi-process support.

    Creates a logger instance with a RotatingFileHandler that rotates when files
    exceed 10MB. Each logger is isolated by name to support multi-process execution.

    Args:
        logger_name: Unique identifier for the logger (used in log messages)
        log_dir: Directory path for log files relative to script location (default: 'logs')
        log_level: Logging verbosity level (default: logging.DEBUG)
        file_name: Base name for log files without extension (default: 'main')

    Returns:
        logging.Logger: Configured logger instance ready for use
    """
    log_format = f'%(asctime)s - {logger_name} - %(levelname)s - %(message)s'
    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Clear existing handlers (to avoid duplicate handlers when called multiple times)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Set up log directory
    log_dir = Path(__file__).parent.parent / Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create formatter
    formatter = logging.Formatter(log_format)

    # Create rotating file handler (rotates at midnight)
    log_file = log_dir / \
        f"{file_name}_{datetime.now().strftime('%Y-%m-%d')}.log"
    # file_handler = logging.handlers.TimedRotatingFileHandler(
    #     log_file,
    #     when='midnight',
    #     backupCount=100,  # Keep logs for 100 days
    #     encoding='utf-8'
    # )
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=10,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)

    # Set suffix for rotated files to include date
    file_handler.suffix = "%Y-%m-%d"

    # Add handler to logger
    logger.addHandler(file_handler)

    return logger
