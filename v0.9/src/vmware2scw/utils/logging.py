"""Structured logging for vmware2scw."""

from __future__ import annotations

import logging

from rich.logging import RichHandler


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger instance."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = RichHandler(
            show_time=True,
            show_path=False,
            markup=True,
            rich_tracebacks=True,
        )
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger


def set_log_level(level: str) -> None:
    """Set global log level (DEBUG, INFO, WARNING, ERROR)."""
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger("vmware2scw").setLevel(numeric)
