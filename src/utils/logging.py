"""
Logging utilities for the audit‑trail pipeline.

This module exposes a helper function ``get_logger`` which configures
structured logging for a given run. Log messages are written both to
a file under the run directory and to the console. Log formatting
includes timestamps and log levels.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


def get_logger(run_id: str, run_dir: Path, log_level: str | int = "INFO") -> logging.Logger:
    """
    Create or retrieve a logger configured for a specific run.

    The logger writes messages to both a file (named ``<run_id>.log``)
    within ``run_dir`` and to the console. If the logger already
    exists, its handlers are replaced to avoid duplicate messages.

    Parameters
    ----------
    run_id : str
        Unique identifier for the current run.
    run_dir : Path
        Directory where log files should be stored. Must exist.
    log_level : str or int, optional
        Logging level (e.g. "INFO", "DEBUG"). Defaults to ``"INFO"``.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    logger = logging.getLogger(run_id)
    # Convert log level string to numeric level if necessary
    if isinstance(log_level, str):
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    else:
        numeric_level = log_level
    logger.setLevel(numeric_level)
    # Remove existing handlers to avoid duplication
    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
    # Ensure run directory exists
    run_dir.mkdir(parents=True, exist_ok=True)
    # File handler
    file_path = run_dir / f"{run_id}.log"
    file_handler = logging.FileHandler(file_path)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
