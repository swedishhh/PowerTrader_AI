"""Logging setup for PowerTrader_AI subprocesses.

Each subprocess calls get_logger(name) once at module level. Output goes to
stdout so pt_controller can capture it into the in-process queue and log file
that back the UI Logs tab.

Timestamp format: 20260516:143201.042
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(
            "%(asctime)s.%(msecs)03d %(levelname)-8s [%(name)s] %(message)s",
            datefmt="%Y%m%d:%H%M%S",
        ))
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
    return logger
