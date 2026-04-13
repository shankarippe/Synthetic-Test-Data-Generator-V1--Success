"""
logging_setup.py
----------------
Enterprise logging configuration.
Sets up three log targets: app.log, error.log, audit.log
"""

import logging
import logging.handlers
import os
from pathlib import Path


def setup_logging(config: dict) -> dict[str, logging.Logger]:
    """
    Initialise three named loggers from config.

    Returns a dict with keys: 'app', 'error', 'audit'
    """
    log_cfg = config.get("logging", {})
    level_name = log_cfg.get("level", "INFO")
    level = getattr(logging, level_name.upper(), logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    def _make_logger(name: str, filepath: str, log_level=logging.DEBUG) -> logging.Logger:
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger(name)
        logger.setLevel(log_level)

        # Rotating file handler (10 MB, keep 5 backups)
        fh = logging.handlers.RotatingFileHandler(
            filepath, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        fh.setFormatter(fmt)
        fh.setLevel(log_level)
        logger.addHandler(fh)

        # Console handler for app logger only
        if name == "app":
            ch = logging.StreamHandler()
            ch.setFormatter(fmt)
            ch.setLevel(level)
            logger.addHandler(ch)

        logger.propagate = False
        return logger

    app_logger = _make_logger("app", log_cfg.get("app_log", "logs/app.log"), level)
    error_logger = _make_logger("error", log_cfg.get("error_log", "logs/error.log"), logging.ERROR)
    audit_logger = _make_logger("audit", log_cfg.get("audit_log", "logs/audit.log"), logging.DEBUG)

    return {"app": app_logger, "error": error_logger, "audit": audit_logger}