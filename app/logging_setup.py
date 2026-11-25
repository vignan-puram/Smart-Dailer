"""Structured (JSON-lines) logging.

Every log record emitted through ``log_event`` is a single JSON object with an
``event`` name and arbitrary key/value fields, so safety decisions can be
grepped, shipped to a log pipeline, or replayed.
"""
from __future__ import annotations

import json
import logging
import sys
from typing import Any

_LOGGER_NAME = "smartdialer"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {"level": record.levelname, "logger": record.name}
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        else:
            payload["message"] = record.getMessage()
        return json.dumps(payload, default=str, sort_keys=False)


def configure_logging(level: str = "INFO", stream=sys.stderr) -> None:
    logger = logging.getLogger(_LOGGER_NAME)
    logger.handlers.clear()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False


def get_logger(component: str) -> logging.Logger:
    return logging.getLogger(f"{_LOGGER_NAME}.{component}")


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields: Any) -> None:
    if logger.isEnabledFor(level):
        logger.log(level, event, extra={"fields": {"event": event, **fields}})
