"""Logger module for unity_orm."""

import logging


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    logger = logging.getLogger(name)
    return logger
