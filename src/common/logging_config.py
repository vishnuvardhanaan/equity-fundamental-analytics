import logging
from pathlib import Path

from .paths import LOG_DIR

LOG_DIR.mkdir(exist_ok=True)

SILVER_LOG_FILE = LOG_DIR / "silver_pipeline.log"
GOLD_LOG_FILE = LOG_DIR / "gold_pipeline.log"


def silver_setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(SILVER_LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def gold_setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(GOLD_LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
