from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

BRONZE_DB = r"E:\\Dev_Environment\\equity-fundamental-engine\\data\\nse_equity_universe_bronze.db"
SILVER_DB = DATA_DIR / "silver" / "nse_equity_universe_silver.db"
GOLD_DB = DATA_DIR / "gold" / "nse_equity_universe_gold.db"
GOLD_EXCEL = DATA_DIR / "gold" / "meta_equity_model.xlsx"

SILVER_SQL_DIR = BASE_DIR / "src" / "silver" / "sql"
GOLD_SQL_DIR = BASE_DIR / "src" / "gold" / "sql"
