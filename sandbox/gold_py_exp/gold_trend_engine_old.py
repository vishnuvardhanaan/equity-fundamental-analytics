import sqlite3

import pandas as pd

from src.common.paths import GOLD_SQL_DIR

EXCEL_PATH = r"E:\Dev_Environment\equity-fundamental-analytics\sandbox\dim_metrics.xlsx"
SQL_FILE = GOLD_SQL_DIR / "gold_dim_trend.sql"


def run_trend_engine(
    conn: sqlite3.Connection,
    macro_phase: str,
) -> pd.DataFrame:
    """
    Trend scoring engine

    Returns:
        DataFrame with columns:
        symbol, trend_score
    """

    # ---------- Load dim_metrics ----------
    df_dim_metrics = pd.read_excel(
        EXCEL_PATH,
        sheet_name="dim_metrics",
    )

    conn.execute("DROP TABLE IF EXISTS dim_metrics;")

    conn.execute(
        """
        CREATE TEMP TABLE dim_metrics (
            metric_id INTEGER,
            metric_name TEXT,
            metric_category TEXT,
            metric_statement TEXT,
            metric_weight REAL,
            higher_the_better INTEGER
        );
        """
    )

    df_dim_metrics.to_sql(
        "dim_metrics",
        conn,
        if_exists="append",
        index=False,
    )

    # ---------- Load dim_trend ----------
    df_dim_trend = pd.read_excel(
        EXCEL_PATH,
        sheet_name="dim_trend",
    )

    conn.execute("DROP TABLE IF EXISTS dim_trend;")

    conn.execute(
        """
        CREATE TEMP TABLE dim_trend (
            macro_id INTEGER,
            macro_phase TEXT,
            sector_id INTEGER,
            sector_name TEXT,
            metric_id INTEGER,
            metric_name TEXT,
            threshold REAL,
            higher_the_better INTEGER,
            score REAL,
            macro_weight REAL,
            sector_weight REAL
        );
        """
    )

    df_dim_trend.to_sql(
        "dim_trend",
        conn,
        if_exists="append",
        index=False,
    )

    # ---------- Execute scoring SQL ----------
    sql = SQL_FILE.read_text()

    trend_scores = pd.read_sql(
        sql,
        conn,
        params={"macro_phase": macro_phase},
    )

    # Expected columns: symbol, trend_score
    return trend_scores
