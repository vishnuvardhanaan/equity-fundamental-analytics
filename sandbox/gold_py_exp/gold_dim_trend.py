import sqlite3

import pandas as pd

from src.common.paths import GOLD_DB, GOLD_SQL_DIR

sql_file = GOLD_SQL_DIR / "gold_dim_trend.sql"

EXCEL = r"E:\Dev_Environment\equity-fundamental-analytics\sandbox\dim_metrics.xlsx"

MACRO_PHASE = "Recovery"  # <-- manual control point


with sqlite3.connect(GOLD_DB) as conn:

    # 1. Load dim_trend from Excel
    df_dim_metrics = pd.read_excel(EXCEL, sheet_name="dim_metrics")

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

    df_dim_metrics.to_sql("dim_metrics", conn, if_exists="append", index=False)

    df_dim_trend = pd.read_excel(EXCEL, sheet_name="dim_trend")

    conn.execute("DROP TABLE IF EXISTS dim_trend;")

    # 2. Create TEMP table explicitly
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

    df_dim_trend.to_sql("dim_trend", conn, if_exists="append", index=False)

    # 2. Ensure trend_score column exists
    cols = pd.read_sql("PRAGMA table_info(gold_scoring_engine);", conn)["name"].tolist()

    if "trend_score" not in cols:
        conn.execute(
            """
            ALTER TABLE gold_scoring_engine
            ADD COLUMN trend_score REAL;
        """
        )

    # 3. Run scoring SQL
    sql = open(sql_file).read()

    df_scores = pd.read_sql(sql, conn, params={"macro_phase": MACRO_PHASE})

    # 4. Update gold_scoring_engine
    conn.executemany(
        """
        UPDATE gold_scoring_engine
        SET trend_score = ?
        WHERE symbol = ?
        """,
        df_scores[["trend_score", "symbol"]].values.tolist(),
    )

    conn.commit()

conn.close()
