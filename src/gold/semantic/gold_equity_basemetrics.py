import logging
import sqlite3

import pandas as pd

from src.common.paths import GOLD_DB, GOLD_SQL_DIR, SILVER_DB


def base_metrics():
    logging.info("Processing Base Metrics")

    with sqlite3.connect(SILVER_DB) as conn1, sqlite3.connect(GOLD_DB) as conn2:

        sql_file = GOLD_SQL_DIR / "gold_equity_basemetrics.sql"

        query = sql_file.read_text()

        df_ebm = pd.read_sql(query, conn1)

        text_columns = [
            "company_name",
            "city",
            "sector",
            "industry",
            "long_business_summary",
        ]

        df_ebm[text_columns] = df_ebm[text_columns].fillna("Unknown")

        if df_ebm.empty:
            raise ValueError("Gold basemetrics query returned no rows")

        if df_ebm["symbol"].duplicated().any():
            raise ValueError("Gold invariant violated: multiple rows per symbol")

        df_ebm.to_sql(
            "gold_equity_basemetrics", conn2, if_exists="replace", index=False
        )

    logging.info("Gold basemetrics rows: %d", len(df_ebm))

    logging.info("Base Metrics Processed and Finished")
