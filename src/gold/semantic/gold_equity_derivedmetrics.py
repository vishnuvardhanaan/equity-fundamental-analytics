import logging
import sqlite3

import pandas as pd

from src.common.paths import GOLD_DB, GOLD_SQL_DIR


def derived_metrics():
    logging.info("Processing Derived Metrics")

    with sqlite3.connect(GOLD_DB) as conn:

        sql_file = GOLD_SQL_DIR / "gold_equity_derivedmetrics.sql"

        query = sql_file.read_text()

        df_edm = pd.read_sql(query, conn)

        if df_edm.empty:
            raise ValueError("Gold derivedmetrics query returned no rows")

        if df_edm["symbol"].duplicated().any():
            raise ValueError("Gold invariant violated: multiple rows per symbol")

        df_edm.to_sql(
            "gold_equity_derivedmetrics", conn, if_exists="replace", index=False
        )

    logging.info("Gold derivedmetrics rows: %d", len(df_edm))

    logging.info("Derived Metrics Processed and Finished")
