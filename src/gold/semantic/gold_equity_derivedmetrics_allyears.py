import logging
import sqlite3

import pandas as pd

from src.common.paths import GOLD_DB, GOLD_SQL_DIR, SILVER_DB


def derived_metrics_allyears():
    logging.info("Processing Derived Metrics for All Years")

    with sqlite3.connect(SILVER_DB) as conn1:

        sql_file = GOLD_SQL_DIR / "gold_equity_derivedmetrics_allyears.sql"

        query = sql_file.read_text()

        df_edm = pd.read_sql(query, conn1)

        if df_edm.empty:
            raise ValueError("Gold derivedmetrics for all years query returned no rows")

        if df_edm[["symbol", "fiscal_year", "metric_name"]].duplicated().any():
            raise ValueError(
                "Gold invariant violated: multiple rows per symbol-fiscal_year-metric_name"
            )

        with sqlite3.connect(GOLD_DB) as conn2:
            df_edm.to_sql(
                "gold_equity_derivedmetrics_allyears",
                conn2,
                if_exists="replace",
                index=False,
            )

    logging.info("Gold derivedmetrics for all years rows: %d", len(df_edm))

    logging.info("Derived Metrics for All Years Processed and Finished")
