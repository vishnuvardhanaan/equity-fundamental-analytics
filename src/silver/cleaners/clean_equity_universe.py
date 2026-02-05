import logging
import sqlite3

import pandas as pd

from src.common.paths import BRONZE_DB, SILVER_DB, SILVER_SQL_DIR


def run():
    logging.info("Cleaning Equity Universe Started")

    conn1 = sqlite3.connect(BRONZE_DB)
    conn2 = sqlite3.connect(SILVER_DB)

    sql_file = SILVER_SQL_DIR / "clean_equity_universe.sql"
    query = sql_file.read_text()

    df_reu = pd.read_sql(query, conn1)

    df_reu["raw_ingested_ts"] = pd.to_datetime(df_reu["raw_ingested_ts"])
    df_reu["clean_processed_ts"] = pd.Timestamp.utcnow()

    for col in df_reu.select_dtypes(include="object").columns:
        df_reu[col] = df_reu[col].astype("string")

    df_reu.to_sql(
        "clean_equity_universe",
        conn2,
        if_exists="replace",
        index=False,
    )

    conn1.close()
    conn2.close()

    logging.info("Cleaning Equity Universe Completed")
