import logging

from src.silver.cleaners import (
    clean_equity_dynamicinfo,
    clean_equity_staticinfo,
    clean_equity_universe,
    clean_stock_balancesheet,
    clean_stock_cashflowstmt,
    clean_stock_incomestmt,
)

SILVER_STEPS = [
    ("Equity Universe", clean_equity_universe.run),
    ("Equity Static Information", clean_equity_staticinfo.run),
    ("Equity Dynamic Information", clean_equity_dynamicinfo.run),
    ("Balance Sheet", clean_stock_balancesheet.run),
    ("Income Statement", clean_stock_incomestmt.run),
    ("Cashflow Statement", clean_stock_cashflowstmt.run),
]


def run_silver_pipeline(status_callback=None):
    logging.info("Starting NSE Equity Database Cleaning")

    for step_name, step_fn in SILVER_STEPS:
        try:
            if status_callback:
                status_callback(step_name, "RUNNING")

            logging.info(f"Running step: {step_name}")
            step_fn()

            logging.info(f"Completed step: {step_name}")

            if status_callback:
                status_callback(step_name, "SUCCESS")

        except Exception as e:
            logging.exception(f"Failed step: {step_name}")

            if status_callback:
                status_callback(step_name, "FAILED")

            break  # fail fast

    logging.info("Processed and Cleaned NSE Equity Database")
