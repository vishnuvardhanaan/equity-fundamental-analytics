import operator
import sqlite3

import pandas as pd

from src.common.paths import GOLD_DB

EXCEL_PATH = r"E:\Dev_Environment\equity-fundamental-analytics\sandbox\dim_metrics.xlsx"


def eval_vectorized(expr, df):
    """
    Vector-safe eval for relative thresholds
    """
    return eval(expr, {"__builtins__": {}}, df.to_dict("series"))


OPS = {
    ">": operator.gt,
    "<": operator.lt,
    "=": operator.eq,
    ">=": operator.ge,
    "<=": operator.le,
}


def run_relative_engine(
    conn: sqlite3.Connection,
    macro_phase: str,
) -> pd.DataFrame:
    """
    Relative scoring engine

    Returns:
        DataFrame with columns:
        symbol, sector, relative_score
    """

    # Load stock data
    stocks = pd.read_sql("SELECT * FROM gold_equity_derivedmetrics", conn)

    # Load rules
    rules = pd.read_excel(
        EXCEL_PATH,
        sheet_name="dim_relative",
    )

    rules = rules[rules["macro_phase"] == macro_phase]

    # Initialize score
    stocks["relative_score"] = 0.0

    # Core loop (UNCHANGED)
    for _, rule in rules.iterrows():

        sector = rule["sector_name"]
        metric = rule["metric_name"]
        threshold_expr = rule["threshold"]

        weighted_score = rule["score"] * rule["macro_weight"] * rule["sector_weight"]

        op_fn = OPS[rule["operator"]]

        mask = stocks["sector"] == sector

        if mask.sum() == 0:
            continue

        try:
            right_values = eval_vectorized(threshold_expr, stocks.loc[mask])

            passed = op_fn(stocks.loc[mask, metric], right_values)

            stocks.loc[mask, "relative_score"] += passed.astype(int) * weighted_score

        except Exception:
            # Rule failure is ignored by design
            continue

    # Final output
    relative_scores = stocks[["symbol", "sector", "relative_score"]].copy()

    return relative_scores
