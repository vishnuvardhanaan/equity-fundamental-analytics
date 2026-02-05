import logging
import operator
import sqlite3
from datetime import date

import pandas as pd

from src.common.paths import GOLD_EXCEL

OPS = {
    ">": operator.gt,
    "<": operator.lt,
    "=": operator.eq,
    ">=": operator.ge,
    "<=": operator.le,
}


def eval_vectorized(expr, df):
    return eval(expr, {"__builtins__": {}}, df.to_dict("series"))


def run_relative_engine(conn: sqlite3.Connection, macro_phase: str):
    logging.info("Calculating Relative Scores")

    stocks = pd.read_sql("SELECT * FROM gold_equity_derivedmetrics", conn)
    rules = pd.read_excel(GOLD_EXCEL, sheet_name="dim_relative")
    rules = rules[rules["macro_phase"] == macro_phase]

    explain_rows = []

    stocks["relative_score"] = 0.0

    for _, rule in rules.iterrows():
        sector = rule["sector_name"]
        metric = rule["metric_name"]
        threshold_expr = rule["threshold"]

        weighted_score = rule["score"] * rule["macro_weight"] * rule["sector_weight"]
        rule_weight = rule["macro_weight"] * rule["sector_weight"]
        op_fn = OPS[rule["operator"]]

        mask = stocks["sector"] == sector
        if mask.sum() == 0:
            continue

        try:
            right_values = eval_vectorized(threshold_expr, stocks.loc[mask])
            passed = op_fn(stocks.loc[mask, metric], right_values)

            stocks.loc[mask, "relative_score"] += passed.astype(int) * weighted_score

            temp = stocks.loc[mask, ["symbol", "sector", metric]].copy()
            temp["metric_name"] = metric
            temp["metric_value"] = temp[metric]
            temp["operator"] = rule["operator"]
            temp["comparator_value"] = right_values.values
            temp["rule_result"] = passed.map({True: "pass", False: "fail"})
            temp["raw_score"] = passed.astype(int) * rule["score"]
            temp["rule_weight"] = rule_weight
            temp["weighted_score"] = temp["raw_score"] * rule_weight
            temp["rule_type"] = "relative"
            temp["macro_phase"] = macro_phase
            temp["run_date"] = date.today()

            explain_rows.append(
                temp[
                    [
                        "symbol",
                        "macro_phase",
                        "sector",
                        "rule_type",
                        "metric_name",
                        "metric_value",
                        "operator",
                        "comparator_value",
                        "rule_result",
                        "raw_score",
                        "rule_weight",
                        "weighted_score",
                        "run_date",
                    ]
                ]
            )

        except Exception:
            continue

    explain_df = pd.concat(explain_rows, ignore_index=True)

    relative_scores = stocks[["symbol", "sector", "relative_score"]]

    logging.info("Completed Relative Rules Scoring")

    return relative_scores, explain_df
