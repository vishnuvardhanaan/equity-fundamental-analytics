import sqlite3

import numpy as np
import pandas as pd

from src.common.paths import GOLD_DB

conn = sqlite3.connect(GOLD_DB)

stocks = pd.read_sql("SELECT * FROM gold_equity_derivedmetrics", conn)

rules = pd.read_excel(
    r"E:\Dev_Environment\equity-fundamental-analytics\sandbox\dim_metrics.xlsx",
    sheet_name="dim_relative",
)

MACRO = "Peak"
rules = rules[rules["macro_phase"] == MACRO]


stocks["relative_score"] = 0


def eval_vectorized(expr, df):
    return eval(expr, {"__builtins__": {}}, df.to_dict("series"))


import operator

OPS = {
    ">": operator.gt,
    "<": operator.lt,
    "=": operator.eq,
    ">=": operator.ge,
    "<=": operator.le,
}

for _, rule in rules.iterrows():

    sector = rule["sector_name"]
    metric = rule["metric_name"]
    threshold_expr = rule["threshold"]

    base_score = rule["score"]
    macro_weight = rule["macro_weight"]
    sector_weight = rule["sector_weight"]
    weighted_score = base_score * macro_weight * sector_weight
    op_fn = OPS[rule["operator"]]

    mask = stocks["sector"] == sector

    if mask.sum() == 0:
        continue

    try:
        right_values = eval_vectorized(threshold_expr, stocks.loc[mask])

        passed = op_fn(stocks.loc[mask, metric], right_values)

        stocks.loc[mask, "relative_score"] += passed.astype(int) * weighted_score

    except Exception as e:
        # Rule failure = ignored, but engine continues
        continue


output = stocks[["symbol", "sector", "relative_score"]].copy()

output["macro_phase"] = MACRO

output.to_sql("gold_scoring_engine", conn, if_exists="replace", index=False)

conn.close()
