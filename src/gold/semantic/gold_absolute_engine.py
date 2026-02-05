import logging
import sqlite3
from datetime import date

import numpy as np
import pandas as pd

from src.common.paths import GOLD_EXCEL


def run_absolute_engine(conn: sqlite3.Connection, macro_phase: str):
    logging.info("Calculating Absolute Scores")
    stocks_df = pd.read_sql("SELECT * FROM gold_equity_derivedmetrics", conn)

    rules_df = pd.read_excel(GOLD_EXCEL, sheet_name="dim_absolute")
    rules_df = rules_df[rules_df["macro_phase"] == macro_phase].copy()

    metrics_df = stocks_df.melt(
        id_vars=["symbol", "sector"],
        var_name="metric_name",
        value_name="metric_value",
    ).dropna(subset=["metric_value"])

    scoring_df = metrics_df.merge(
        rules_df,
        left_on=["sector", "metric_name"],
        right_on=["sector_name", "metric_name"],
        how="inner",
    )

    scoring_df["rule_passed"] = np.select(
        [
            scoring_df["operator"] == ">",
            scoring_df["operator"] == "<",
            scoring_df["operator"] == "=",
            scoring_df["operator"] == ">=",
            scoring_df["operator"] == "<=",
        ],
        [
            scoring_df["metric_value"] > scoring_df["threshold"],
            scoring_df["metric_value"] < scoring_df["threshold"],
            scoring_df["metric_value"] == scoring_df["threshold"],
            scoring_df["metric_value"] >= scoring_df["threshold"],
            scoring_df["metric_value"] <= scoring_df["threshold"],
        ],
        default=False,
    )

    scoring_df["raw_score"] = np.where(
        scoring_df["rule_passed"], scoring_df["score"], 0
    )
    scoring_df["rule_weight"] = scoring_df["macro_weight"] * scoring_df["sector_weight"]
    scoring_df["weighted_score"] = scoring_df["raw_score"] * scoring_df["rule_weight"]

    explain_df = scoring_df.assign(
        rule_type="absolute",
        comparator_value=scoring_df["threshold"],
        rule_result=np.where(scoring_df["rule_passed"], "pass", "fail"),
        macro_phase=macro_phase,
        run_date=date.today(),
    )[
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

    absolute_scores = (
        scoring_df.groupby(["symbol", "sector"], as_index=False)["weighted_score"]
        .sum()
        .rename(columns={"weighted_score": "absolute_score"})
        .assign(macro_phase=macro_phase)
        .reindex(columns=["symbol", "macro_phase", "sector", "absolute_score"])
    )

    logging.info("Completed Absolute Rules Scoring")

    return absolute_scores, explain_df
