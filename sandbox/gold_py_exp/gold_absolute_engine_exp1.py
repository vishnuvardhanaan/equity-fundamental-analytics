import sqlite3

import numpy as np
import pandas as pd

from src.common.paths import GOLD_DB

EXCEL_PATH = r"E:\Dev_Environment\equity-fundamental-analytics\sandbox\dim_metrics.xlsx"


def run_absolute_engine(conn: sqlite3.Connection, macro_phase: str) -> pd.DataFrame:
    """
    Absolute scoring engine

    Returns:
        DataFrame with columns:
        symbol, sector, absolute_score
    """

    # Load data
    stocks_df = pd.read_sql("SELECT * FROM gold_equity_derivedmetrics", conn)

    rules_df = pd.read_excel(EXCEL_PATH, sheet_name="dim_absolute")

    rules_df = rules_df[rules_df["macro_phase"] == macro_phase].copy()

    # Melt metrics
    metrics_df = stocks_df.melt(
        id_vars=["symbol", "sector"],
        var_name="metric_name",
        value_name="metric_value",
    ).dropna(subset=["metric_value"])

    # Join rules
    scoring_df = metrics_df.merge(
        rules_df,
        left_on=["sector", "metric_name"],
        right_on=["sector_name", "metric_name"],
        how="inner",
    )

    # Rule evaluation (unchanged)
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

    scoring_df["absolute_score"] = np.where(
        scoring_df["rule_passed"],
        scoring_df["score"] * scoring_df["macro_weight"] * scoring_df["sector_weight"],
        0.0,
    )

    # Aggregate
    absolute_scores = scoring_df.groupby(["symbol", "sector"], as_index=False)[
        "absolute_score"
    ].sum()

    return absolute_scores
