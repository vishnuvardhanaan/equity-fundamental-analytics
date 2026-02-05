import sqlite3

import numpy as np
import pandas as pd

from src.common.paths import GOLD_DB

EXCEL_PATH = r"E:\Dev_Environment\equity-fundamental-analytics\sandbox\dim_metrics.xlsx"
DEFAULT_MACRO_PHASE = "Peak"

conn = sqlite3.connect(GOLD_DB)

stocks_df = pd.read_sql("SELECT * FROM gold_equity_derivedmetrics", conn)

rules_df = pd.read_excel(EXCEL_PATH, sheet_name="dim_absolute")

rules_df = rules_df[rules_df["macro_phase"] == DEFAULT_MACRO_PHASE].copy()

id_cols = ["symbol", "sector"]

metrics_df = stocks_df.melt(
    id_vars=id_cols, var_name="metric_name", value_name="metric_value"
)

# Drop null metrics early
metrics_df = metrics_df.dropna(subset=["metric_value"])

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

scoring_df["rule_score"] = np.where(
    scoring_df["rule_passed"],
    scoring_df["score"] * scoring_df["macro_weight"] * scoring_df["sector_weight"],
    0.0,
)

final_scores = (
    scoring_df.groupby("symbol", as_index=False)["rule_score"]
    .sum()
    .rename(columns={"rule_score": "absolute_score"})
)

final_scores["macro_phase"] = DEFAULT_MACRO_PHASE

final_scores.to_sql("gold_scoring_engine", conn, if_exists="replace", index=False)

conn.close()
