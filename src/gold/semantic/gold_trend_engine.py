import logging
from datetime import date

import numpy as np
import pandas as pd

from src.common.paths import GOLD_EXCEL


def run_trend_engine(conn, macro_phase: str):
    logging.info("Calculating Trend Scores")
    hist = pd.read_sql(
        """
        SELECT symbol, sector, metric_name, fiscal_year, metric_value
        FROM gold_equity_derivedmetrics_allyears
        """,
        conn,
    )

    hist = hist.sort_values(["symbol", "metric_name", "fiscal_year"])
    hist["previous_value"] = hist.groupby(["symbol", "metric_name"])[
        "metric_value"
    ].shift(1)

    hist["trend_pct"] = np.where(
        (hist["previous_value"].isna()) | (hist["previous_value"] == 0),
        np.nan,
        (hist["metric_value"] - hist["previous_value"]) / hist["previous_value"].abs(),
    )

    dim_metrics = pd.read_excel(
        GOLD_EXCEL,
        sheet_name="dim_metrics",
        usecols=["metric_name", "higher_the_better"],
    )

    hist = hist.merge(dim_metrics, on="metric_name", how="left")

    hist["trend_signal"] = np.select(
        [
            hist["trend_pct"].isna(),
            (hist["higher_the_better"] == 1) & (hist["trend_pct"] >= 0.02),
            (hist["higher_the_better"] == 1) & (hist["trend_pct"] <= -0.02),
            (hist["higher_the_better"] == 0) & (hist["trend_pct"] <= -0.02),
            (hist["higher_the_better"] == 0) & (hist["trend_pct"] >= 0.02),
        ],
        ["stable", "improving", "deteriorating", "improving", "deteriorating"],
        default="stable",
    )

    hist["rn"] = hist.groupby(["symbol", "metric_name"])["fiscal_year"].rank(
        ascending=False, method="first"
    )

    final = hist[hist["rn"] == 1]

    dim_trend = pd.read_excel(
        GOLD_EXCEL,
        sheet_name="dim_trend",
    )

    dim_trend = dim_trend[dim_trend["macro_phase"] == macro_phase]

    df = final.merge(
        dim_trend,
        left_on=["metric_name", "sector"],
        right_on=["metric_name", "sector_name"],
        how="left",
    )

    df["raw_score"] = np.where(df["trend_signal"] == "improving", df["score"], 0)
    df["rule_weight"] = df["macro_weight"] * df["sector_weight"]
    df["weighted_score"] = df["raw_score"] * df["rule_weight"]

    explain_df = df.assign(
        rule_type="trend",
        operator="trend",
        comparator_value="improving",
        rule_result=np.where(df["raw_score"] > 0, "pass", "fail"),
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

    trend_scores = (
        df.groupby(["symbol", "sector"], as_index=False)["weighted_score"]
        .sum()
        .rename(columns={"weighted_score": "trend_score"})
    )

    logging.info("Completed Trend Rules Scoring")

    return trend_scores, explain_df
