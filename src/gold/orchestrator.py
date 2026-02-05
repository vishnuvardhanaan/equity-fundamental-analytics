import logging
import sqlite3

import pandas as pd

from src.common.paths import GOLD_DB
from src.gold.semantic.gold_absolute_engine import run_absolute_engine
from src.gold.semantic.gold_equity_basemetrics import base_metrics
from src.gold.semantic.gold_equity_derivedmetrics import derived_metrics
from src.gold.semantic.gold_equity_derivedmetrics_allyears import (
    derived_metrics_allyears,
)
from src.gold.semantic.gold_equity_views import company_views
from src.gold.semantic.gold_relative_engine import run_relative_engine
from src.gold.semantic.gold_trend_engine import run_trend_engine


def run_gold_scoring_engine(macro_phase: str, progress_callback=None):
    logging.info("Starting Gold Scoring Engine")

    def update_progress(step, message):
        if progress_callback:
            progress_callback(step, message)

    with sqlite3.connect(GOLD_DB) as conn:

        update_progress(10, "Computing base metrics")
        base_metrics()

        update_progress(20, "Computing derived metrics (latest year)")
        derived_metrics()

        update_progress(30, "Computing derived metrics (all years)")
        derived_metrics_allyears()

        update_progress(45, "Running absolute scoring engine")
        abs_df, abs_explain = run_absolute_engine(conn, macro_phase)

        update_progress(60, "Running relative scoring engine")
        rel_df, rel_explain = run_relative_engine(conn, macro_phase)

        update_progress(75, "Running trend scoring engine")
        trend_df, trend_explain = run_trend_engine(conn, macro_phase)

        update_progress(85, "Merging scores & writing gold tables")
        final_df = (
            abs_df.merge(rel_df, on=["symbol", "sector"], how="left")
            .merge(trend_df, on=["symbol", "sector"], how="left")
            .fillna(0.0)
        )

        # final_df["macro_phase"] = macro_phase
        final_df["total_score"] = (
            final_df["absolute_score"]
            + final_df["relative_score"]
            + final_df["trend_score"]
        )

        final_df.to_sql(
            "gold_scoring_engine",
            conn,
            if_exists="replace",
            index=False,
        )

        explain_df = pd.concat(
            [abs_explain, rel_explain, trend_explain], ignore_index=True
        )

        explain_df.to_sql(
            "gold_rule_evaluations",
            conn,
            if_exists="replace",
            index=False,
        )

        conn.commit()

    update_progress(95, "Gold Scoring Engine completed")
    logging.info("Gold Scoring Engine + Explainability Completed")

    company_views()
    update_progress(100, "Creating company views")
    logging.info("Update of Company Views Completed")


if __name__ == "__main__":
    # Single control point
    MACRO_PHASE = "Recovery"  # Recovery | Expansion | Peak | Recession
    run_gold_scoring_engine(MACRO_PHASE)
