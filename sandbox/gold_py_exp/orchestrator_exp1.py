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
from src.gold.semantic.gold_relative_engine import run_relative_engine
from src.gold.semantic.gold_trend_engine import run_trend_engine


def run_gold_scoring_engine(macro_phase: str) -> None:
    """
    Orchestrates Absolute + Relative + Trend scoring engines
    and materializes final gold_scoring_engine table.
    """

    logging.info("Starting Gold Scoring Engine")
    logging.info("Macro phase: %s", macro_phase)

    with sqlite3.connect(GOLD_DB) as conn:

        # --------------------------------------------------
        # 1. Update Base and Derived Metrics
        # --------------------------------------------------
        logging.info("Updating Base Metrics")
        base_metrics()
        derived_metrics()
        derived_metrics_allyears()

        # --------------------------------------------------
        # 1. Run Absolute Engine
        # --------------------------------------------------
        logging.info("Running Absolute Scoring Engine")
        abs_df = run_absolute_engine(conn, macro_phase)
        # Expected: symbol, sector, absolute_score

        # --------------------------------------------------
        # 2. Run Relative Engine
        # --------------------------------------------------
        logging.info("Running Relative Scoring Engine")
        rel_df = run_relative_engine(conn, macro_phase)
        # Expected: symbol, sector, relative_score

        # --------------------------------------------------
        # 3. Run Trend Engine
        # --------------------------------------------------
        logging.info("Running Trend Scoring Engine")
        trend_df = run_trend_engine(conn, macro_phase)
        # Expected: symbol, trend_score

        # --------------------------------------------------
        # 4. Merge all scores
        # --------------------------------------------------
        logging.info("Merging scores")

        final_df = abs_df.merge(
            rel_df,
            on=["symbol", "sector"],
            how="left",
        ).merge(
            trend_df,
            on=["symbol", "sector"],
            how="left",
        )

        # Fill missing scores safely
        final_df["relative_score"] = final_df["relative_score"].fillna(0.0)
        final_df["trend_score"] = final_df["trend_score"].fillna(0.0)

        # Add macro phase for lineage
        final_df["macro_phase"] = macro_phase

        # Optional: total score (nice to have)
        final_df["total_score"] = (
            final_df["absolute_score"]
            + final_df["relative_score"]
            + final_df["trend_score"]
        )

        # --------------------------------------------------
        # 5. Persist Gold Table (single write)
        # --------------------------------------------------
        logging.info("Writing gold_scoring_engine")

        final_df.to_sql(
            "gold_scoring_engine",
            conn,
            if_exists="replace",
            index=False,
        )

        conn.commit()

    conn.close()

    logging.info("Gold Scoring Engine completed successfully")


if __name__ == "__main__":
    # Single control point
    MACRO_PHASE = "Peak"  # Recovery | Expansion | Peak | Recession
    run_gold_scoring_engine(MACRO_PHASE)
