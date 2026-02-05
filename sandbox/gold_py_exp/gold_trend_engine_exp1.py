import numpy as np
import pandas as pd


def run_trend_engine(conn, macro_phase: str) -> pd.DataFrame:
    """
    Pandas port of gold_dim_trend.sql
    Returns: symbol, sector, trend_score
    """

    # --------------------------------------------------
    # 1. Load historical metric data
    # --------------------------------------------------
    hist = pd.read_sql(
        """
        SELECT
            symbol,
            sector,
            metric_name,
            fiscal_year,
            metric_value
        FROM gold_equity_derivedmetrics_allyears
        """,
        conn,
    )

    # Ensure proper ordering
    hist = hist.sort_values(["symbol", "metric_name", "fiscal_year"])

    # --------------------------------------------------
    # 2. Base + LAG(previous_value)
    # --------------------------------------------------
    hist["previous_value"] = hist.groupby(["symbol", "metric_name"])[
        "metric_value"
    ].shift(1)

    # --------------------------------------------------
    # 3. delta_calc (trend_pct)
    # --------------------------------------------------
    hist["trend_pct"] = np.where(
        (hist["previous_value"].isna()) | (hist["previous_value"] == 0),
        np.nan,
        (hist["metric_value"] - hist["previous_value"]) / hist["previous_value"].abs(),
    )

    # --------------------------------------------------
    # 4. Load dim_metrics (from Excel, NOT DB)
    # --------------------------------------------------
    dim_metrics = pd.read_excel(
        r"E:\Dev_Environment\equity-fundamental-analytics\sandbox\dim_metrics.xlsx",
        sheet_name="dim_metrics",
        usecols=["metric_name", "higher_the_better"],
    )

    hist = hist.merge(
        dim_metrics,
        on="metric_name",
        how="left",
    )

    # --------------------------------------------------
    # 5. Trend signal logic (exact SQL semantics)
    # --------------------------------------------------
    conditions = [
        hist["trend_pct"].isna(),
        (hist["higher_the_better"] == 1) & (hist["trend_pct"] >= 0.02),
        (hist["higher_the_better"] == 1) & (hist["trend_pct"] <= -0.02),
        (hist["higher_the_better"] == 0) & (hist["trend_pct"] <= -0.02),
        (hist["higher_the_better"] == 0) & (hist["trend_pct"] >= 0.02),
    ]

    choices = [
        "stable",
        "improving",
        "deteriorating",
        "improving",
        "deteriorating",
    ]

    hist["trend_signal"] = np.select(
        conditions,
        choices,
        default="stable",
    )

    # --------------------------------------------------
    # 6. ROW_NUMBER() logic → latest fiscal year
    # --------------------------------------------------
    hist["rn"] = hist.groupby(["symbol", "metric_name"])["fiscal_year"].rank(
        method="first", ascending=False
    )

    final_trend_fact = hist[hist["rn"] == 1].copy()

    # --------------------------------------------------
    # 7. Load dim_trend rules (Excel only)
    # --------------------------------------------------
    dim_trend = pd.read_excel(
        r"E:\Dev_Environment\equity-fundamental-analytics\sandbox\dim_metrics.xlsx",
        sheet_name="dim_trend",
        usecols=[
            "metric_name",
            "sector_name",
            "macro_phase",
            "score",
            "macro_weight",
            "sector_weight",
        ],
    )

    dim_trend = dim_trend[dim_trend["macro_phase"] == macro_phase]

    # --------------------------------------------------
    # 8. metric_scores join
    # --------------------------------------------------
    df = final_trend_fact.merge(
        dim_trend,
        left_on=["metric_name", "sector"],
        right_on=["metric_name", "sector_name"],
        how="left",
    )

    # --------------------------------------------------
    # 9. Metric trend score
    # --------------------------------------------------
    df["metric_trend_score"] = np.where(
        df["trend_signal"] == "improving",
        df["score"] * df["macro_weight"] * df["sector_weight"],
        0.0,
    )

    # --------------------------------------------------
    # 10. Final aggregation
    # --------------------------------------------------
    trend_scores = (
        df.groupby(["symbol", "sector"], as_index=False)["metric_trend_score"]
        .sum()
        .rename(columns={"metric_trend_score": "trend_score"})
    )

    return trend_scores
