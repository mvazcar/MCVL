"""
Step 03: Compute days worked per month from affiliation episodes.
Replicates 03_MonthlyVars.do.

For each person x year x firm_cc2 x alta x baja combination, computes
how many days were worked in each of the 12 months.
"""
import polars as pl
from config import TEMP_DIR


def _parse_date_components(df: pl.DataFrame) -> pl.DataFrame:
    """Parse entry_date and exit_date (YYYYMMDD ints) into date components."""
    return df.with_columns(
        # alta components
        (pl.col("entry_date") // 10000).cast(pl.Int32).alias("alta_y"),
        ((pl.col("entry_date") % 10000) // 100).cast(pl.Int32).alias("alta_m"),
        (pl.col("entry_date") % 100).cast(pl.Int32).alias("alta_d"),
        # baja components
        (pl.col("exit_date") // 10000).cast(pl.Int32).alias("baja_y"),
        ((pl.col("exit_date") % 10000) // 100).cast(pl.Int32).alias("baja_m"),
        (pl.col("exit_date") % 100).cast(pl.Int32).alias("baja_d"),
    )


def _days_in_month(year_col: str, month_val: int) -> pl.Expr:
    """
    Number of days in a given calendar month.
    Uses the Stata convention of 30-day months (capped at 30).
    """
    # Standard days per month
    base = pl.when(pl.lit(month_val).is_in([1, 3, 5, 7, 8, 10, 12])).then(31)
    base = base.when(pl.lit(month_val).is_in([4, 6, 9, 11])).then(30)
    # February: 28 or 29 (leap year check)
    base = base.when(pl.lit(month_val) == 2).then(
        pl.when(
            ((pl.col(year_col) % 4 == 0) & (pl.col(year_col) % 100 != 0))
            | (pl.col(year_col) % 400 == 0)
        ).then(29).otherwise(28)
    ).otherwise(30)
    return base


def compute_monthly_days(merged: pl.DataFrame) -> pl.DataFrame:
    """
    Compute days1..days12 for each row (person x year x firm_cc2 x alta x baja).

    Logic (from 03_MonthlyVars.do):
    - Full month (30 days): month is fully spanned by the episode
    - Entry month: days from alta until end of month
    - Exit month: days from 1st until baja
    - Same-month episodes: baja_day - alta_day + 1
    - Capped at 30 days per month
    - February adjustments for full-month contracts
    """
    df = _parse_date_components(merged)

    year_col = "year"
    days_cols = {}

    for m in range(1, 13):
        # Condition: month fully worked
        full_month = (
            # Year is strictly between alta and baja years
            ((pl.col(year_col) > pl.col("alta_y")) & (pl.col(year_col) < pl.col("baja_y")))
            # Same year as alta and baja, month strictly between
            | ((pl.col(year_col) == pl.col("alta_y")) & (pl.col(year_col) == pl.col("baja_y"))
               & (pl.lit(m) > pl.col("alta_m")) & (pl.lit(m) < pl.col("baja_m")))
            # Same year as alta, not baja year, month after alta_month
            | ((pl.col(year_col) == pl.col("alta_y")) & (pl.col(year_col) != pl.col("baja_y"))
               & (pl.lit(m) > pl.col("alta_m")))
            # Same year as baja, not alta year, month before baja_month
            | ((pl.col(year_col) == pl.col("baja_y")) & (pl.col(year_col) != pl.col("alta_y"))
               & (pl.lit(m) < pl.col("baja_m")))
        )

        # Month of entry (not full)
        is_entry_month = (
            (pl.col(year_col) == pl.col("alta_y")) & (pl.col("alta_m") == m)
        )

        # Month of exit (not full)
        is_exit_month = (
            (pl.col(year_col) == pl.col("baja_y")) & (pl.col("baja_m") == m)
        )

        # Same month entry and exit
        same_month = is_entry_month & is_exit_month

        # Days in the calendar month (for computing "rest of month after entry")
        if m < 12:
            # Days remaining after alta in the month: (last_day_of_month - alta_day + 1)
            # last_day_of_month is computed but we cap at 30 for Stata compat
            entry_days = (30 - pl.col("alta_d") + 1).clip(1, 30)
        else:
            entry_days = (31 - pl.col("alta_d") + 1).clip(1, 30)

        exit_days = pl.col("baja_d").clip(1, 30)
        same_days = (pl.col("baja_d") - pl.col("alta_d") + 1).clip(1, 30)

        days_expr = (
            pl.when(same_month).then(same_days)
            .when(is_entry_month & ~is_exit_month).then(entry_days)
            .when(is_exit_month & ~is_entry_month).then(exit_days)
            .when(full_month).then(30)
            .otherwise(None)
        )

        # Cap at 30
        days_expr = days_expr.clip(upper_bound=30)

        # alta == baja and both in this month -> 1 day
        days_expr = (
            pl.when(
                (pl.col("entry_date") == pl.col("exit_date"))
                & (pl.col("alta_m") == m)
                & (pl.col(year_col) == pl.col("alta_y"))
            ).then(1)
            .otherwise(days_expr)
        )

        days_cols[f"days{m}"] = days_expr

    df = df.with_columns(**days_cols)

    # February adjustments (from Stata):
    # Full Feb if: alta=Feb 1 and (exit is a different month or different year)
    feb1 = (pl.col("alta_d") == 1) & (pl.col("alta_m") == 2) & (pl.col(year_col) == pl.col("alta_y"))
    exit_not_feb = (pl.col(year_col) != pl.col("baja_y")) | (pl.col("baja_m") != 2)
    df = df.with_columns(
        pl.when(feb1 & exit_not_feb).then(30).otherwise(pl.col("days2")).alias("days2")
    )

    # baja = 28Feb or 29Feb and alta not in Feb -> full month
    feb28 = (pl.col("baja_d") == 28) & (pl.col("baja_m") == 2) & (pl.col(year_col) == pl.col("baja_y"))
    feb29 = (pl.col("baja_d") == 29) & (pl.col("baja_m") == 2) & (pl.col(year_col) == pl.col("baja_y"))
    alta_not_feb = (pl.col(year_col) != pl.col("alta_y")) | (pl.col("alta_m") != 2)
    df = df.with_columns(
        pl.when((feb28 | feb29) & alta_not_feb).then(30).otherwise(pl.col("days2")).alias("days2")
    )

    # alta=Feb 1, baja=Feb 28 or 29 (same year) -> full month
    feb1_same = feb1 & (pl.col(year_col) == pl.col("baja_y")) & (pl.col("baja_m") == 2)
    df = df.with_columns(
        pl.when(feb1_same & (pl.col("baja_d").is_in([28, 29]))).then(30)
        .otherwise(pl.col("days2")).alias("days2")
    )

    # Drop intermediate columns
    df = df.drop("alta_y", "alta_m", "alta_d", "baja_y", "baja_m", "baja_d")

    return df


def save_step03(df: pl.DataFrame):
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    p = TEMP_DIR / "monthly_days.parquet"
    df.write_parquet(p)
    print(f"  Saved {p}")
