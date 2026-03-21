"""
Step 04: Build annual summaries from wide-format episode data.
Replaces the monthly reshape (which produced 354M+ rows) with direct
annual aggregation from the days1..12 / contribution_1..12 columns.

Computes per person-year:
  - Annual contributions (sum across months, per firm and total)
  - Main job identification (highest annual contribution firm)
  - Annual days worked (max across episodes per month, then sum)
  - Contract type indicators
"""
import gc
import polars as pl
from config import (
    YEAR_FIRST, YEAR_LATEST, TEMP_DIR,
    WORK_REGIME_RANGES, SELF_EMP_RANGES, DOMESTIC_CODES, UNEMP_JR,
    BASQUE_NAVARRA_MUNI,
)


def _in_ranges(col: pl.Expr, ranges: list) -> pl.Expr:
    cond = pl.lit(False)
    for item in ranges:
        if isinstance(item, tuple):
            lo, hi = item
            cond = cond | ((col >= lo) & (col <= hi))
        else:
            cond = cond | (col == item)
    return cond


def build_annual_from_wide(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert wide-format episode data (with days1..12, contribution_1..12)
    into annual person-year summaries.
    """
    print("=" * 60)
    print("STEP 04: Building annual summaries from wide format")
    print("=" * 60)

    # -- Annual contribution per episode (sum of monthly contributions) -----
    contrib_cols = [f"contribution_{m}" for m in range(1, 13) if f"contribution_{m}" in df.columns]
    if contrib_cols:
        df = df.with_columns(
            (pl.sum_horizontal(*[pl.col(c).fill_null(0) for c in contrib_cols])
             .cast(pl.Float64) / 100.0).alias("annual_contribution"),
        )
    else:
        df = df.with_columns(pl.lit(0.0).alias("annual_contribution"))

    # Annual autonomous contributions
    contrib_aut_cols = [f"contribution_aut_{m}" for m in range(1, 13)
                        if f"contribution_aut_{m}" in df.columns]
    if contrib_aut_cols:
        df = df.with_columns(
            (pl.sum_horizontal(*[pl.col(c).fill_null(0) for c in contrib_aut_cols])
             .cast(pl.Float64) / 100.0).alias("annual_contribution_aut"),
        )

    # -- Annual days per episode (sum of monthly days) ----------------------
    days_cols = [f"days{m}" for m in range(1, 13) if f"days{m}" in df.columns]
    if days_cols:
        df = df.with_columns(
            pl.sum_horizontal(*[pl.col(c).fill_null(0) for c in days_cols])
            .alias("episode_days"),
        )

    # -- Contract type indicators -------------------------------------------
    print("  Computing contract type indicators...")
    if "contribution_regime" in df.columns:
        regime = pl.col("contribution_regime")
        df = df.with_columns(
            (_in_ranges(regime, WORK_REGIME_RANGES)
             & ~pl.col("job_relationship").is_in(UNEMP_JR))
            .cast(pl.Int8).alias("rel_contract"),
            _in_ranges(regime, SELF_EMP_RANGES).cast(pl.Int8).alias("self_emp"),
            _in_ranges(regime, [(c, c) if isinstance(c, int) else c for c in DOMESTIC_CODES])
            .cast(pl.Int8).alias("emp_hogar"),
            pl.col("job_relationship").is_in(UNEMP_JR).cast(pl.Int8).alias("unemp_jr"),
        )

    # Basque/Navarra indicator
    if "firm_muni" in df.columns:
        df = df.with_columns(
            _in_ranges(pl.col("firm_muni"), BASQUE_NAVARRA_MUNI)
            .cast(pl.Int8).alias("basque_navarra")
        )

    # -- Annual days: max across episodes per month, then sum ---------------
    print("  Computing annual days worked...")
    if days_cols:
        # Select only needed columns to reduce memory
        days_select_cols = ["person_id", "year"] + days_cols
        if "rel_contract" in df.columns:
            days_select_cols.append("rel_contract")

        days_subset = df.select(days_select_cols)
        if "rel_contract" in days_subset.columns:
            days_subset = days_subset.filter(pl.col("rel_contract") == 1)

        max_exprs = [pl.col(f"days{m}").max().alias(f"days{m}") for m in range(1, 13)
                     if f"days{m}" in days_subset.columns]
        days_annual = days_subset.group_by("person_id", "year").agg(max_exprs)
        del days_subset
        gc.collect()

        days_annual = days_annual.with_columns(
            pl.sum_horizontal(*[pl.col(f"days{m}").fill_null(0) for m in range(1, 13)
                                if f"days{m}" in days_annual.columns])
            .alias("days")
        ).select("person_id", "year", "days")

        # Lags
        days_annual = days_annual.sort("person_id", "year").with_columns(
            pl.col("days").shift(1).over("person_id").alias("days_lag1"),
            pl.col("days").shift(2).over("person_id").alias("days_lag2"),
            pl.col("days").shift(3).over("person_id").alias("days_lag3"),
        )
    else:
        days_annual = pl.DataFrame(schema={"person_id": pl.Utf8, "year": pl.Int16, "days": pl.Int32})

    # -- Collapse to person-firm-year (for main job identification) ----------
    print("  Identifying main job per person-year...")
    # Group by person x firm x year, sum contributions
    agg_exprs = [
        pl.col("annual_contribution").sum().alias("firm_annual_contrib"),
        pl.col("episode_days").sum().alias("firm_days"),
    ]
    # Carry forward first values of episode-level variables
    for c in ["contribution_regime", "contribution_group", "contract_type",
              "ptcoef", "firm_muni", "firm_workers", "firm_age",
              "firm_jur_type", "firm_jur_status", "firm_id", "firm_cc",
              "firm_main_prov", "firm_ett", "sector_cnae09", "sector_cnae93",
              "rel_contract", "self_emp", "emp_hogar", "unemp_jr",
              "basque_navarra"]:
        if c in df.columns:
            agg_exprs.append(pl.col(c).first())

    person_firm_year = df.group_by("person_id", "firm_cc2", "year").agg(agg_exprs)
    # Free the large episode-level DataFrame (99M+ rows)
    del df
    gc.collect()

    # Main job = firm with highest annual contribution per person-year
    person_firm_year = person_firm_year.with_columns(
        (pl.col("firm_annual_contrib") == pl.col("firm_annual_contrib").max().over("person_id", "year"))
        .cast(pl.Int8).alias("is_main_job")
    )

    # Keep main job characteristics for the annual panel
    main_job = (
        person_firm_year
        .filter(pl.col("is_main_job") == 1)
        .sort("person_id", "year", "firm_annual_contrib", descending=[False, False, True])
        .unique(subset=["person_id", "year"], keep="first")
    )

    # -- Collapse to person-year -------------------------------------------
    print("  Collapsing to person-year...")
    # Sum contributions across all firms
    person_year = person_firm_year.group_by("person_id", "year").agg(
        pl.col("firm_annual_contrib").sum().alias("total_contribution"),
        pl.col("self_emp").max().alias("self_emp_year") if "self_emp" in person_firm_year.columns else pl.lit(None).alias("self_emp_year"),
        pl.col("emp_hogar").max().alias("emp_hogar_year") if "emp_hogar" in person_firm_year.columns else pl.lit(None).alias("emp_hogar_year"),
        pl.col("unemp_jr").max().alias("unemp_year") if "unemp_jr" in person_firm_year.columns else pl.lit(None).alias("unemp_year"),
        pl.col("basque_navarra").max().alias("basque_navarra_year") if "basque_navarra" in person_firm_year.columns else pl.lit(None).alias("basque_navarra_year"),
    )

    del person_firm_year
    gc.collect()

    # Merge main job info
    main_cols = ["person_id", "year", "firm_cc2", "firm_annual_contrib"]
    for c in ["contribution_regime", "contribution_group", "contract_type",
              "ptcoef", "firm_muni", "firm_workers", "firm_age",
              "firm_jur_type", "firm_jur_status", "firm_id", "firm_cc",
              "firm_main_prov", "firm_ett", "sector_cnae09", "sector_cnae93",
              "rel_contract"]:
        if c in main_job.columns:
            main_cols.append(c)

    person_year = person_year.join(
        main_job.select(main_cols).rename({
            "firm_cc2": "main_firm_cc2",
            "firm_annual_contrib": "main_firm_contrib",
        }),
        on=["person_id", "year"],
        how="left",
    )

    # Merge annual days
    person_year = person_year.join(days_annual, on=["person_id", "year"], how="left")

    print(f"  => {len(person_year):,} person-year rows, "
          f"{person_year.n_unique('person_id'):,} persons\n")

    return person_year


def save_step04(df: pl.DataFrame):
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    p = TEMP_DIR / "annual_from_episodes.parquet"
    df.write_parquet(p)
    print(f"  Saved {p}")
