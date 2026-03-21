"""
Step 07: Build final annual person-year panel.
Replicates 07_01_Prep_Data_RemoveAllAfter2_NotClustering.do.

Takes annual summary from step04+step05 and merges with fiscal data,
pensions. Adds CPI deflation, lagged income, geographic classification.

KEY DESIGN: Full panel without sample restrictions. All individuals kept.
"""
import polars as pl
from config import (
    YEAR_FIRST, YEAR_LATEST, FISCAL_FIRST, TEMP_DIR, OUTPUT_DIR,
    CPI, CPI_BASE, EDU_MAP_4, PROVINCE_TO_CA,
)


def build_annual_panel(
    annual_with_demos: pl.DataFrame,
    fiscal_work: pl.DataFrame,
    fiscal_unemp: pl.DataFrame,
    fiscal_prof: pl.DataFrame,
    pensions: pl.DataFrame,
    firm_id_correction: pl.DataFrame,
) -> pl.DataFrame:
    """
    Combine annual episode summary (with demographics) with fiscal data
    and pensions to produce the final annual panel.
    """
    print("=" * 60)
    print("STEP 07: Building final annual panel")
    print("=" * 60)

    df = annual_with_demos

    # -- Fix firm_id == 0 --------------------------------------------------
    print("  Fixing firm_id == 0...")
    if "firm_cc" in df.columns and "firm_id" in df.columns:
        # Ensure types match for join
        df = df.with_columns(pl.col("firm_cc").cast(pl.Utf8).alias("firm_cc"))
        firm_id_correction = firm_id_correction.with_columns(
            pl.col("firm_cc").cast(pl.Utf8))
        df = df.join(firm_id_correction, on="firm_cc", how="left")
        df = df.with_columns(
            pl.when(
                (pl.col("firm_id").cast(pl.Utf8).str.strip_chars() == "0")
                & pl.col("firm_id_correction").is_not_null()
            ).then(pl.col("firm_id_correction"))
            .otherwise(pl.col("firm_id"))
            .alias("firm_id")
        )
        if "firm_id_correction" in df.columns:
            df = df.drop("firm_id_correction")

    # -- Merge fiscal work data --------------------------------------------
    print("  Merging fiscal work data...")
    # Normalize firm_id: AFILIAD is Int64 (e.g. 841610),
    # FISCAL is zero-padded String (e.g. "000000000841610").
    # Convert both to plain integer strings for matching.
    if "firm_id" in df.columns:
        df = df.with_columns(pl.col("firm_id").cast(pl.Utf8).alias("firm_id"))
        fw = fiscal_work.rename({"wage": "fiscal_wage", "inkind": "fiscal_inkind"})
        fw = fw.with_columns(
            pl.col("firm_id").str.strip_chars().str.strip_chars_start("0")
            .replace("", "0")  # handle all-zero IDs
            .alias("firm_id")
        )
        df = df.join(fw, on=["person_id", "firm_id", "year"], how="left")
    else:
        # Fall back: aggregate fiscal work to person x year
        fw_py = fiscal_work.group_by("person_id", "year").agg(
            pl.col("wage").sum().alias("fiscal_wage"),
            pl.col("inkind").sum().alias("fiscal_inkind"),
        )
        df = df.join(fw_py, on=["person_id", "year"], how="left")

    # Fill missing wage/inkind
    for c in ["fiscal_wage", "fiscal_inkind"]:
        df = df.with_columns(pl.col(c).fill_null(0.0))

    df = df.with_columns(
        (pl.col("fiscal_wage") + pl.col("fiscal_inkind")).alias("wage"),
    )

    # -- Merge unemployment income -----------------------------------------
    print("  Merging unemployment + professional income...")
    df = df.join(fiscal_unemp, on=["person_id", "year"], how="left")
    df = df.join(fiscal_prof, on=["person_id", "year"], how="left")

    # -- Merge pensions ----------------------------------------------------
    print("  Merging pension records...")
    df = df.join(pensions, on="person_id", how="left")

    # -- Fill missing income fields ----------------------------------------
    for c in ["inc_unemp", "inc_prof", "inkind_unemp", "inkind_prof"]:
        if c in df.columns:
            df = df.with_columns(pl.col(c).fill_null(0.0))

    # -- Total income ------------------------------------------------------
    df = df.with_columns(
        (pl.col("wage")
         + pl.col("inc_unemp").fill_null(0.0)
         + pl.col("inc_prof").fill_null(0.0))
        .alias("tot_inc"),
    )

    # -- Province and Comunidad Autonoma -----------------------------------
    if "firm_muni" in df.columns:
        df = df.with_columns(
            (pl.col("firm_muni") // 1000).alias("province"),
        )
    elif "person_muni_latest" in df.columns:
        df = df.with_columns(
            (pl.col("person_muni_latest") // 1000).alias("province"),
        )
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Int32).alias("province"))

    ca_df = pl.DataFrame({
        "province": list(PROVINCE_TO_CA.keys()),
        "comunidad": list(PROVINCE_TO_CA.values()),
    }).with_columns(pl.col("province").cast(pl.Int32))
    df = df.join(ca_df, on="province", how="left")

    # -- 4-level education -------------------------------------------------
    edu4_df = pl.DataFrame({
        "education": list(EDU_MAP_4.keys()),
        "education_4": list(EDU_MAP_4.values()),
    }).with_columns(pl.col("education").cast(pl.Int32, strict=False))

    if "education" in df.columns:
        df = df.with_columns(pl.col("education").cast(pl.Int32, strict=False))
        df = df.join(edu4_df, on="education", how="left")
        df = df.rename({"education": "education_7cats", "education_4": "education"})

    # -- Age (integer) -----------------------------------------------------
    if "age" in df.columns:
        df = df.with_columns(
            pl.col("age").round(0).cast(pl.Int32, strict=False).alias("age_int"),
        )
    elif "birth_year" in df.columns:
        df = df.with_columns(
            (pl.col("year").cast(pl.Int32) - pl.col("birth_year")).alias("age_int"),
        )

    # -- Death year --------------------------------------------------------
    if "death_year_month" in df.columns:
        df = df.with_columns(
            (pl.col("death_year_month") // 100).alias("death_year"),
        )

    # -- CPI deflation -----------------------------------------------------
    print("  Deflating to 2018 euros...")
    cpi_df = pl.DataFrame({
        "year": list(CPI.keys()),
        "cpi": list(CPI.values()),
    }).with_columns(pl.col("year").cast(pl.Int16))
    df = df.join(cpi_df, on="year", how="left")
    df = df.with_columns(
        pl.col("cpi").fill_null(CPI_BASE),
        (pl.col("cpi").fill_null(CPI_BASE) / CPI_BASE).alias("cpi_factor"),
    )
    for c in ["wage", "inc_unemp", "inc_prof", "tot_inc"]:
        if c in df.columns:
            df = df.with_columns(
                (pl.col(c) / pl.col("cpi_factor")).alias(f"real_{c}"),
            )

    # -- Lagged income -----------------------------------------------------
    df = df.sort("person_id", "year")
    df = df.with_columns(
        pl.col("tot_inc").shift(1).over("person_id").alias("tot_inc_lag"),
    )

    # -- Days-based variables ----------------------------------------------
    if "days" in df.columns:
        df = df.with_columns(pl.col("days").fill_null(0))
        if "days_lag1" in df.columns:
            df = df.with_columns(
                (pl.col("days_lag1").fill_null(0) >= 360).cast(pl.Int8).alias("fullyear_lag1"),
            )

    n_persons = df.n_unique("person_id")
    print(f"\n  => Final panel: {len(df):,} person-year rows, "
          f"{n_persons:,} unique persons\n")

    return df


def save_step07(annual: pl.DataFrame):
    """Save the final annual panel."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    p = OUTPUT_DIR / "mcvl_annual_panel_full.parquet"
    annual.write_parquet(p)
    print(f"  Saved {p}")

    # Summary stats
    summary_cols = ["person_id"]
    agg_exprs = [pl.col("person_id").n_unique().alias("n_persons")]
    for c in ["real_wage", "real_tot_inc", "days"]:
        if c in annual.columns:
            agg_exprs.append(pl.col(c).mean().alias(f"mean_{c}"))

    summary = annual.group_by("year").agg(agg_exprs).sort("year")
    sp = OUTPUT_DIR / "annual_panel_summary.csv"
    summary.write_csv(sp)
    print(f"  Saved {sp}")
