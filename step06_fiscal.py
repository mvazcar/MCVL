"""
Step 06: Fix firm IDs, process fiscal data, pensions.
Replicates 06_01_FixIDs.do, 06_02_DatosFiscales_Unemp.do,
06_03_Pensions.do.

Note: annual days are computed in step 04 directly from the wide-format data.
"""
import polars as pl
from config import (
    YEAR_FIRST, YEAR_LATEST, FISCAL_FIRST, TEMP_DIR,
)
from readers import read_fiscal, read_prestac, read_all_afiliad


# =====================================================================
# 06_01: Fix firm IDs
# =====================================================================

def build_firm_id_correction(
    year_first: int = YEAR_FIRST,
    year_latest: int = YEAR_LATEST,
) -> pl.DataFrame:
    """
    Build lookup firm_cc -> firm_id_correction for cases where firm_id == 0.
    """
    print("  06_01: Building firm_id correction table...")
    frames = []
    for yr in range(year_first, year_latest + 1):
        afil = read_all_afiliad(yr)
        afil = afil.with_columns(
            (pl.col("exit_date") // 10000).cast(pl.Int32).alias("exit_yr"),
        )
        afil = afil.filter(pl.col("exit_yr") >= 2003)
        pairs = afil.select("firm_cc", "firm_id").unique()
        pairs = pairs.with_columns(pl.lit(yr).cast(pl.Int16).alias("year"))
        frames.append(pairs)

    all_pairs = pl.concat(frames)
    all_pairs = all_pairs.sort("firm_cc", "firm_id", "year", descending=[False, False, True])
    all_pairs = all_pairs.unique(subset=["firm_cc", "firm_id"], keep="first")

    counts = all_pairs.group_by("firm_cc").len().rename({"len": "totalno"})
    all_pairs = all_pairs.join(counts, on="firm_cc", how="left")

    problematic = all_pairs.filter(pl.col("totalno") > 1)
    has_zero = problematic.filter(
        pl.col("firm_id").cast(pl.Utf8).str.strip_chars() == "0"
    ).select("firm_cc").unique()
    problematic = problematic.join(has_zero, on="firm_cc", how="semi")
    problematic = problematic.filter(pl.col("totalno") < 4)

    fix2 = problematic.filter(
        (pl.col("totalno") == 2)
        & (pl.col("firm_id").cast(pl.Utf8).str.strip_chars() != "0")
    ).select("firm_cc", "firm_id")

    fix3 = (
        problematic
        .filter(
            (pl.col("totalno") == 3)
            & (pl.col("firm_id").cast(pl.Utf8).str.strip_chars() != "0")
        )
        .sort("firm_cc", "year", descending=[False, True])
        .unique(subset=["firm_cc"], keep="first")
        .select("firm_cc", "firm_id")
    )

    correction = pl.concat([fix2, fix3])
    correction = correction.rename({"firm_id": "firm_id_correction"})
    print(f"    => {len(correction):,} firm_cc corrections")
    return correction


# =====================================================================
# 06_02: Process fiscal (tax) data
# =====================================================================

def process_fiscal_data(
    year_first: int = FISCAL_FIRST,
    year_latest: int = YEAR_LATEST,
) -> dict[str, pl.DataFrame]:
    """
    Read all FISCAL files and split by payment type.
    """
    print("  06_02: Processing fiscal data...")
    frames = []
    for yr in range(year_first, year_latest + 1):
        print(f"    FISCAL {yr}...", end=" ", flush=True)
        df = read_fiscal(yr)
        frames.append(df)
        print(f"{len(df):,} rows")

    fiscal_all = pl.concat(frames, how="diagonal_relaxed")
    print(f"    => {len(fiscal_all):,} total fiscal records")

    # Unemployment
    unemp = (
        fiscal_all
        .filter(pl.col("payment_type") == "C")
        .group_by("person_id", "year")
        .agg(pl.col("wage").sum().alias("inc_unemp"),
             pl.col("inkind").sum().alias("inkind_unemp"))
    )

    # Professional activities
    prof = fiscal_all.filter(
        (pl.col("payment_type").is_in(["G", "H"]))
        & ~(
            ((pl.col("payment_type") == "G") & pl.col("payment_subtype").is_in([0, 2]))
            | ((pl.col("payment_type") == "H") & (pl.col("payment_subtype") == 3))
        )
    )
    prof_agg = (
        prof.group_by("person_id", "year")
        .agg(pl.col("wage").sum().alias("inc_prof"),
             pl.col("inkind").sum().alias("inkind_prof"))
    )

    # Pensions (fiscal)
    pension_fiscal = (
        fiscal_all
        .filter(pl.col("payment_type") == "B")
        .group_by("person_id", "year")
        .agg(pl.col("wage").sum().alias("inc_pension"))
    )

    # Work income (not B/C/D/E/G/H/I/J/K/L/M)
    work_agg = (
        fiscal_all
        .filter(~pl.col("payment_type").is_in(
            ["B", "C", "D", "E", "G", "H", "I", "J", "K", "L", "M"]))
        .group_by("person_id", "firm_id", "year")
        .agg(pl.col("wage").sum(), pl.col("inkind").sum())
    )

    return {
        "work": work_agg,
        "unemp": unemp,
        "prof": prof_agg,
        "pension_fiscal": pension_fiscal,
    }


# =====================================================================
# 06_03: Pensions (from PRESTAC)
# =====================================================================

def build_pensions(
    year_first: int = YEAR_FIRST,
    year_latest: int = YEAR_LATEST,
) -> pl.DataFrame:
    """Extract retirement year per person from PRESTAC files."""
    print("  06_03: Processing pension records...")
    frames = []
    for yr in range(year_first, year_latest + 1):
        print(f"    PRESTAC {yr}...", end=" ", flush=True)
        df = read_prestac(yr)
        frames.append(df)
        print(f"{len(df):,} records")

    all_pensions = pl.concat(frames, how="diagonal_relaxed")
    all_pensions = all_pensions.sort("person_id", "year", "date1",
                                      descending=[False, False, True])
    all_pensions = all_pensions.unique(subset=["person_id", "year"], keep="last")
    all_pensions = all_pensions.with_columns(
        (pl.col("date1") // 100).cast(pl.Int32).alias("retirementyear")
    )
    result = all_pensions.group_by("person_id").agg(
        pl.col("retirementyear").min()
    )
    print(f"    => {len(result):,} persons with retirement records")
    return result


# =====================================================================
# Combined step 06
# =====================================================================

def run_step06(
    year_first: int = YEAR_FIRST,
    year_latest: int = YEAR_LATEST,
) -> dict:
    """Run all step 06 components."""
    print("=" * 60)
    print("STEP 06: Fiscal data, pensions, firm ID fixes")
    print("=" * 60)

    firm_id_corr = build_firm_id_correction(year_first, year_latest)
    fiscal = process_fiscal_data(max(year_first, FISCAL_FIRST), year_latest)
    pensions = build_pensions(year_first, year_latest)

    return {
        "firm_id_correction": firm_id_corr,
        "fiscal": fiscal,
        "pensions": pensions,
    }


def save_step06(results: dict):
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    for key, df in results.items():
        if isinstance(df, dict):
            for subkey, subdf in df.items():
                p = TEMP_DIR / f"step06_{key}_{subkey}.parquet"
                subdf.write_parquet(p)
                print(f"  Saved {p}")
        else:
            p = TEMP_DIR / f"step06_{key}.parquet"
            df.write_parquet(p)
            print(f"  Saved {p}")
