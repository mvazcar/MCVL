"""
Main pipeline orchestration.
Runs all steps in sequence, with intermediate parquet checkpoints.
Replicates the full mcvl_data_processing Stata pipeline.

Pipeline flow:
  Step 01: Build individuals_full + firms_all panels (from PERSONAL+CONVIVIR+AFILIAD)
  Step 02: Merge COTIZA contributions with AFILIAD affiliations per cohort
  Step 03: Compute monthly days worked (wide format: days1..12)
  Step 04: Build annual summaries from wide format (contributions, days, main job)
  Step 05: Add demographics + household variables
  Step 06: Process FISCAL data, pensions, firm ID corrections (independent)
  Step 07: Final assembly: merge fiscal, pensions, CPI deflation, geographic vars
"""
import gc
import time
import polars as pl
from config import YEAR_FIRST, YEAR_LATEST, TEMP_DIR

from step01_panels import (
    build_individuals_full, build_individuals_last,
    build_firms_all, save_individuals, save_firms,
)
from step02_merge import merge_all_cohorts, save_step02
from step03_days import compute_monthly_days, save_step03
from step04_reshape import build_annual_from_wide, save_step04
from step05_other_vars import add_demographic_vars, save_step05
from step06_fiscal import run_step06, save_step06
from step07_final import build_annual_panel, save_step07


def run_pipeline(
    year_first: int = YEAR_FIRST,
    year_latest: int = YEAR_LATEST,
    resume_from: int | None = None,
) -> pl.DataFrame:
    """
    Full pipeline: read raw MCVL -> produce annual person-year panel.

    Parameters
    ----------
    year_first, year_latest : year range
    resume_from : step number (1-7) to resume from using saved parquets.
                  Loads only the parquets needed for the resume step.
    """
    t0 = time.time()
    r = resume_from or 0

    # == Step 01: Individuals + Firms panels =================================
    if r < 2:
        individuals_full = build_individuals_full(year_first, year_latest)
        individuals_last = build_individuals_last(individuals_full)
        save_individuals(individuals_full, individuals_last)
        del individuals_full
        gc.collect()

        firms_all = build_firms_all(year_first, year_latest)
        save_firms(firms_all)
        del firms_all
        gc.collect()
    else:
        print("Step 01: skipped (resume)")
        individuals_last = None

    t1 = time.time()
    print(f"  Step 01 done in {t1 - t0:.1f}s\n")

    # == Step 02: Merge contributions + affiliations =========================
    if r < 3:
        if individuals_last is None:
            individuals_last = pl.read_parquet(TEMP_DIR / "individuals_last.parquet")
        merged = merge_all_cohorts(individuals_last, year_first, year_latest)
        save_step02(merged)
        del individuals_last
        gc.collect()
    else:
        print("Step 02: skipped (resume)")
        merged = None

    t2 = time.time()
    print(f"  Step 02 done in {t2 - t1:.1f}s\n")

    # == Step 03: Monthly days worked ========================================
    if r < 4:
        if merged is None:
            merged = pl.read_parquet(TEMP_DIR / "merged_contrib_afil.parquet")
        print("=" * 60)
        print("STEP 03: Computing monthly days worked")
        print("=" * 60)
        with_days = compute_monthly_days(merged)
        save_step03(with_days)
        print(f"  => {len(with_days):,} rows\n")
        del merged
        gc.collect()
    else:
        print("Step 03: skipped (resume)")
        with_days = None

    t3 = time.time()
    print(f"  Step 03 done in {t3 - t2:.1f}s\n")

    # == Step 04: Annual summaries from wide format ==========================
    if r < 5:
        if with_days is None:
            print("Loading monthly_days from parquet...")
            with_days = pl.read_parquet(TEMP_DIR / "monthly_days.parquet")
        annual_episodes = build_annual_from_wide(with_days)
        save_step04(annual_episodes)
        del with_days
        gc.collect()
    else:
        print("Step 04: skipped (resume)")
        annual_episodes = None

    t4 = time.time()
    print(f"  Step 04 done in {t4 - t3:.1f}s\n")

    # == Step 05: Demographics + household vars ==============================
    if r < 6:
        if annual_episodes is None:
            annual_episodes = pl.read_parquet(TEMP_DIR / "annual_from_episodes.parquet")
        individuals_full = pl.read_parquet(TEMP_DIR / "individuals_full.parquet")
        annual_with_demos = add_demographic_vars(annual_episodes, individuals_full)
        save_step05(annual_with_demos)
        del individuals_full, annual_episodes
        gc.collect()
    else:
        print("Step 05: skipped (resume)")
        annual_with_demos = None

    t5 = time.time()
    print(f"  Step 05 done in {t5 - t4:.1f}s\n")

    # == Step 06: Fiscal, pensions, firm IDs (independent) ===================
    if r < 7:
        step06 = run_step06(year_first, year_latest)
        save_step06(step06)
        fiscal_work = step06["fiscal"]["work"]
        fiscal_unemp = step06["fiscal"]["unemp"]
        fiscal_prof = step06["fiscal"]["prof"]
        pensions = step06["pensions"]
        firm_id_corr = step06["firm_id_correction"]
        del step06
        gc.collect()
    else:
        print("Step 06: skipped (resume)")
        fiscal_work = pl.read_parquet(TEMP_DIR / "step06_fiscal_work.parquet")
        fiscal_unemp = pl.read_parquet(TEMP_DIR / "step06_fiscal_unemp.parquet")
        fiscal_prof = pl.read_parquet(TEMP_DIR / "step06_fiscal_prof.parquet")
        pensions = pl.read_parquet(TEMP_DIR / "step06_pensions.parquet")
        firm_id_corr = pl.read_parquet(TEMP_DIR / "step06_firm_id_correction.parquet")

    t6 = time.time()
    print(f"  Step 06 done in {t6 - t5:.1f}s\n")

    # == Step 07: Final annual panel =========================================
    if annual_with_demos is None:
        annual_with_demos = pl.read_parquet(TEMP_DIR / "annual_with_demographics.parquet")

    annual = build_annual_panel(
        annual_with_demos=annual_with_demos,
        fiscal_work=fiscal_work,
        fiscal_unemp=fiscal_unemp,
        fiscal_prof=fiscal_prof,
        pensions=pensions,
        firm_id_correction=firm_id_corr,
    )
    save_step07(annual)

    t7 = time.time()
    print(f"  Step 07 done in {t7 - t6:.1f}s\n")

    total = t7 - t0
    print("=" * 60)
    print(f"PIPELINE COMPLETE in {total / 60:.1f} minutes")
    print(f"  {annual.n_unique('person_id'):,} persons, {len(annual):,} person-year rows")
    print(f"  Output: {TEMP_DIR.parent / 'output'}")
    print("=" * 60)

    return annual
