"""
Step 02: Merge contributions with affiliations per cohort.
Replicates 02_MergeMCVL_05_12.do + 02_MergeMCVL_13_latest.do.

For each MCVL extract year, takes persons whose MCVL_last == that year,
merges their contribution bases with affiliation episodes.
Episodes are expanded to year-level within [1960..YEAR_LATEST].
"""
import polars as pl
from config import YEAR_FIRST, YEAR_LATEST, TEMP_DIR
from readers import read_all_cotiza, read_cotiza_autonomous, read_all_afiliad


def _read_and_dedup_cotiza(
    extract_year: int, cohort_pids: pl.DataFrame,
) -> pl.DataFrame:
    """Read COTIZA (regular + autonomous) for one extract year, filter to cohort."""
    contrib = read_all_cotiza(extract_year)
    contrib = contrib.join(cohort_pids, on="person_id", how="semi")

    try:
        contrib_aut = read_cotiza_autonomous(extract_year)
        contrib_aut = contrib_aut.join(cohort_pids, on="person_id", how="semi")
        contrib = contrib.join(
            contrib_aut, on=["person_id", "firm_cc2", "year"],
            how="full", coalesce=True,
        )
    except Exception:
        for m in range(1, 13):
            contrib = contrib.with_columns(
                pl.lit(None, dtype=pl.Int64).alias(f"contribution_aut_{m}")
            )

    # Deduplicate: max contribution per pid+firm+year+month
    agg_exprs = []
    for m in range(1, 13):
        for prefix in ("contribution_", "contribution_aut_"):
            cn = f"{prefix}{m}"
            if cn in contrib.columns:
                agg_exprs.append(pl.col(cn).max())

    return contrib.group_by("person_id", "firm_cc2", "year").agg(agg_exprs)


def _read_and_prep_afiliad(
    extract_year: int, cohort_pids: pl.DataFrame,
) -> pl.DataFrame:
    """Read AFILIAD for one extract year, filter to cohort, parse dates."""
    afil = read_all_afiliad(extract_year)
    afil = afil.join(cohort_pids, on="person_id", how="semi")

    # Remove exact duplicate episodes
    afil = afil.unique(subset=["person_id", "entry_date", "exit_date", "firm_cc2"])

    # Parse entry/exit dates (YYYYMMDD integers)
    afil = afil.with_columns(
        (pl.col("entry_date") // 10000).cast(pl.Int16).alias("entry_year"),
        (pl.col("exit_date") // 10000).cast(pl.Int16).alias("exit_year"),
    )

    # Drop episodes with negative duration
    afil = afil.filter(pl.col("exit_date") >= pl.col("entry_date"))
    return afil


def _expand_and_merge(
    afil: pl.DataFrame, contrib: pl.DataFrame,
    min_year: int = 1960, max_year: int = YEAR_LATEST,
) -> pl.DataFrame:
    """
    Expand affiliation episodes to year-level (capped to [min_year, max_year])
    and left-join COTIZA data. Processes in batches to control memory.
    """
    # Cap expansion range
    afil = afil.with_columns(
        pl.max_horizontal(pl.col("entry_year"), pl.lit(min_year)).cast(pl.Int16).alias("exp_start"),
        pl.min_horizontal(pl.col("exit_year"), pl.lit(max_year)).cast(pl.Int16).alias("exp_end"),
    ).with_columns(
        (pl.col("exp_end") - pl.col("exp_start") + 1).clip(lower_bound=0).alias("n_years"),
    ).filter(pl.col("n_years") > 0)

    # Expand all episodes to year-level at once
    expanded = afil.with_columns(
        pl.int_ranges(pl.lit(0), pl.col("n_years")).alias("yr_offset"),
    ).explode("yr_offset").with_columns(
        (pl.col("exp_start") + pl.col("yr_offset")).cast(pl.Int16).alias("year"),
    ).drop("yr_offset", "n_years", "exp_start", "exp_end")

    # Left join with COTIZA
    merged = expanded.join(
        contrib,
        on=["person_id", "firm_cc2", "year"],
        how="left",
    )

    return merged


def merge_one_cohort(
    extract_year: int,
    individuals_last: pl.DataFrame,
) -> pl.DataFrame:
    """
    For a single MCVL extract year:
    1. Read COTIZA (regular + autonomous)
    2. Read AFILIAD
    3. Keep only persons with MCVL_last == extract_year
    4. Expand episodes to year-level, merge with COTIZA
    """
    cohort = individuals_last.filter(
        pl.col("MCVL_last") == extract_year
    ).select("person_id", "birth_year")

    if len(cohort) == 0:
        return pl.DataFrame()

    print(f"  {extract_year}: {len(cohort):,} persons in cohort")
    cohort_pids = cohort.select("person_id")

    # Contributions
    print(f"    Reading COTIZA...", end=" ", flush=True)
    contrib = _read_and_dedup_cotiza(extract_year, cohort_pids)
    print(f"{len(contrib):,} person-firm-year rows")

    # Affiliations
    print(f"    Reading AFILIAD...", end=" ", flush=True)
    afil = _read_and_prep_afiliad(extract_year, cohort_pids)
    print(f"{len(afil):,} episodes")

    # Expand and merge in batches
    print(f"    Expanding+merging...", end=" ", flush=True)
    merged = _expand_and_merge(afil, contrib)

    if len(merged) == 0:
        print("no data")
        return pl.DataFrame()

    print(f"{len(merged):,} rows")
    return merged


def merge_all_cohorts(
    individuals_last: pl.DataFrame,
    year_first: int = YEAR_FIRST,
    year_latest: int = YEAR_LATEST,
) -> pl.DataFrame:
    """
    Loop over all extract years, merge contributions+affiliations.
    Saves each cohort to a temp parquet to manage memory, then reads all back.
    """
    import gc

    print("=" * 60)
    print("STEP 02: Merging contributions + affiliations by cohort")
    print("=" * 60)

    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    cohort_files = []

    for yr in range(year_first, year_latest + 1):
        result = merge_one_cohort(yr, individuals_last)
        if len(result) > 0:
            p = TEMP_DIR / f"cohort_{yr}.parquet"
            result.write_parquet(p)
            cohort_files.append(p)
            del result
            gc.collect()

    # Read back lazily and concatenate (avoids loading all into memory at once)
    print("\n  Concatenating cohorts...", end=" ", flush=True)
    merged = pl.scan_parquet(cohort_files).collect()
    gc.collect()

    # Cleanup temp cohort files
    for p in cohort_files:
        p.unlink(missing_ok=True)

    print(f"{len(merged):,} rows, {merged.n_unique('person_id'):,} persons\n")
    return merged


def save_step02(merged: pl.DataFrame):
    """Save intermediate parquet."""
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    p = TEMP_DIR / "merged_contrib_afil.parquet"
    merged.write_parquet(p)
    print(f"  Saved {p}")
