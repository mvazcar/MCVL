"""
Step 01: Build individuals_full and firms_all panels.
Replicates 01_Past_info.do.

individuals_full: rectangularised person x year panel with demographics.
firms_all:        rectangularised firm_cc2 x year panel with firm chars.
"""
import gc
import polars as pl
from config import YEAR_FIRST, YEAR_LATEST, VALID_JUR_STATUS, TEMP_DIR
from readers import read_personal, read_convivir, read_all_afiliad


# ═══════════════════════════════════════════════════════════════════════════════
# INDIVIDUALS PANEL
# ═══════════════════════════════════════════════════════════════════════════════

def build_individuals_all(
    year_first: int = YEAR_FIRST,
    year_latest: int = YEAR_LATEST,
) -> pl.DataFrame:
    """
    Combine all PERSONAL + CONVIVIR files, determine MCVL_entry / MCVL_last,
    clean inconsistencies (birth_date, sex). Returns individuals_all before
    rectangularisation.
    """
    frames = []
    for yr in range(year_first, year_latest + 1):
        print(f"  PERSONAL+CONVIVIR {yr}...", end=" ", flush=True)
        pers = read_personal(yr)
        conv = read_convivir(yr)

        # Merge individuals with convivientes
        merged = pers.join(conv, on="person_id", how="left", suffix="_conv")

        # Resolve duplicate birth_date/sex cols from convivir
        # CONVIVIR has its own birth_date and sex for the person (same as PERSONAL)
        # We keep the PERSONAL version and drop the convivir person-level copy
        drop_cols = [c for c in merged.columns if c.endswith("_conv")]
        merged = merged.drop(drop_cols)

        merged = merged.with_columns(pl.lit(yr).cast(pl.Int16).alias("year"))
        merged = merged.unique(subset=["person_id"])
        frames.append(merged)
        del pers, conv
        print(f"{len(merged):,} persons")

    individuals_all = pl.concat(frames, how="diagonal_relaxed")
    del frames
    gc.collect()

    # MCVL_last and MCVL_entry per person
    agg = individuals_all.group_by("person_id").agg(
        pl.col("year").max().alias("MCVL_last"),
        pl.col("year").min().alias("MCVL_entry"),
    )
    individuals_all = individuals_all.join(agg, on="person_id", how="left")

    return individuals_all


def clean_inconsistencies(individuals_all: pl.DataFrame) -> pl.DataFrame:
    """
    Clean birth_date inconsistencies, fill missing from most recent year.
    Replicates the cleaning logic from 01_Past_info.do.
    """
    df = individuals_all

    # Parse birth_date string (YYYYMM) -> birth_year + birth_month
    df = df.with_columns(
        pl.col("birth_date").str.slice(0, 4).cast(pl.Int32, strict=False).alias("birth_year"),
        pl.col("birth_date").str.slice(4, 2).cast(pl.Int32, strict=False).alias("birth_month"),
    )

    # Drop persons whose birth_year changes across years
    birth_sd = df.group_by("person_id").agg(
        pl.col("birth_year").n_unique().alias("n_bdays"),
        pl.col("birth_year").drop_nulls().n_unique().alias("n_bdays_nonull"),
    )
    bad_bday = birth_sd.filter(pl.col("n_bdays_nonull") > 1).select("person_id")
    n_bad = len(bad_bday)
    if n_bad > 0:
        print(f"  Dropping {n_bad:,} persons with inconsistent birth_year")
    df = df.join(bad_bday, on="person_id", how="anti")

    # Drop persons with missing birth_year everywhere
    missing_by = df.group_by("person_id").agg(
        pl.col("birth_year").drop_nulls().len().alias("n_valid"),
    ).filter(pl.col("n_valid") == 0).select("person_id")
    n_miss = len(missing_by)
    if n_miss > 0:
        print(f"  Dropping {n_miss:,} persons with no birth_year")
    df = df.join(missing_by, on="person_id", how="anti")

    # For each person, take the MOST RECENT year's values (sort desc by year)
    # for sex, birth_prov, ss_reg_prov, birth_country, death_year_month, education
    # Use the most recent non-null/non-zero value
    df = df.sort("person_id", "year", descending=[False, True])

    fill_vars_nonzero = ["sex", "birth_prov", "ss_reg_prov"]
    fill_vars_null = ["birth_country", "death_year_month", "nationality", "education"]

    # For variables where 0 is like missing
    for var in fill_vars_nonzero:
        if var not in df.columns:
            continue
        df = df.with_columns(
            pl.when((pl.col(var) == 0) | pl.col(var).is_null())
            .then(None)
            .otherwise(pl.col(var))
            .alias(var)
        )

    # Take first non-null per person for each variable
    agg_exprs = [
        pl.col("MCVL_last").first(),
        pl.col("MCVL_entry").first(),
        pl.col("birth_year").drop_nulls().first(),
        pl.col("birth_month").drop_nulls().first(),
        pl.col("birth_date").drop_nulls().first(),
    ]
    for var in fill_vars_nonzero + fill_vars_null:
        if var not in df.columns:
            continue
        agg_exprs.append(pl.col(var).drop_nulls().first())

    # Also bring conviviente columns (from most recent year)
    conv_cols = [c for c in df.columns if c.startswith("birth_date") and c != "birth_date"
                 or c.startswith("sex") and c != "sex"]
    for c in conv_cols:
        agg_exprs.append(pl.col(c).first())

    # Also person_muni_latest, edu_code
    for c in ["person_muni_latest", "edu_code"]:
        if c in df.columns:
            agg_exprs.append(pl.col(c).drop_nulls().first())

    persons = df.group_by("person_id").agg(agg_exprs)

    # Drop invalid sex
    persons = persons.filter(pl.col("sex").is_in([1, 2]))

    # Recode sex: 2 -> 0 (female)
    persons = persons.with_columns(
        pl.when(pl.col("sex") == 2).then(0).otherwise(pl.col("sex")).alias("sex")
    )

    return persons


def rectangularise_individuals(persons: pl.DataFrame) -> pl.DataFrame:
    """
    Expand each person from age 16 (or MCVL_entry) until MCVL_last + 4 years
    (capped at YEAR_LATEST). Replicates the rectangularisation in 01_Past_info.do.
    """
    # Compute d16 = birth_year + 16
    persons = persons.with_columns(
        (pl.col("birth_year") + 16).alias("d16"),
    )

    # start_year = min(d16, MCVL_entry)
    # end_year = min(MCVL_last + 4, YEAR_LATEST)
    persons = persons.with_columns(
        pl.min_horizontal("d16", "MCVL_entry").alias("start_year"),
        pl.min_horizontal(
            pl.col("MCVL_last") + 4,
            pl.lit(YEAR_LATEST),
        ).alias("end_year"),
    )

    # Expand: create one row per year for each person
    persons = persons.with_columns(
        (pl.col("end_year") - pl.col("start_year") + 1).clip(lower_bound=1).alias("n_years"),
    )

    expanded = persons.select(
        pl.col("person_id"),
        pl.col("start_year"),
        pl.col("n_years"),
        pl.int_ranges(pl.lit(0), pl.col("n_years")).alias("year_offset"),
    ).explode("year_offset")

    expanded = expanded.with_columns(
        (pl.col("start_year") + pl.col("year_offset")).cast(pl.Int16).alias("year"),
    ).drop("start_year", "n_years", "year_offset")

    # Join back all person attributes
    individuals_full = expanded.join(
        persons.drop("start_year", "end_year", "n_years", "d16"),
        on="person_id",
        how="left",
    )

    return individuals_full.sort("person_id", "year")


def build_individuals_full(
    year_first: int = YEAR_FIRST,
    year_latest: int = YEAR_LATEST,
) -> pl.DataFrame:
    """Full pipeline: read -> combine -> clean -> rectangularise."""
    print("=" * 60)
    print("STEP 01a: Building individuals_all from PERSONAL+CONVIVIR")
    print("=" * 60)
    individuals_all = build_individuals_all(year_first, year_latest)
    print(f"  => {individuals_all.n_unique('person_id'):,} unique persons\n")

    print("  Cleaning inconsistencies...")
    persons = clean_inconsistencies(individuals_all)
    print(f"  => {len(persons):,} persons after cleaning\n")

    print("  Rectangularising...")
    individuals_full = rectangularise_individuals(persons)
    print(f"  => {len(individuals_full):,} person-year rows\n")

    return individuals_full


def build_individuals_last(individuals_full: pl.DataFrame) -> pl.DataFrame:
    """
    Create the lookup: person_id -> MCVL_last, birth_year, birth_date.
    Used to assign each person to their cohort extract year.
    """
    return (
        individuals_full
        .filter(pl.col("year") == pl.col("MCVL_last"))
        .select("person_id", "MCVL_last", "birth_year", "birth_date")
        .unique(subset=["person_id"])
    )


# ═══════════════════════════════════════════════════════════════════════════════
# FIRMS PANEL
# ═══════════════════════════════════════════════════════════════════════════════

def build_firms_all(
    year_first: int = YEAR_FIRST,
    year_latest: int = YEAR_LATEST,
) -> pl.DataFrame:
    """
    Build firm_cc2 x year panel from AFILIAD files.
    Replicates the EMPRESAS section of 01_Past_info.do.
    """
    print("=" * 60)
    print("STEP 01b: Building firms_all from AFILIAD files")
    print("=" * 60)

    frames = []
    for yr in range(year_first, year_latest + 1):
        print(f"  AFILIAD {yr}...", end=" ", flush=True)
        afil = read_all_afiliad(yr)

        # Keep only firm-level variables (drop person-level)
        firm_cols = [
            "firm_cc2", "firm_muni", "sector_cnae93", "sector_cnae09",
            "firm_workers", "firm_age", "firm_ett", "firm_jur_type",
            "firm_jur_status", "firm_main_prov",
        ]
        available = [c for c in firm_cols if c in afil.columns]
        firms = afil.select(available).unique()
        del afil
        gc.collect()

        # Clean firm_jur_type: non-numeric -> 0
        if "firm_jur_type" in firms.columns:
            firms = firms.with_columns(
                pl.col("firm_jur_type").fill_null(0).alias("firm_jur_type")
            )

        # Clean firm_jur_status: keep only valid codes
        if "firm_jur_status" in firms.columns:
            firms = firms.with_columns(
                pl.when(pl.col("firm_jur_status").is_in(list(VALID_JUR_STATUS)))
                .then(pl.col("firm_jur_status"))
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias("firm_jur_status")
            )

        # For duplicate firm_cc2, keep the one with most recent firm_age
        firms = firms.sort("firm_cc2", "firm_age", descending=[False, True])
        firms = firms.unique(subset=["firm_cc2"], keep="first")

        firms = firms.with_columns(pl.lit(yr).cast(pl.Int16).alias("year"))
        frames.append(firms)
        print(f"{len(firms):,} firms")

    firms_all = pl.concat(frames, how="diagonal_relaxed")

    # Determine first and last year each firm is observed
    firm_years = firms_all.group_by("firm_cc2").agg(
        pl.col("year").min().alias("ini_year"),
        pl.col("year").max().alias("end_year"),
    )
    firms_all = firms_all.join(firm_years, on="firm_cc2", how="left")

    # Fill forward/backward missing CNAE codes within firm
    firms_all = firms_all.sort("firm_cc2", "year")
    for col in ["sector_cnae93", "sector_cnae09", "firm_workers",
                 "firm_age", "firm_ett", "firm_jur_type",
                 "firm_jur_status", "firm_main_prov", "firm_muni"]:
        if col in firms_all.columns:
            firms_all = firms_all.with_columns(
                pl.col(col).forward_fill().over("firm_cc2").alias(col)
            )

    print(f"  => {firms_all.n_unique('firm_cc2'):,} unique firms, "
          f"{len(firms_all):,} firm-year rows\n")

    return firms_all


# ═══════════════════════════════════════════════════════════════════════════════
# SAVE
# ═══════════════════════════════════════════════════════════════════════════════

def save_individuals(individuals_full: pl.DataFrame, individuals_last: pl.DataFrame):
    """Save individuals_full and individuals_last parquets."""
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    p1 = TEMP_DIR / "individuals_full.parquet"
    individuals_full.write_parquet(p1)
    print(f"  Saved {p1}")

    p3 = TEMP_DIR / "individuals_last.parquet"
    individuals_last.write_parquet(p3)
    print(f"  Saved {p3}")


def save_firms(firms_all: pl.DataFrame):
    """Save firms_all parquet."""
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    p2 = TEMP_DIR / "firms_all.parquet"
    firms_all.write_parquet(p2)
    print(f"  Saved {p2}")
