"""
Step 05: Add demographic and household variables at annual level.
Replicates 05_OtherVars.do.

Merges person-year panel with individuals_full to get demographics and
convivientes data, computes age, family composition, contract type classification.
"""
import polars as pl
from config import TEMP_DIR


# ── Contract type reclassification (from 05_OtherVars.do) ──────────────────

CONTRACT_REMAP = {
    100: [1, 17, 22, 49, 69, 70, 71, 32, 33],
    109: [11, 35, 101, 109],
    130: [9, 29, 59],
    150: [8, 20, 28, 40, 41, 42, 43, 44, 45, 46, 47, 48, 50, 60, 61, 62,
          80, 86, 88, 91, 150, 151, 152, 153, 154, 155, 156, 157],
    200: [3],
    209: [38, 102, 209],
    250: [63, 81, 89, 98, 250, 251, 252, 253, 254, 255, 256, 257],
    300: [18],
    309: [185, 186, 309],
    350: [181, 182, 183, 184, 350, 351, 352, 353, 354, 355, 356, 357],
    401: [14],
    402: [15],
    410: [16, 72, 82, 92, 75],
    420: [58, 96],
    421: [85, 87, 97],
    430: [30, 31],
    441: [5],
    450: [457],
    500: [4],
    510: [73, 83, 93, 76],
    520: [6],
    540: [34],
    550: [557],
}

_CONTRACT_MAP = {}
for new_code, originals in CONTRACT_REMAP.items():
    for o in originals:
        _CONTRACT_MAP[o] = new_code

PERMANENT_CODES = {23, 65, 100, 109, 130, 131, 139, 141, 150, 189,
                   200, 209, 230, 231, 239, 250, 289,
                   300, 309, 330, 331, 339, 350, 389}
TEMPORARY_CODES = {7, 10, 12, 13, 24, 26, 27, 36, 37, 53, 54, 55, 56, 57,
                   64, 66, 67, 68, 74, 77, 78, 79, 84, 94,
                   401, 402, 403, 408, 410, 418, 420, 421, 430, 431, 441, 450, 451, 452,
                   500, 501, 502, 503, 508, 510, 518, 520, 530, 531, 541, 550, 551, 552}
AMBIGUOUS_CODES = {25, 19, 39, 51, 52, 90, 95, 540, 990}


def add_demographic_vars(
    annual: pl.DataFrame,
    individuals_full: pl.DataFrame,
) -> pl.DataFrame:
    """
    Merge annual panel with individuals_full to get demographics.
    Compute age, family composition, contract type classification.
    """
    print("=" * 60)
    print("STEP 05: Adding demographic/household variables")
    print("=" * 60)

    # Select columns from individuals_full
    indiv_cols = ["person_id", "year", "birth_year", "birth_month", "birth_date",
                  "sex", "nationality", "birth_country", "death_year_month",
                  "education", "person_muni_latest",
                  "MCVL_entry", "MCVL_last"]

    # Add conviviente columns
    conv_cols = [c for c in individuals_full.columns
                 if (c.startswith("birth_date") and c != "birth_date")
                 or (c.startswith("sex") and c != "sex")]
    indiv_cols.extend(conv_cols)
    available = [c for c in indiv_cols if c in individuals_full.columns]

    indiv_subset = individuals_full.select(available)

    # Merge (m:1 on person_id, year)
    df = annual.join(indiv_subset, on=["person_id", "year"], how="left", suffix="_indiv")

    # Resolve overlapping columns
    for c in indiv_subset.columns:
        if c in ("person_id", "year"):
            continue
        c_indiv = f"{c}_indiv"
        if c_indiv in df.columns:
            df = df.with_columns(
                pl.coalesce([pl.col(c_indiv), pl.col(c)]).alias(c)
            ).drop(c_indiv)

    # -- Age computation (annual precision) ---------------------------------
    if "birth_year" in df.columns:
        df = df.with_columns(
            (pl.col("year").cast(pl.Float64) - pl.col("birth_year").cast(pl.Float64)).alias("age")
        )

    # Entry age: use MCVL_entry year
    df = df.with_columns(
        (pl.col("MCVL_entry").cast(pl.Float64) - pl.col("birth_year").cast(pl.Float64))
        .alias("entryage")
    )

    # -- Family composition from convivientes (annual level) ----------------
    for i in range(2, 11):
        bd_col = f"birth_date{i}"
        if bd_col not in df.columns:
            continue
        df = df.with_columns(
            pl.col(bd_col).cast(pl.Utf8).str.strip_chars().str.slice(0, 4)
            .cast(pl.Int32, strict=False).alias(f"_yb{i}"),
        )
        df = df.with_columns(
            pl.when(pl.col(f"_yb{i}").is_not_null()).then(
                pl.col("year").cast(pl.Float64) - pl.col(f"_yb{i}").cast(pl.Float64)
            ).otherwise(None).alias(f"age{i}")
        )
        df = df.with_columns(
            pl.when(pl.col(f"age{i}") < 0).then(None).otherwise(pl.col(f"age{i}"))
            .round(0).cast(pl.Int32, strict=False).alias(f"age{i}_b")
        )

    # Family size and age groups
    age_b_cols = [f"age{i}_b" for i in range(2, 11) if f"age{i}_b" in df.columns]
    if age_b_cols:
        df = df.with_columns(
            (1 + pl.sum_horizontal(
                *[pl.col(c).is_not_null().cast(pl.Int32) for c in age_b_cols]
            )).alias("famsize"),
            pl.sum_horizontal(*[
                pl.col(c).is_between(0, 6).fill_null(False).cast(pl.Int32) for c in age_b_cols
            ]).alias("famsize_06"),
            pl.sum_horizontal(*[
                pl.col(c).is_between(7, 15).fill_null(False).cast(pl.Int32) for c in age_b_cols
            ]).alias("famsize_715"),
            pl.sum_horizontal(*[
                pl.col(c).is_between(65, 120).fill_null(False).cast(pl.Int32) for c in age_b_cols
            ]).alias("famsize_a65"),
        )

    # -- Contract type classification (for main job) ------------------------
    if "contract_type" in df.columns:
        ct_map = pl.DataFrame({
            "contract_type": list(_CONTRACT_MAP.keys()),
            "contractb": list(_CONTRACT_MAP.values()),
        }).with_columns(pl.col("contract_type").cast(pl.Int64))

        df = df.join(ct_map, on="contract_type", how="left")
        df = df.with_columns(
            pl.coalesce(["contractb", "contract_type"]).alias("contractb")
        )
        df = df.with_columns(
            pl.when(pl.col("contractb").is_in(list(PERMANENT_CODES))).then(1)
            .when(pl.col("contractb").is_in(list(TEMPORARY_CODES))).then(0)
            .when(pl.col("contractb").is_in(list(AMBIGUOUS_CODES))).then(None)
            .otherwise(None)
            .alias("permanent")
        )

    # -- Drop intermediate columns -----------------------------------------
    drop_cols = [f"_yb{i}" for i in range(2, 11)]
    drop_cols += [f"age{i}" for i in range(2, 11)]
    drop_cols += [f"age{i}_b" for i in range(2, 11)]
    drop_cols += [f"birth_date{i}" for i in range(2, 11)]
    sex_conv = [f"sex{i}" for i in range(2, 11) if f"sex{i}" in df.columns]
    drop_cols += sex_conv
    drop_cols = [c for c in drop_cols if c in df.columns]
    df = df.drop(drop_cols)

    n_persons = df.n_unique("person_id")
    print(f"  => {len(df):,} rows, {n_persons:,} persons\n")
    return df


def save_step05(df: pl.DataFrame):
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    p = TEMP_DIR / "annual_with_demographics.parquet"
    df.write_parquet(p)
    print(f"  Saved {p}")
