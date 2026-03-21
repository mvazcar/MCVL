"""
Readers for MCVL raw semicolon-delimited files.
Each reader returns a polars LazyFrame / DataFrame with standardised column names.
Handles format changes across eras (2005-2024).
"""
import polars as pl
from config import (
    raw_path, afiliad_parts, cotiza_parts_regular,
    PERSONAL_POS, CONVIVIR_N_FIELDS,
    AFIL_COMMON, AFIL_CNAE, afiliad_era,
    COTIZA_FIELDS, cotiza_era,
    FISCAL_POS, fiscal_era,
    PRESTAC_POS, PENSION_CLASSES, PARTIAL_PENSION_CLASSES,
    EDU_MAP_7,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _read_raw(path, **kw):
    """Read a semicolon-delimited raw MCVL file."""
    return pl.read_csv(
        path, separator=";", has_header=False, infer_schema=False,
        truncate_ragged_lines=True, encoding="utf8-lossy", **kw,
    )


def _safe_int(expr: pl.Expr, dtype=pl.Int64) -> pl.Expr:
    return expr.str.strip_chars().cast(dtype, strict=False)


def _safe_str(expr: pl.Expr) -> pl.Expr:
    return expr.str.strip_chars()


def _col(cols, idx):
    """Get column name by 0-based index, returning None if idx is None."""
    if idx is None:
        return None
    return cols[idx]


# ── PERSONAL ──────────────────────────────────────────────────────────────────

def read_personal(year: int) -> pl.DataFrame:
    """Read PERSONAL file. Returns one row per person (deduped)."""
    path = raw_path(year, "PERSONAL")
    df = _read_raw(path)
    cols = df.columns
    p = PERSONAL_POS

    result = df.select(
        _safe_str(pl.col(cols[p["person_id"]])).alias("person_id"),
        _safe_str(pl.col(cols[p["birth_date"]])).alias("birth_date"),
        _safe_int(pl.col(cols[p["sex"]]), pl.Int8).alias("sex"),
        _safe_str(pl.col(cols[p["nationality"]])).alias("nationality_raw"),
        _safe_str(pl.col(cols[p["birth_prov"]])).alias("birth_prov_raw"),
        _safe_int(pl.col(cols[p["ss_reg_prov"]]), pl.Int32).alias("ss_reg_prov"),
        _safe_int(pl.col(cols[p["person_muni_latest"]]), pl.Int32).alias("person_muni_latest"),
        _safe_str(pl.col(cols[p["death_year_month"]])).alias("death_year_month_raw"),
        _safe_str(pl.col(cols[p["birth_country"]])).alias("birth_country_raw"),
        _safe_str(pl.col(cols[p["edu_code"]])).alias("edu_code"),
    )

    # Parse nationality: strip "N", cast int, 99->null
    for c in ("nationality_raw", "birth_country_raw"):
        alias = c.replace("_raw", "")
        result = result.with_columns(
            pl.col(c).str.replace(r"^N", "").str.strip_chars()
            .cast(pl.Int32, strict=False)
            .replace(99, None)
            .alias(alias)
        )

    # Parse birth_prov
    result = result.with_columns(
        pl.col("birth_prov_raw").str.replace(r"^N", "").str.strip_chars()
        .cast(pl.Int32, strict=False).alias("birth_prov")
    )

    # Parse death_year_month
    result = result.with_columns(
        _safe_int(pl.col("death_year_month_raw"), pl.Int64).alias("death_year_month")
    )

    # Use birth_country as fallback for nationality
    result = result.with_columns(
        pl.when(pl.col("nationality").is_null())
        .then(pl.col("birth_country"))
        .otherwise(pl.col("nationality"))
        .alias("nationality")
    )

    # Map edu_code -> education (7-level)
    edu_df = pl.DataFrame({
        "edu_code": list(EDU_MAP_7.keys()),
        "education": list(EDU_MAP_7.values()),
    })
    result = result.join(edu_df, on="edu_code", how="left")

    # Deduplicate: drop full-row dupes first, then drop persons with conflicting rows
    result = result.unique()
    dup_pids = (
        result.group_by("person_id").len()
        .filter(pl.col("len") > 1)
        .select("person_id")
    )
    result = result.join(dup_pids, on="person_id", how="anti")

    return result.select(
        "person_id", "birth_date", "sex", "nationality", "birth_country",
        "birth_prov", "ss_reg_prov", "person_muni_latest", "death_year_month",
        "edu_code", "education",
    )


# ── CONVIVIR ──────────────────────────────────────────────────────────────────

def read_convivir(year: int) -> pl.DataFrame:
    """Read CONVIVIR file. Returns person_id + birth_date/sex for up to 10 convivientes."""
    path = raw_path(year, "CONVIVIR")
    df = _read_raw(path)
    cols = df.columns

    renames = {"person_id": cols[0], "birth_date": cols[1], "sex": cols[2]}
    for i in range(2, 11):  # convivientes 2-10
        bd_idx = 1 + (i - 1) * 2      # 3, 5, 7, ...
        sx_idx = bd_idx + 1            # 4, 6, 8, ...
        if sx_idx < len(cols):
            renames[f"birth_date{i}"] = cols[bd_idx]
            renames[f"sex{i}"] = cols[sx_idx]

    result = df.select([
        _safe_str(pl.col(v)).alias(k) if k == "person_id"
        else pl.col(v).alias(k)
        for k, v in renames.items()
    ])

    # Deduplicate
    result = result.unique(subset=["person_id"])
    return result


# ── AFILIAD ───────────────────────────────────────────────────────────────────

def read_afiliad(year: int, part: int) -> pl.DataFrame:
    """Read one AFILIAD partition. Returns standardised columns."""
    path = raw_path(year, "AFILIAD", part)
    df = _read_raw(path)
    cols = df.columns
    era = afiliad_era(year)
    cnae = AFIL_CNAE[era]

    # Build select expressions for all common fields
    exprs = []
    for name, idx in AFIL_COMMON.items():
        if idx >= len(cols):
            continue
        c = cols[idx]
        if name in ("person_id", "firm_cc2", "firm_cc", "firm_jur_status",
                     "new_date_contract1", "new_date_contract2",
                     "new_date_contribution_group",
                     "prev_contract1", "prev_contract2",
                     "prev_ptcoef1", "prev_ptcoef2", "prev_contribution_group"):
            exprs.append(_safe_str(pl.col(c)).alias(name))
        else:
            exprs.append(_safe_int(pl.col(c)).alias(name))

    # CNAE fields
    if cnae.get("sector_cnae93") is not None and cnae["sector_cnae93"] < len(cols):
        exprs.append(_safe_int(pl.col(cols[cnae["sector_cnae93"]])).alias("sector_cnae93"))
    else:
        exprs.append(pl.lit(None, dtype=pl.Int64).alias("sector_cnae93"))

    if cnae.get("sector_cnae09") is not None and cnae["sector_cnae09"] < len(cols):
        exprs.append(_safe_int(pl.col(cols[cnae["sector_cnae09"]])).alias("sector_cnae09"))
    else:
        exprs.append(pl.lit(None, dtype=pl.Int64).alias("sector_cnae09"))

    result = df.select(exprs)
    return result


def read_all_afiliad(year: int) -> pl.DataFrame:
    """Read and concatenate all AFILIAD partitions for one year."""
    parts = afiliad_parts(year)
    frames = [read_afiliad(year, p) for p in parts]
    return pl.concat(frames, how="diagonal_relaxed")


# ── COTIZA ────────────────────────────────────────────────────────────────────

def read_cotiza(year: int, part: int, autonomous: bool = False) -> pl.DataFrame:
    """
    Read one COTIZA partition.
    autonomous=True reads COTIZA13 (self-employed contribution bases).
    Returns: person_id, firm_cc2, year, contribution_1..12
    """
    path = raw_path(year, "COTIZA", part)
    df = _read_raw(path)
    cols = df.columns
    era = cotiza_era(year)
    fld = COTIZA_FIELDS[era]
    prefix = "contribution_aut_" if autonomous else "contribution_"

    exprs = [
        _safe_str(pl.col(cols[fld["person_id"]])).alias("person_id"),
        _safe_str(pl.col(cols[fld["firm_cc2"]])).alias("firm_cc2"),
        _safe_int(pl.col(cols[fld["year"]]), pl.Int16).alias("year"),
    ]

    start = fld["contrib_start"]
    for m in range(12):
        idx = start + m
        if idx < len(cols):
            exprs.append(_safe_int(pl.col(cols[idx])).alias(f"{prefix}{m+1}"))
        else:
            exprs.append(pl.lit(None, dtype=pl.Int64).alias(f"{prefix}{m+1}"))

    return df.select(exprs)


def read_all_cotiza(year: int) -> pl.DataFrame:
    """Read all 12 regular COTIZA partitions for one year, append them."""
    frames = [read_cotiza(year, p) for p in cotiza_parts_regular()]
    return pl.concat(frames)


def read_cotiza_autonomous(year: int) -> pl.DataFrame:
    """Read COTIZA13 (autonomous contributions) for one year."""
    return read_cotiza(year, 13, autonomous=True)


# ── FISCAL ────────────────────────────────────────────────────────────────────

def read_fiscal(year: int) -> pl.DataFrame:
    """
    Read FISCAL file for one year.
    Returns: person_id, firm_id, firm_jur_status, payment_type, payment_subtype,
             wage (= payment_amount + wage_il/amount_il), inkind (= payment_inkind + inkind_il)
    All in euro-cents (raw).
    """
    path = raw_path(year, "FISCAL")
    df = _read_raw(path)
    cols = df.columns
    era = fiscal_era(year)
    fld = FISCAL_POS[era]

    # Build select expressions including IL fields based on era
    select_exprs = [
        _safe_str(pl.col(cols[fld["person_id"]])).alias("person_id"),
        _safe_str(pl.col(cols[fld["firm_id"]])).alias("firm_id"),
        _safe_str(pl.col(cols[fld["firm_jur_status"]])).alias("firm_jur_status"),
        _safe_str(pl.col(cols[fld["payment_type"]])).alias("payment_type"),
        _safe_int(pl.col(cols[fld["payment_subtype"]]), pl.Int32).alias("payment_subtype"),
        _safe_int(pl.col(cols[fld["payment_amount"]])).alias("payment_amount"),
        (
            _safe_int(pl.col(cols[fld["payment_inkind"]]))
            if fld["payment_inkind"] is not None else pl.lit(0)
        ).alias("payment_inkind"),
    ]

    # IL fields vary by era
    if era == "2006-2015":
        select_exprs.append(pl.lit(0).alias("wage_il"))
        select_exprs.append(pl.lit(0).alias("inkind_il"))
    elif era == "2016":
        select_exprs.append(
            _safe_int(pl.col(cols[fld["amount_il"]])).fill_null(0).alias("wage_il"))
        select_exprs.append(pl.lit(0).alias("inkind_il"))
    else:  # 2017-2024
        select_exprs.append(
            _safe_int(pl.col(cols[fld["wage_il"]])).fill_null(0).alias("wage_il"))
        select_exprs.append(
            _safe_int(pl.col(cols[fld["inkind_il"]])).fill_null(0).alias("inkind_il"))

    result = df.select(select_exprs)

    # Compute composite wage and inkind (still in cents)
    result = result.with_columns(
        (pl.col("payment_amount").fill_null(0) + pl.col("wage_il")).alias("wage_cents"),
        (pl.col("payment_inkind").fill_null(0) + pl.col("inkind_il")).alias("inkind_cents"),
    )

    # Compute final wage = (wage_cents + inkind_cents) / 100
    result = result.with_columns(
        ((pl.col("wage_cents") + pl.col("inkind_cents")).cast(pl.Float64) / 100.0).alias("wage"),
        (pl.col("inkind_cents").cast(pl.Float64) / 100.0).alias("inkind"),
    )

    result = result.with_columns(pl.lit(year).cast(pl.Int16).alias("year"))

    return result.select(
        "person_id", "firm_id", "firm_jur_status",
        "payment_type", "payment_subtype", "year", "wage", "inkind",
    )


# ── PRESTAC ───────────────────────────────────────────────────────────────────

def read_prestac(year: int) -> pl.DataFrame:
    """Read PRESTAC file. Returns pension records with class, date1, regimep."""
    path = raw_path(year, "PRESTAC")
    df = _read_raw(path)
    cols = df.columns
    p = PRESTAC_POS

    result = df.select(
        _safe_str(pl.col(cols[p["person_id"]])).alias("person_id"),
        _safe_int(pl.col(cols[p["year"]]), pl.Int16).alias("year"),
        _safe_str(pl.col(cols[p["class"]])).alias("class"),
        _safe_int(pl.col(cols[p["regimep"]]), pl.Int32).alias("regimep"),
        _safe_int(pl.col(cols[p["date1"]])).alias("date1"),
    )

    # Filter: keep only retirement-related classes, exclude partial
    result = result.filter(
        pl.col("class").is_in(PENSION_CLASSES)
        & ~pl.col("class").is_in(PARTIAL_PENSION_CLASSES)
    )

    # Drop regimes 36, 37 (as in Stata)
    result = result.filter(
        ~pl.col("regimep").is_in([36, 37])
    )

    return result


# ── DIVISION ──────────────────────────────────────────────────────────────────

def read_division(year: int) -> pl.DataFrame:
    """Read DIVISION file (space-delimited). Maps person_id to AFILIAD/COTIZA file numbers."""
    path = raw_path(year, "DIVISION")
    df = pl.read_csv(
        path, separator=" ", has_header=False, infer_schema=False,
        truncate_ragged_lines=True, encoding="utf8-lossy",
    )
    cols = df.columns
    return df.select(
        _safe_str(pl.col(cols[0])).alias("person_id"),
        _safe_int(pl.col(cols[1]), pl.Int8).alias("relacionesfile"),
        _safe_int(pl.col(cols[2]), pl.Int8).alias("basesfile"),
    )
