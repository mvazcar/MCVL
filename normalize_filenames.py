"""
Unzip and normalize MCVL raw files from raw_zipped/ to raw/.

Full pipeline:
    raw_zipped/1145{YY}{S|T}.zip  -->  unzip  -->  normalize names  -->  raw/{YEAR}/MCVL{YEAR}{TYPE}{N}_CDF.TXT

The original zips (from DGOSS) produce wildly inconsistent filenames across
years. This script handles:

  2004  Mainframe names   EST.LABT2004.AFILANON.FICHERO1.TXT  (some corrupted)
  2005  Extra B prefix    MCVL2005BAFILIAD1.TXT
  2006  No year, .trs     AFILANON1.trs, CONVIVI.trs, DATOS_FISCALES.trs
  2007  No year, .trs     (same as 2006)
  2008  No year, .trs     (same as 2006)
  2009  Inner zips         MCVL2009COTIZA1.zip (+ missing _CDF suffix)
  2010  Typo + case        AFLIAID1 (should be AFILIAD1), .TXt, .txt
  2011  .F2013 infix       MCVL2011.F2013.AFILIA1_CDF.txt
  2013  Stale 2012 files   MCVL2012FISCAL_CDF.TXT in 2013 folder
  2015  Double-F typo      MCVL2015COTIZA11_CDFF.TXT
  2016  SDF typo           MCVL2016PERSONAL_SDF.TXT
  2017  Nested subdir      MCVL2017/MCVL2017/ subfolder inside zip
  2020  Missing _CDF       MCVL2020PERSONAL.TXT
  2021  Missing _CDF       MCVL2021PERSONAL.TXT

Usage:
    python normalize_filenames.py                     # dry run (shows what would happen)
    python normalize_filenames.py --execute           # unzip + rename
    python normalize_filenames.py --rename-only       # skip unzip, just rename existing files
    python normalize_filenames.py --rename-only --execute
"""

import os
import re
import zipfile
import shutil
import argparse
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RAW_ZIPPED = BASE_DIR / "raw_zipped"
RAW_DIR = BASE_DIR / "raw"

# Zip filename pattern: 1145{YY}{S|T}.zip -> year 20{YY}
ZIP_PATTERN = re.compile(r"1145(\d{2})[ST]\.zip", re.IGNORECASE)

# --------------------------------------------------------------------------
# Year ranges
# --------------------------------------------------------------------------
YEARS = range(2004, 2025)

# --------------------------------------------------------------------------
# 2004: mainframe-style names
# --------------------------------------------------------------------------
MAP_2004 = {
    "EST.LABT2004.AFILANON.FICHERO1.TXT": "MCVL2004AFILIAD1_CDF.TXT",
    "EST.LABT2004.AFILANON.FICHERO2.EST.LABT2004.AFILANON.FICHERO2.EST.LAB.TXT": "MCVL2004AFILIAD2_CDF.TXT",
    "EST.LABT2004.AFILANON.FICHERO3.TXT": "MCVL2004AFILIAD3_CDF.TXT",
    "EST.LABT2004.PERSANON.TXT": "MCVL2004PERSONAL_CDF.TXT",
    "EST.LABT2004.PREANON.TXT": "MCVL2004PRESTAC_CDF.TXT",
    "EST.LABT2004.DIVISION.TXT": "MCVL2004DIVISION_CDF.TXT",
    "EST.LABT2004.CPROPIA.ANONIMO.TXT": "MCVL2004CONVIVIR_CDF.TXT",
}
for _n in range(1, 13):
    _key = f"EST.LABT2004.COTIANON.FICHER{'O' if _n < 10 else ''}{_n}.TXT"
    MAP_2004[_key] = f"MCVL2004COTIZA{_n}_CDF.TXT"
# Corrupted COTIZA6 overwrites normal entry
MAP_2004["EST.LABT2004.COTIANON.FICHERO6.EST.LABT2004.COTIANON.FICHERO6.EST.LAB.TXT"] = "MCVL2004COTIZA6_CDF.TXT"
MAP_2004.pop("EST.LABT2004.COTIANON.FICHERO6.TXT", None)

# --------------------------------------------------------------------------
# 2006-2008: .trs names with no year
# --------------------------------------------------------------------------
MAP_TRS_SPECIAL = {
    "CONVIVI.trs": "CONVIVIR",
    "PERSANON.trs": "PERSONAL",
    "PREANON.trs": "PRESTAC",
    "DATOS_FISCALES.trs": "FISCAL",
    "DIVISION.trs": "DIVISION",
}


# =========================================================================
# Step 1: Unzip
# =========================================================================
def unzip_all(execute: bool = False):
    """Unzip raw_zipped/*.zip into raw/{year}/."""
    if not RAW_ZIPPED.is_dir():
        print(f"WARNING: {RAW_ZIPPED} not found, skipping unzip step.")
        return

    for zf_name in sorted(os.listdir(RAW_ZIPPED)):
        m = ZIP_PATTERN.match(zf_name)
        if not m:
            continue
        year = 2000 + int(m.group(1))
        zf_path = RAW_ZIPPED / zf_name
        year_dir = RAW_DIR / str(year)

        if year_dir.is_dir() and any(year_dir.iterdir()):
            print(f"  {year}: already unzipped, skipping")
            continue

        print(f"  {year}: {zf_name} -> raw/{year}/")
        if not execute:
            continue

        year_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zf_path, "r") as zf:
            zf.extractall(year_dir)

        # Flatten nested subdirectories (e.g., 2017/MCVL2017/*.TXT -> 2017/*.TXT)
        _flatten_subdirs(year_dir)

        # Unzip inner zips (e.g., 2009 has .zip files inside the main zip)
        _unzip_inner(year_dir)


def _flatten_subdirs(year_dir: Path):
    """Move files from subdirectories up to year_dir, then remove empty subdirs."""
    for sub in list(year_dir.iterdir()):
        if sub.is_dir():
            for f in sub.iterdir():
                if f.is_file():
                    dest = year_dir / f.name
                    if not dest.exists():
                        shutil.move(str(f), str(dest))
            # Remove now-empty subdir
            try:
                sub.rmdir()
            except OSError:
                pass  # not empty, leave it


def _unzip_inner(year_dir: Path):
    """Unzip any .zip files found inside a year directory, then remove the zips.

    This handles years like 2009 where the main zip contains inner zips
    (e.g., MCVL2009AFILIAD1_CDF.zip) that must be extracted a second time.
    """
    inner_zips = list(year_dir.glob("*.zip"))
    if inner_zips:
        print(f"    -> {len(inner_zips)} inner zip(s) found, extracting...")
    for zf_path in inner_zips:
        try:
            with zipfile.ZipFile(zf_path, "r") as zf:
                zf.extractall(year_dir)
            zf_path.unlink()
            print(f"       {zf_path.name} -> extracted + removed")
        except zipfile.BadZipFile:
            print(f"    WARNING: bad inner zip: {zf_path.name}")


# =========================================================================
# Step 2: Normalize filenames
# =========================================================================
def build_rename_plan() -> list[tuple[Path, Path | None]]:
    """Build (old_path, new_path) pairs. new_path=None means stale file."""
    plan = []
    for year in YEARS:
        year_dir = RAW_DIR / str(year)
        if not year_dir.is_dir():
            continue
        for fname in sorted(os.listdir(year_dir)):
            old_path = year_dir / fname
            if not old_path.is_file():
                continue
            new_name = _compute_new_name(year, fname)
            if new_name is None:
                plan.append((old_path, None))  # stale
            elif new_name != fname:
                plan.append((old_path, year_dir / new_name))
    return plan


def _compute_new_name(year: int, fname: str) -> str | None:
    """Normalized filename, or None for stale files to flag."""

    # Already normalized?
    if re.match(rf"MCVL{year}(AFILIAD|COTIZA|CONVIVIR|PERSONAL|PRESTAC|FISCAL|DIVISION)\d*_CDF\.TXT$", fname):
        return fname

    # ---- 2004: mainframe names ----
    if year == 2004:
        return MAP_2004.get(fname, fname)

    # ---- 2005: strip B prefix, add _CDF ----
    if year == 2005:
        m = re.match(r"MCVL2005B(\w+?)(\d*)\.TXT", fname, re.IGNORECASE)
        if m:
            return f"MCVL2005{m.group(1).upper()}{m.group(2)}_CDF.TXT"
        return fname

    # ---- 2006-2008: .trs files, no year in name ----
    if year in (2006, 2007, 2008):
        if fname in MAP_TRS_SPECIAL:
            return f"MCVL{year}{MAP_TRS_SPECIAL[fname]}_CDF.TXT"
        m = re.match(r"AFILANON(\d+)\.trs", fname, re.IGNORECASE)
        if m:
            return f"MCVL{year}AFILIAD{m.group(1)}_CDF.TXT"
        m = re.match(r"COTIANON(\d+)\.trs", fname, re.IGNORECASE)
        if m:
            return f"MCVL{year}COTIZA{m.group(1)}_CDF.TXT"
        return fname

    # ---- 2009: missing _CDF on COTIZA files ----
    if year == 2009:
        m = re.match(r"(MCVL2009COTIZA\d+)\.TXT", fname, re.IGNORECASE)
        if m and "_CDF" not in fname.upper():
            return m.group(1).upper() + "_CDF.TXT"
        return _normalize_ext(fname)

    # ---- 2010: AFLIAID typo + case ----
    if year == 2010:
        new = re.sub(r"AFLIAID", "AFILIAD", fname, flags=re.IGNORECASE)
        return _normalize_ext(new)

    # ---- 2011: .F2013.AFILIA -> AFILIAD ----
    if year == 2011:
        m = re.match(r"MCVL2011\.F2013\.AFILIA(\d+)_CDF\.txt", fname, re.IGNORECASE)
        if m:
            return f"MCVL2011AFILIAD{m.group(1)}_CDF.TXT"
        return _normalize_ext(fname)

    # ---- 2013: stale 2012 files ----
    if year == 2013:
        if fname.upper().startswith("MCVL2012"):
            return None  # stale
        return _normalize_ext(fname)

    # ---- 2015: CDFF -> CDF ----
    if year == 2015:
        return _normalize_ext(fname.replace("_CDFF.", "_CDF."))

    # ---- 2016: SDF -> CDF ----
    if year == 2016:
        return _normalize_ext(fname.replace("_SDF.", "_CDF."))

    # ---- 2020-2021: PERSONAL.TXT -> PERSONAL_CDF.TXT ----
    if year in (2020, 2021):
        if re.match(r"MCVL\d{4}PERSONAL\.TXT$", fname, re.IGNORECASE):
            return f"MCVL{year}PERSONAL_CDF.TXT"
        return _normalize_ext(fname)

    # ---- Generic: normalize extension case ----
    return _normalize_ext(fname)


def _normalize_ext(fname: str) -> str:
    """Ensure .TXT extension is uppercase."""
    base, ext = os.path.splitext(fname)
    if ext.lower() == ".txt" and ext != ".TXT":
        return base + ".TXT"
    return fname


# =========================================================================
# Main
# =========================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Unzip and normalize MCVL raw files: raw_zipped/ -> raw/")
    parser.add_argument("--execute", action="store_true",
                        help="Actually perform operations (default: dry run)")
    parser.add_argument("--rename-only", action="store_true",
                        help="Skip unzip, only rename existing files in raw/")
    args = parser.parse_args()

    mode = "EXECUTE" if args.execute else "DRY RUN"
    print(f"=== MCVL filename normalizer ({mode}) ===\n")

    # Step 1: Unzip
    if not args.rename_only:
        print("--- Step 1: Unzip raw_zipped/ -> raw/ ---\n")
        unzip_all(execute=args.execute)
        print()

    # Step 2: Rename
    print("--- Step 2: Normalize filenames ---\n")
    plan = build_rename_plan()

    renames = [(old, new) for old, new in plan if new is not None]
    stale = [old for old, _ in plan if _ is None]

    if not renames and not stale:
        print("  All files already normalized. Nothing to do.")
        return

    if renames:
        print(f"  {len(renames)} file(s) to rename:\n")
        for old, new in renames:
            print(f"    {old.parent.name}/{old.name}")
            print(f"      -> {new.name}")
        print()

    if stale:
        print(f"  {len(stale)} stale file(s) (wrong year in directory):\n")
        for f in stale:
            print(f"    {f.parent.name}/{f.name}  [STALE]")
        print()

    if not args.execute:
        print(f"*** DRY RUN -- pass --execute to apply changes ***")
        return

    # Execute renames
    ok = 0
    for old, new in renames:
        try:
            if new.exists():
                print(f"  SKIP (target exists): {new.parent.name}/{new.name}")
                continue
            old.rename(new)
            ok += 1
        except OSError as e:
            print(f"  ERROR: {old.name} -> {new.name}: {e}")

    print(f"\nDone. {ok}/{len(renames)} files renamed.")
    if stale:
        print(f"NOTE: {len(stale)} stale files left untouched -- review manually.")


if __name__ == "__main__":
    main()
