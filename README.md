# MCVL Data Processing Pipeline (Python/Polars)

Python translation of the Stata data-processing code from **Arellano, Bonhomme, De Vera, Hospido & Wei (2022),** *Income Risk Inequality: Evidence from Spanish Administrative Records.* DOI: https://doi.org/10.3982/QE1887

Two main updates:

- Extended processing from 2005–2018 to 2005–2024, covering the latest MCVL release.
- Replaced the original Stata pipeline with Python, using Polars and .parquet files. This results in substantial speed gains.

There is also a minor-but-handy addition: the script normalize_filenames.py cleans up the raw files as received:

- Fixes double-compressed archives.
- Corrects typos in filenames.
- Accounts for a naming change introduced in the 2004 files.
- Renames everything to a standard convention common to all years.

## What is the MCVL?

The **Muestra Continua de Vidas Laborales** (Continuous Sample of Working Lives) is a 4% random sample of Spanish Social Security records, published annually by the Ministerio de Inclusion, Seguridad Social y Migraciones. It links employment histories (AFILIAD), contribution bases (COTIZA), employer-reported tax withholdings (FISCAL), pension records (PRESTAC), demographics (PERSONAL), and household composition (CONVIVIR) at the individual level.

Researchers can apply for access at: https://www.seg-social.es/wps/portal/wss/internet/EstadisticasPresupuestosEstudios/Estadisticas/EST211

## Repository structure

```
MCVL/
  config.py               # CPI, education maps, province-to-comunidad, year ranges
  readers.py              # Raw file readers with era-specific field positions
  step01_panels.py        # Build individual + firm panels from PERSONAL/CONVIVIR/AFILIAD
  step02_merge.py         # Merge COTIZA contributions with AFILIAD episodes per cohort
  step03_days.py          # Compute monthly days worked (days1..days12)
  step04_reshape.py       # Annual aggregation: person-firm-year -> person-year
  step05_other_vars.py    # Add demographics, family composition, contract classification
  step06_fiscal.py        # Read FISCAL + PRESTAC, build fiscal income + pensions
  step07_final.py         # Final assembly: merge fiscal, CPI deflation, geography
  pipeline.py             # Orchestrator with memory-aware resume logic
  run.py                  # CLI entry point (--resume N, --years FIRST LAST)
  normalize_filenames.py  # Unzip + rename raw files to standard convention
  VARIABLES.md            # Complete variable documentation (70 columns)
  output/                 # Final panel: mcvl_annual_panel_full.parquet
  temp/                   # Intermediate parquets (~13 GB) for step-by-step runs
  raw_zipped/             # Original zip files from DGOSS (1145{YY}{S|T}.zip, ~26 GB)
  raw/                    # Unzipped + normalized TXT files, one folder per year
    2004/ ... 2024/       #   MCVL{YEAR}{TYPE}{N}_CDF.TXT
```

## Quick start

### 1. Obtain the data

Place the original MCVL zip files in `raw_zipped/`. Files are named `1145{YY}S.zip` (2004-2005) or `1145{YY}T.zip` (2006-2024).

### 2. Unzip and normalize filenames

The original zips produce wildly inconsistent filenames across years (mainframe-style names, `.trs` extensions, typos, nested zips, etc.). The normalization script handles all 12 year-specific quirks:

```bash
python normalize_filenames.py              # dry run -- shows what would happen
python normalize_filenames.py --execute    # unzip + rename to MCVL{YEAR}{TYPE}{N}_CDF.TXT
```

### 3. Run the pipeline

```bash
# Full run (2005-2024)
python run.py

# Resume from a specific step (uses saved parquets in temp/)
python run.py --resume 4

# Custom year range
python run.py --years 2010 2020
```

**Memory note**: The full 2005-2024 run requires ~50 GB RAM per step. On a 128 GB machine, steps may need to run separately. The pipeline saves intermediate parquets to `temp/` after each step, and `--resume N` skips completed steps.

### 4. Output

Final panel: `output/mcvl_annual_panel_full.parquet`

| Dimension | Value |
|-----------|-------|
| Rows | 53,965,964 person-year |
| Persons | 2,882,981 unique |
| Years | 1960-2024 |
| Columns | 70 |
| Size | 1.5 GB |

See [`VARIABLES.md`](VARIABLES.md) for complete variable documentation.

## Dependencies

- Python 3.13+
- [Polars](https://pola.rs/) >= 1.0

```bash
pip install polars
```

## Raw file naming quirks (why normalize_filenames.py exists)

| Year | Original naming | Problem |
|------|----------------|---------|
| 2004 | `EST.LABT2004.AFILANON.FICHERO1.TXT` | Mainframe-style; some filenames corrupted (doubled) |
| 2005 | `MCVL2005BAFILIAD1.TXT` | Extra `B` prefix, no `_CDF` suffix |
| 2006-08 | `AFILANON1.trs` | No year in name, `.trs` extension, different type names |
| 2009 | `MCVL2009COTIZA1.zip` | Inner zips inside the main zip; COTIZA files missing `_CDF` |
| 2010 | `MCVL2010AFLIAID1_CDF.TXT` | Typo: `AFLIAID` instead of `AFILIAD`; mixed case extensions |
| 2011 | `MCVL2011.F2013.AFILIA1_CDF.txt` | `.F2013.` infix; `AFILIA` instead of `AFILIAD` |
| 2013 | `MCVL2012FISCAL_CDF.TXT` in 2013 folder | Stale files from wrong year |
| 2015 | `MCVL2015COTIZA11_CDFF.TXT` | Double-F typo in suffix |
| 2016 | `MCVL2016PERSONAL_SDF.TXT` | `SDF` instead of `CDF` |
| 2017 | `MCVL2017/MCVL2017/*.TXT` | Nested subdirectory inside zip |
| 2020-21 | `MCVL2020PERSONAL.TXT` | Missing `_CDF` suffix |

## Performance (reference machine)

| Component | Spec |
|-----------|------|
| CPU | AMD Ryzen 9 9900X 12-Core (24 threads) |
| RAM | 128 GB DDR5 |
| Storage | Samsung 990 PRO 2TB NVMe SSD |
| OS | Windows 11 Pro |
| Python | 3.13 |
| Polars | 1.38.1 |

| Pipeline step | Time | Peak RAM | Output |
|---------------|------|----------|--------|
| Step 01a: individuals panel | ~8 min | ~45 GB | 116M rows |
| Step 01b: firms panel | ~5 min | ~30 GB | 50M rows |
| Step 02: merge COTIZA + AFILIAD | ~15 min | ~50 GB | 99M rows |
| Step 03: compute monthly days | ~5 min | ~45 GB | 99M rows |
| Step 04: annual aggregation | ~8 min | ~50 GB | 54M rows |
| Step 05: add demographics | ~3 min | ~25 GB | 54M rows |
| Step 06: fiscal + pensions | ~5 min | ~15 GB | various |
| Step 07: final assembly | ~3 min | ~20 GB | 54M rows |
| **Total** | **~50-60 min** | **~50 GB peak** | **1.5 GB parquet** |

Steps were run as separate Python invocations to stay within 128 GB RAM. Running all steps in one process may require more memory due to Python/Polars not fully releasing large allocations.

## Correspondence: Stata -> Python

| Stata file | Python file | Description |
|------------|-------------|-------------|
| `01_Past_Info.do` | `step01_panels.py` | Build individual + firm panels |
| `02_MergeMCVL_*.do` | `step02_merge.py` | Merge COTIZA with AFILIAD episodes |
| `03_MonthlyVars.do` | `step03_days.py` | Monthly days worked |
| `04_ReshapeData.do` | `step04_reshape.py` | Wide-to-long + annual aggregation |
| `05_OtherVars.do` | `step05_other_vars.py` | Demographics, family, contracts |
| `06_*.do` (FixIDs, Fiscales, Pensions) | `step06_fiscal.py` | Fiscal income, pensions, firm ID fixes |
| `07_*_Prep_Data_*.do` | `step07_final.py` | Final assembly + CPI deflation |
| `mcvl_reading_*.do` (14 files) | `readers.py` | Raw file readers (single file, all eras) |
| `00_Main.do` | `pipeline.py` + `run.py` | Orchestration + CLI |

Key differences from the original Stata code:
- **Years**: Extended from 2005-2018 to 2005-2024
- **Days calculation**: Simplified (max per person-month vs. pairwise overlap detection)
- **No sample restrictions**: All individuals kept; filtering deferred to analysis
- **Memory management**: Explicit cleanup + step-by-step resume for large datasets

## Built with Claude Code

This Python translation was developed using [Claude Code](https://docs.anthropic.com/en/docs/claude-code), Anthropic's AI coding agent. Claude Code translated each Stata .do file into Python/Polars, extended the coverage from 2005–2018 to 2005–2024, and debugged memory issues for the full 21-year run. It also documented all raw file format variations across two decades of MCVL extracts and wrote the accompanying documentation.