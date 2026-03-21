# Variable Documentation

Complete variable reference for the MCVL annual person-year panel produced by this pipeline.

## Output

**File**: `output/mcvl_annual_panel_full.parquet` (1.5 GB)

| Dimension | Value |
|-----------|-------|
| Rows | 53,965,964 person-year |
| Persons | 2,882,981 |
| Years | 1960-2024 |
| Columns | 70 |

### Coverage by era

| Period | Episodes (AFILIAD) | Contributions (COTIZA) | Fiscal wages | Unemployment/Professional |
|--------|--------------------|----------------------|--------------|--------------------------|
| 1960-1979 | Yes | No | No | No |
| 1980-2005 | Yes | Yes | No | No |
| 2006-2024 | Yes | Yes | Yes | Yes |

---

## Output variables

### Identity & tracking

| Variable | Type | Description |
|----------|------|-------------|
| `person_id` | String | Anonymised individual identifier |
| `year` | Int16 | Calendar year |
| `MCVL_entry` | Int16 | First MCVL extract year the person appears in |
| `MCVL_last` | Int16 | Last MCVL extract year (determines which extract supplies AFILIAD/COTIZA data) |

### Demographics (from PERSONAL)

| Variable | Type | Description |
|----------|------|-------------|
| `birth_year` | Int32 | Year of birth |
| `birth_month` | Int32 | Month of birth |
| `birth_date` | String | Birth date string (YYYYMM) |
| `sex` | Int8 | 1 = male, 0 = female |
| `nationality` | Int32 | Country code (falls back to `birth_country` if null) |
| `birth_country` | Int32 | Country of birth code |
| `age` | Float64 | `year - birth_year` |
| `age_int` | Int32 | Age rounded to integer |
| `entryage` | Float64 | `MCVL_entry - birth_year` |
| `death_year_month` | Int64 | YYYYMM of death (null if alive) |
| `death_year` | Int64 | Year of death |

### Education

| Variable | Type | Description |
|----------|------|-------------|
| `education` | Int64 | 4-level: 1=below primary, 2=primary, 3=secondary/vocational, 4=higher |
| `education_7cats` | Int32 | 7-level: 10=illiterate, 20=below primary, 30=primary, 40=secondary, 50=diploma, 60=degree, 70=master/PhD |

**Raw mapping** (PERSONAL field `edu_code` -> `education_7cats` -> `education`):

| edu_code | education_7cats | education (4-level) |
|----------|----------------|---------------------|
| 10, 11 | 10 | 1 (Below primary) |
| 20, 21, 22 | 20 | 1 (Below primary) |
| 30, 31, 32 | 30 | 2 (Primary) |
| 40, 41, 42 | 40 | 3 (Secondary/vocational) |
| 43, 44, 45 | 50 | 4 (Higher) |
| 46, 47 | 60 | 4 (Higher) |
| 48 | 70 | 4 (Higher) |

### Geography

| Variable | Type | Description |
|----------|------|-------------|
| `person_muni_latest` | Int32 | Municipality of residence (latest available) |
| `province` | Int64 | Province code (`firm_muni // 1000`) |
| `comunidad` | Int64 | Autonomous community (1-19, mapped from province) |

### Family (from CONVIVIR)

| Variable | Type | Description |
|----------|------|-------------|
| `famsize` | Int32 | Household size (1 + number of convivientes present) |
| `famsize_06` | Int32 | Number of convivientes aged 0-6 |
| `famsize_715` | Int32 | Number of convivientes aged 7-15 |
| `famsize_a65` | Int32 | Number of convivientes aged 65+ |

### Main job (from AFILIAD, highest annual contribution firm)

| Variable | Type | Description |
|----------|------|-------------|
| `main_firm_cc2` | String | Main firm account code |
| `main_firm_contrib` | Float64 | Annual contribution at main firm (euros) |
| `firm_id` | String | Firm CIF identifier |
| `firm_cc` | String | Full firm contribution account |
| `firm_muni` | Int64 | Firm municipality code |
| `firm_workers` | Int64 | Number of workers at firm |
| `firm_age` | Int64 | Firm age |
| `firm_jur_type` | Int64 | Firm juridical type |
| `firm_jur_status` | String | Firm juridical status (A-W) |
| `firm_main_prov` | Int64 | Firm main province |
| `firm_ett` | Int64 | Temporary employment agency indicator |
| `sector_cnae09` | Int64 | Industry code CNAE-2009 (from 2010+) |
| `sector_cnae93` | Int64 | Industry code CNAE-93 (through 2009) |

### Contract and regime

| Variable | Type | Description |
|----------|------|-------------|
| `contribution_regime` | Int64 | Social Security contribution regime |
| `contribution_group` | Int64 | Contribution group |
| `contract_type` | Int64 | Original contract type code |
| `contractb` | Int64 | Reclassified contract type (collapsed codes) |
| `permanent` | Int32 | 1=permanent, 0=temporary, null=ambiguous |
| `ptcoef` | Int64 | Part-time coefficient |
| `rel_contract` | Int8 | 1 if work regime (not unemployment registration) |

### Employment indicators

| Variable | Type | Description |
|----------|------|-------------|
| `days` | Int32 | Annual days worked (max across episodes per month, then summed) |
| `days_lag1` | Int32 | Days worked in year t-1 |
| `days_lag2` | Int32 | Days worked in year t-2 |
| `days_lag3` | Int32 | Days worked in year t-3 |
| `fullyear_lag1` | Int8 | 1 if `days_lag1 >= 360` |
| `self_emp_year` | Int8 | 1 if any self-employment episode that year |
| `emp_hogar_year` | Int8 | 1 if any domestic worker episode that year |
| `unemp_year` | Int8 | 1 if any unemployment registration that year |
| `basque_navarra_year` | Int8 | 1 if any episode in Basque Country/Navarra (no FISCAL data) |

### Income - nominal (euros)

All fiscal income variables come from **employer-reported tax withholdings (Model 190 -- Retenciones e ingresos a cuenta del IRPF)**, not the individual's tax return. This is **gross income before taxes**, not net/after-tax income. The amounts reflect what employers reported paying to each worker.

| Variable | Type | Source | Description |
|----------|------|--------|-------------|
| `total_contribution` | Float64 | COTIZA | Sum of monthly contribution bases across all firms (euros, from cents/100) |
| `fiscal_wage` | Float64 | FISCAL | Wage from fiscal records for main firm |
| `fiscal_inkind` | Float64 | FISCAL | In-kind payments from fiscal records |
| `wage` | Float64 | FISCAL | `fiscal_wage + fiscal_inkind` |
| `inc_unemp` | Float64 | FISCAL | Unemployment income (payment_type = "C") |
| `inkind_unemp` | Float64 | FISCAL | Unemployment in-kind |
| `inc_prof` | Float64 | FISCAL | Professional/self-employment income (payment_type G/H, filtered) |
| `inkind_prof` | Float64 | FISCAL | Professional in-kind |
| `tot_inc` | Float64 | Computed | `wage + inc_unemp + inc_prof` |
| `tot_inc_lag` | Float64 | Computed | `tot_inc` in year t-1 |

### Income - real (2018 constant euros)

All `real_*` variables are deflated to **2018 constant euros** using the formula:

```
cpi_factor = year_cpi / 103.664
real_value = nominal_value / cpi_factor
```

| Variable | Type | Description |
|----------|------|-------------|
| `real_wage` | Float64 | `wage / cpi_factor` |
| `real_inc_unemp` | Float64 | `inc_unemp / cpi_factor` |
| `real_inc_prof` | Float64 | `inc_prof / cpi_factor` |
| `real_tot_inc` | Float64 | `tot_inc / cpi_factor` |

### CPI and pensions

| Variable | Type | Description |
|----------|------|-------------|
| `cpi` | Float64 | CPI index value for the year (INE IPC general, base 2013=100) |
| `cpi_factor` | Float64 | `cpi / 103.664` (ratio to 2018 CPI; equals 1.0 in 2018) |
| `retirementyear` | Int32 | Year of first retirement (from PRESTAC, null if no pension) |

---

## Raw data sources

All raw files are at `raw/{year}/MCVL{year}{TYPE}_CDF.TXT`.
Format: semicolon-delimited, no headers, encoding `utf8-lossy`.

### PERSONAL (demographics)

One file per year. 10-11 fields. Stable positions across 2005-2024.

| Position | Output column | Type | Notes |
|----------|---------------|------|-------|
| 0 | person_id | String | Anonymised ID |
| 1 | birth_date | String | YYYYMM format |
| 2 | sex | Int8 | 1=male, 2=female (recoded to 0=female) |
| 3 | nationality | Int32 | Prefixed with "N", stripped; 99=null |
| 4 | birth_prov | Int32 | Prefixed with "N", stripped |
| 5 | ss_reg_prov | Int32 | SS registration province |
| 6 | person_muni_latest | Int32 | Latest municipality |
| 7 | death_year_month | Int64 | YYYYMM of death |
| 8 | birth_country | Int32 | Prefixed with "N", stripped; 99=null |
| 9 | edu_code | String | Mapped via EDU_MAP_7 |

### CONVIVIR (household members)

One file per year. 21 fields: person_id, birth_date, sex, then 9 pairs of (birth_date_N, sex_N) for convivientes 2-10.

### AFILIAD (employment episodes)

3 partitions per year (2005-2012), 4 partitions (2013-2024). ~30 fields.

| Position | Output column | Type | Notes |
|----------|---------------|------|-------|
| 0 | person_id | String | |
| 1 | contribution_regime | Int64 | |
| 2 | contribution_group | Int64 | |
| 3 | contract_type | Int64 | |
| 4 | ptcoef | Int64 | Part-time coefficient |
| 5 | entry_date | Int64 | YYYYMMDD |
| 6 | exit_date | Int64 | YYYYMMDD |
| 7 | reason_dismissal | Int64 | |
| 8 | disability | Int64 | |
| 9 | firm_cc2 | Int64 | Firm account (truncated) |
| 10 | firm_muni | Int64 | |
| 11 | sector_cnae93 or 09 | Int64 | **Era-dependent** (see below) |
| 12 | firm_workers | Int64 | |
| 13 | firm_age | Int64 | |
| 14 | job_relationship | Int64 | |
| 15 | firm_ett | Int64 | |
| 16 | firm_jur_type | Int64 | |
| 17 | firm_jur_status | String | |
| 18 | firm_id | String | CIF identifier |
| 19 | firm_cc | String | Full account |
| 20 | firm_main_prov | Int64 | |
| 29 | sector_cnae93 | Int64 | Only for 2010-2024 eras |

**CNAE eras:**
- 2006-2009: position 11 = `sector_cnae93`, no `sector_cnae09`
- 2010-2024: position 11 = `sector_cnae09`, position 29 = `sector_cnae93`

### COTIZA (contribution bases)

12 regular partitions + 1 autonomous (part 13) per year.

| Position (2005-2012) | Position (2013-2024) | Output column | Type |
|----------------------|---------------------|---------------|------|
| 0 | 0 | person_id | String |
| 1 | 1 | firm_cc2 | String |
| 2 | 2 | year | Int16 |
| 8 | 3 | contribution_1 | Int64 (cents) |
| 9 | 4 | contribution_2 | Int64 (cents) |
| ... | ... | ... | ... |
| 19 | 14 | contribution_12 | Int64 (cents) |

Autonomous contributions (part 13) use the same layout but produce `contribution_aut_1..12`.

### FISCAL (employer-reported tax withholdings)

Source: **Model 190** (*Retenciones e ingresos a cuenta del IRPF*) -- employer declarations to the Agencia Tributaria of payments made to each worker, including wage income, unemployment benefits, and professional fees. This is **gross income before taxes** (not the individual's tax return, and not the tax amount itself).

One file per year, **2006-2024 only**. Field count varies:

| Era | Fields | IL handling |
|-----|--------|-------------|
| 2006-2015 | 44 | No IL fields (wage_il=0, inkind_il=0) |
| 2016 | 46 | amount_il at pos 7 -> wage_il; inkind_il=0 |
| 2017-2024 | 48 | wage_il at pos 7, inkind_il at pos 11 |

| Position | Output column | Type | Notes |
|----------|---------------|------|-------|
| 0 | person_id | String | |
| 1 | firm_jur_status | String | |
| 2 | firm_id | String | Zero-padded (15 chars) |
| 4 | payment_type | String | A/B/C/D/E/G/H/... |
| 5 | payment_subtype | Int32 | |
| 6 | payment_amount | Int64 | Euro-cents |
| 7 | wage_il or amount_il | Int64 | Era-dependent |
| 8/10 | payment_inkind | Int64 | Euro-cents (position shifts by era) |
| 11 | inkind_il | Int64 | 2017+ only |

**Computed**: `wage = (payment_amount + wage_il + payment_inkind + inkind_il) / 100`

**Payment type classification:**
- Work income: everything NOT in {B, C, D, E, G, H, I, J, K, L, M} -> `fiscal_work`
- Unemployment: type "C" -> `fiscal_unemp`
- Professional: types "G"/"H" (excluding G+subtype{0,2} and H+subtype{3}) -> `fiscal_prof`
- Pensions: type "B" -> `fiscal_pension` (not merged to panel)

**Important**: `firm_id` in FISCAL is zero-padded string (e.g., `"000000000841610"`), while AFILIAD stores it as Int64 (`841610`). The pipeline strips leading zeros before joining.

### PRESTAC (pensions)

One file per year. Key fields at stable positions:

| Position | Output column | Type |
|----------|---------------|------|
| 0 | person_id | String |
| 1 | year | Int16 |
| 3 | class | String |
| 9 | regimep | Int32 |
| 10 | date1 | Int64 |

Filtered to retirement classes {20,21,22,23,24,26,J1,J2,J4,J5}, excluding partial {25,J3} and regimes {36,37}. Produces `retirementyear = date1 // 100` (earliest across all years).

---

## Pipeline steps

```
Step 01  PERSONAL + CONVIVIR + AFILIAD  ->  individuals_full (116M person-year rows)
         AFILIAD                         ->  firms_all (50M firm-year rows)

Step 02  COTIZA + AFILIAD per cohort     ->  merged_contrib_afil (99M episode-year rows)
         (each person assigned to MCVL_last extract; episodes expanded to year-level)

Step 03  Compute days1..12 per episode   ->  monthly_days (99M rows, wide format)

Step 04  Annual aggregation              ->  annual_from_episodes (54M person-year rows)
         (contribution sums, main job ID, days worked, contract indicators)

Step 05  Merge demographics              ->  annual_with_demographics (54M rows)
         (age, family composition, contract reclassification)

Step 06  FISCAL + PRESTAC + firm ID fix  ->  fiscal_work, fiscal_unemp, fiscal_prof, pensions
         (independent of steps 02-05)

Step 07  Final assembly                  ->  mcvl_annual_panel_full.parquet
         (merge fiscal, pensions, CPI deflation, geographic vars)
```

### Running

```bash
cd MCVL  # repository root

# Full run (2005-2024) -- requires ~50GB RAM per step, run steps separately
python run.py

# Resume from a specific step (uses saved parquets in temp/)
python run.py --resume 4

# Custom year range
python run.py --years 2010 2020
```

Due to memory constraints (128GB RAM), the full pipeline may need to be run step-by-step. Intermediate parquets are saved in `temp/` (~13GB total). Steps 04-07 can be run separately after steps 01-03 complete:

```python
# In Python, run each step individually:
python -c "
import polars as pl
df = pl.read_parquet('temp/monthly_days.parquet')
from step04_reshape import build_annual_from_wide, save_step04
result = build_annual_from_wide(df)
save_step04(result)
"
```

### CPI deflator

**Source**: INE (Instituto Nacional de Estadistica) -- IPC general (base 2013=100). This is the standard CPI deflator used in Spanish labor economics and matches the series used by Arellano et al. in the original Stata replication.

**Base year**: 2018 (CPI = 103.664). All `real_*` variables are in 2018 constant euros.

**Formula**: `cpi_factor = year_cpi / 103.664`; `real_value = nominal_value / cpi_factor`

| Year | CPI | cpi_factor | Year | CPI | cpi_factor | Year | CPI | cpi_factor |
|------|------|-----------|------|------|-----------|------|------|-----------|
| 2005 | 83.694 | 0.807 | 2012 | 98.614 | 0.951 | 2019 | 104.390 | 1.007 |
| 2006 | 86.637 | 0.836 | 2013 | 100.000 | 0.965 | 2020 | 104.056 | 1.004 |
| 2007 | 89.067 | 0.859 | 2014 | 99.836 | 0.963 | 2021 | 107.271 | 1.035 |
| 2008 | 92.693 | 0.894 | 2015 | 99.348 | 0.958 | 2022 | 116.268 | 1.122 |
| 2009 | 92.413 | 0.892 | 2016 | 99.022 | 0.955 | 2023 | 120.372 | 1.161 |
| 2010 | 93.907 | 0.906 | 2017 | 101.008 | 0.974 | 2024 | 123.790 | 1.194 |
| 2011 | 96.256 | 0.929 | **2018** | **103.664** | **1.000** | | | |

### Key design decisions

1. **All individuals kept** -- no sample restrictions at the data processing stage. Filtering is deferred to analysis scripts.
2. **One extract per person** -- each person's AFILIAD + COTIZA data comes from their `MCVL_last` extract only (no cross-extract merging).
3. **Days calculation simplified** -- uses max across episodes per person-month (instead of Stata's pairwise overlap detection). May slightly overcount for workers with overlapping part-time jobs.
4. **Annual aggregation from wide format** -- skips the monthly reshape (which would produce 354M+ rows) and computes annual summaries directly from days1..12 / contribution_1..12 columns.
5. **firm_id normalization** -- AFILIAD stores as Int64, FISCAL as zero-padded String. Leading zeros stripped before joining.

### Known limitations and caveats

1. **No fiscal data before 2006** -- FISCAL files only exist from 2006 onwards. All fiscal income variables (`fiscal_wage`, `wage`, `inc_unemp`, `inc_prof`, `tot_inc`, and their `real_*` counterparts) are null for pre-2006 observations. For pre-2006 years, only COTIZA contribution bases (`total_contribution`) are available as an income proxy.
2. **Basque Country and Navarra missing fiscal data** -- These regions have their own tax system (*Hacienda Foral*) and do not report through the national Model 190. Workers in firms located in Basque/Navarra provinces have no FISCAL data. The variable `basque_navarra_year` flags these cases (1 = any episode in Basque Country or Navarra that year).
3. **Fiscal income is employer-reported, not individual tax returns** -- The FISCAL file comes from employer declarations (Model 190), not from the individual's IRPF return. It captures gross payments made by employers but does not include deductions, allowances, or the actual tax liability.
4. **Fiscal wage matched to main job only** -- `fiscal_wage` is joined on `person_id + firm_id + year` for the main job (highest contribution firm). Fiscal income from secondary jobs is not captured in the panel.
5. **Contribution bases are top-coded** -- COTIZA contribution bases have regulatory ceilings that vary by year, month, and contribution group. High earners hit the cap, compressing the upper tail of `total_contribution`. The `bounds.dta` file (in the Stata replication) contains min/max by year x month x group (1980-2018) but has not been integrated into this pipeline.
6. **MCVL is a 4% random sample** -- The MCVL is a non-refreshing 4% sample of Social Security records. Once a person enters the sample (first extract year = `MCVL_entry`), they remain in subsequent extracts. This means the sample is representative of the stock of SS-registered individuals each year, but certain populations (very short employment spells, informal workers, civil servants not in the general SS regime) may be underrepresented.
7. **Employment history extends back to 1960** -- AFILIAD episodes can span decades. The panel includes observations as far back as 1960 for workers with long careers, but pre-1980 data has no contribution bases and pre-2006 data has no fiscal records.

---

## Intermediate parquets (temp/)

| File | Size | Rows | Description |
|------|------|------|-------------|
| `individuals_full.parquet` | 123 MB | 116M | Rectangularised person x year panel with demographics |
| `individuals_last.parquet` | 14 MB | 3M | person_id -> MCVL_last lookup |
| `firms_all.parquet` | 93 MB | 50M | Firm x year panel |
| `merged_contrib_afil.parquet` | 3.9 GB | 99M | Episodes expanded to year-level with contribution bases |
| `monthly_days.parquet` | 4.2 GB | 99M | Same + days1..12 columns |
| `annual_from_episodes.parquet` | 1.9 GB | 54M | Person-year with main job, days, contributions |
| `annual_with_demographics.parquet` | 2.4 GB | 54M | + demographics, family, contract classification |
| `step06_*.parquet` | 370 MB | various | Fiscal work/unemp/prof, pensions, firm ID corrections |

## Dependencies

- Python 3.13+
- polars >= 1.0
