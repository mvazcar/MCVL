"""
Configuration for the ABDVHW replication pipeline.
Translates mcvl_data_processing Stata code (Arellano et al.) to Python/Polars.

Key design: keep ALL individuals (no filtering here), defer sample restrictions
to the analysis stage. Extends to 2024 (Stata only went to 2018).
"""
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_ROOT     = Path(__file__).resolve().parent
RAW_DIR   = _ROOT / "raw"
OUTPUT_DIR = _ROOT / "output"
TEMP_DIR   = _ROOT / "temp"

# ---------------------------------------------------------------------------
# Year ranges
# ---------------------------------------------------------------------------
YEAR_FIRST  = 2005   # First MCVL extract to process (Stata: $yrfirst)
YEAR_LATEST = 2024   # Last extract  (Stata was 2018, we extend)
FISCAL_FIRST = 2006  # FISCAL files only exist from 2006

# ---------------------------------------------------------------------------
# File naming
# ---------------------------------------------------------------------------
def raw_path(year: int, filetype: str, part: int | None = None) -> Path:
    suffix = f"{part}" if part is not None else ""
    return RAW_DIR / str(year) / f"MCVL{year}{filetype}{suffix}_CDF.TXT"

def afiliad_parts(year: int) -> list[int]:
    return [1, 2, 3] if year <= 2012 else [1, 2, 3, 4]

def cotiza_parts_regular() -> list[int]:
    return list(range(1, 13))

# ---------------------------------------------------------------------------
# CPI deflator (base 2018 = 103.664)
# ---------------------------------------------------------------------------
CPI = {
    2005:  83.694, 2006:  86.637, 2007:  89.067, 2008: 92.693,
    2009:  92.413, 2010:  93.907, 2011:  96.256, 2012: 98.614,
    2013: 100.000, 2014:  99.836, 2015:  99.348, 2016: 99.022,
    2017: 101.008, 2018: 103.664,
    2019: 104.390, 2020: 104.056, 2021: 107.271, 2022: 116.268,
    2023: 120.372, 2024: 123.790,
}
CPI_BASE = 103.664

# ---------------------------------------------------------------------------
# Education mapping  (7-level as in Stata 01_Past_info.do)
# ---------------------------------------------------------------------------
EDU_MAP_7 = {
    "10": 10, "11": 10,                    # No sabe leer ni escribir
    "20": 20, "21": 20, "22": 20,          # Inferior a graduado escolar
    "30": 30, "31": 30, "32": 30,          # Graduado escolar o equivalente
    "40": 40, "41": 40, "42": 40,          # Bachiller / FP 2
    "43": 50, "44": 50, "45": 50,          # Diplomado / Tecnico
    "46": 60, "47": 60,                    # Licenciado / Graduado
    "48": 70,                              # Master / Doctorado
}

# 4-level collapse used in 07_01
EDU_MAP_4 = {10: 1, 20: 1, 30: 2, 40: 3, 50: 4, 60: 4, 70: 4}

EDU_LABELS_7 = {
    10: "No sabe leer ni escribir",
    20: "Inferior a graduado escolar",
    30: "Graduado escolar o equivalente",
    40: "Bachiller / FP 2",
    50: "Diplomado / Tecnico",
    60: "Licenciado / Graduado",
    70: "Master / Doctorado",
}
EDU_LABELS_4 = {1: "Below primary", 2: "Primary", 3: "Secondary/Voc", 4: "Higher"}

# ---------------------------------------------------------------------------
# PERSONAL field positions  (0-indexed, stable 2005-2024)
# 2006-2014: 11 fields (extra v11 dropped); 2015-2024: 10 fields
# ---------------------------------------------------------------------------
PERSONAL_POS = {
    "person_id": 0, "birth_date": 1, "sex": 2, "nationality": 3,
    "birth_prov": 4, "ss_reg_prov": 5, "person_muni_latest": 6,
    "death_year_month": 7, "birth_country": 8, "edu_code": 9,
}

# ---------------------------------------------------------------------------
# CONVIVIR field positions  (stable 2005-2024, 21 fields)
# person_id(0), birth_date(1), sex(2), then (birth_date_N, sex_N) pairs
# ---------------------------------------------------------------------------
CONVIVIR_N_FIELDS = 21  # 1 header set + 9 conviviente pairs

# ---------------------------------------------------------------------------
# AFILIAD field positions  (varies by era)
# ---------------------------------------------------------------------------
# Common positions that are stable across 2006-2024:
AFIL_COMMON = {
    "person_id": 0, "contribution_regime": 1, "contribution_group": 2,
    "contract_type": 3, "ptcoef": 4, "entry_date": 5, "exit_date": 6,
    "reason_dismissal": 7, "disability": 8, "firm_cc2": 9, "firm_muni": 10,
    # position 11 differs: cnae93 in 2006-2009, cnae09 in 2010+
    "firm_workers": 12, "firm_age": 13, "job_relationship": 14,
    "firm_ett": 15, "firm_jur_type": 16, "firm_jur_status": 17,
    "firm_id": 18, "firm_cc": 19, "firm_main_prov": 20,
    "new_date_contract1": 21, "prev_contract1": 22, "prev_ptcoef1": 23,
    "new_date_contract2": 24, "prev_contract2": 25, "prev_ptcoef2": 26,
    "new_date_contribution_group": 27, "prev_contribution_group": 28,
}

def afiliad_era(year: int) -> str:
    if year <= 2009:   return "2006-2009"
    elif year <= 2012: return "2010-2012"
    elif year <= 2020: return "2013-2020"
    else:              return "2021-2024"

# sector_cnae positions per era
AFIL_CNAE = {
    "2006-2009": {"sector_cnae93": 11, "sector_cnae09": None},
    "2010-2012": {"sector_cnae09": 11, "sector_cnae93": 29},
    "2013-2020": {"sector_cnae09": 11, "sector_cnae93": 29},
    "2021-2024": {"sector_cnae09": 11, "sector_cnae93": 29},
}

# ---------------------------------------------------------------------------
# COTIZA field positions
# ---------------------------------------------------------------------------
COTIZA_FIELDS = {
    "2005-2012": {"person_id": 0, "firm_cc2": 1, "year": 2, "contrib_start": 8},
    "2013-2024": {"person_id": 0, "firm_cc2": 1, "year": 2, "contrib_start": 3},
}

def cotiza_era(year: int) -> str:
    return "2005-2012" if year <= 2012 else "2013-2024"

# ---------------------------------------------------------------------------
# FISCAL field positions
# ---------------------------------------------------------------------------
FISCAL_POS = {
    "2006-2015": {
        "person_id": 0, "firm_jur_status": 1, "firm_id": 2,
        "payment_type": 4, "payment_subtype": 5,
        "payment_amount": 6, "payment_inkind": 8,
        "wage_il": None, "inkind_il": None, "amount_il": None,
    },
    "2016": {
        "person_id": 0, "firm_jur_status": 1, "firm_id": 2,
        "payment_type": 4, "payment_subtype": 5,
        "payment_amount": 6, "amount_il": 7,
        "payment_inkind": 10,
        "wage_il": None, "inkind_il": None,
    },
    "2017-2024": {
        "person_id": 0, "firm_jur_status": 1, "firm_id": 2,
        "payment_type": 4, "payment_subtype": 5,
        "payment_amount": 6, "wage_il": 7,
        "payment_inkind": 10, "inkind_il": 11,
        "amount_il": None,
    },
}

def fiscal_era(year: int) -> str:
    if year <= 2015:   return "2006-2015"
    elif year == 2016: return "2016"
    else:              return "2017-2024"

# ---------------------------------------------------------------------------
# PRESTAC field positions  (key fields stable 2005-2024)
# ---------------------------------------------------------------------------
PRESTAC_POS = {
    "person_id": 0, "year": 1, "class": 3, "regimep": 9, "date1": 10,
}

# Pension classes indicating retirement
PENSION_CLASSES = {"20", "21", "22", "23", "24", "25", "26",
                   "J1", "J2", "J3", "J4", "J5"}
PARTIAL_PENSION_CLASSES = {"25", "J3"}

# ---------------------------------------------------------------------------
# Contribution regime ranges for "relevant work contracts"
# (from 06_04_Count_Days.do and 07_01)
# ---------------------------------------------------------------------------
WORK_REGIME_RANGES = [
    (111, 137), (140, 180), (611, 650),
    (811, 823), (840, 850), (911, 950),
]

# Self-employment regimes
SELF_EMP_RANGES = [(521, 540), (721, 740), (825, 831)]

# Domestic worker regimes
DOMESTIC_CODES = [138] + list(range(1200, 1251))

# Unemployment job_relationship codes
UNEMP_JR = list(range(751, 757))

# Basque Country & Navarra municipality ranges (no FISCAL data)
BASQUE_NAVARRA_MUNI = [(1000, 1999), (20000, 20999), (31000, 31999), (48000, 48999)]

# ---------------------------------------------------------------------------
# Province -> Comunidad Autonoma
# ---------------------------------------------------------------------------
PROVINCE_TO_CA = {
    4:1, 11:1, 14:1, 18:1, 21:1, 23:1, 29:1, 41:1,   # Andalucia
    22:2, 44:2, 50:2,                                   # Aragon
    33:3,                                                # Asturias
    7:4,                                                 # Baleares
    35:5, 38:5,                                          # Canarias
    39:6,                                                # Cantabria
    5:7, 9:7, 24:7, 34:7, 37:7, 40:7, 42:7, 47:7, 49:7, # CyL
    2:8, 13:8, 16:8, 19:8, 45:8,                        # CLM
    8:9, 17:9, 25:9, 43:9,                              # Cataluna
    3:10, 12:10, 46:10,                                  # Valencia
    6:11, 10:11,                                         # Extremadura
    15:12, 27:12, 32:12, 36:12,                          # Galicia
    28:13,                                               # Madrid
    30:14,                                               # Murcia
    31:15,                                               # Navarra
    1:16, 48:16, 20:16,                                  # Pais Vasco
    26:17,                                               # La Rioja
    51:18,                                               # Ceuta
    52:19,                                               # Melilla
}

# Valid firm_jur_status codes
VALID_JUR_STATUS = set("ABCDEFGHJNPQRSUVW")
