import pandas as pd
import re
import ast


def load_raw_data(path="trials_last_5_years.csv") -> pd.DataFrame:
    """Carga el CSV descargado desde ClinicalTrials.gov"""
    return pd.read_csv(path)


def clean_trials_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # --- Convierte columnas que son listas guardadas como string
    for col in ["countries", "collaborators", "conditions"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else x
            )

    # =========================
    # FECHAS
    # =========================
    for col in ["startDate", "primaryCompletionDate", "completionDate"]:
        if col in df.columns:
            df[col + "_dt"] = pd.to_datetime(df[col], errors="coerce")

    df["start_year"] = df["startDate_dt"].dt.year
    df["start_month"] = df["startDate_dt"].dt.to_period("M").astype(str)

    # =========================
    # NORMALIZACIÓN DE TEXTO
    # =========================
    def norm_text(x):
        if not isinstance(x, str):
            return x
        x = x.strip()
        x = re.sub(r"\s+", " ", x)
        return x

    text_cols = [
        "briefTitle",
        "officialTitle",
        "overallStatus",
        "studyType",
        "phase",
        "condition",
        "leadSponsor",
    ]

    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].apply(norm_text)

    # =========================
    # LIMPIEZA DE SPONSOR
    # =========================
    def clean_sponsor(s):
        if not isinstance(s, str) or not s.strip():
            return None
        s = s.lower()
        s = re.sub(r"\b(inc|inc\.|ltd|llc|plc|gmbh|sa|ag|bv)\b", "", s)
        s = re.sub(r"[^\w\s&-]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s.title()

    df["leadSponsor_clean"] = df["leadSponsor"].apply(clean_sponsor)

    # =========================
    # ÁREA TERAPÉUTICA
    # =========================
    THERAPEUTIC_AREAS = {
        "Oncology": ["cancer", "tumor", "carcinoma", "neoplasm", "lymphoma", "leukemia", "melanoma", "sarcoma"],
        "Cardiology": ["cardio", "heart", "coronary", "myocard", "hypertension", "stroke"],
        "Neurology": ["alzheimer", "parkinson", "epile", "multiple sclerosis", "migraine", "dementia"],
        "Immunology": ["rheumatoid", "lupus", "psoriasis", "crohn", "colitis", "asthma", "eczema"],
        "Infectious": ["covid", "hiv", "hepatitis", "influenza", "tuberculosis", "malaria"],
        "Endocrine/Metabolic": ["diabetes", "obesity", "thyroid", "metabolic", "hyperlipid", "cholesterol"],
        "Psychiatry": ["depression", "anxiety", "schizophrenia", "bipolar", "addiction", "opioid"],
    }

    def map_area(cond):
        if not isinstance(cond, str) or not cond.strip():
            return "Other"
        c = cond.lower()
        for area, kws in THERAPEUTIC_AREAS.items():
            if any(k in c for k in kws):
                return area
        return "Other"

    df["therapeutic_area"] = df["condition"].apply(map_area)

    # =========================
    # FLAG BIG PHARMA
    # =========================
    BIG_PHARMA_PATTERNS = [
        r"\bpfizer\b",
        r"\broche\b|\bgenentech\b",
        r"\bnovartis\b",
        r"\bastrazeneca\b",
        r"\bsanofi\b",
        r"\bgsk\b|\bglaxosmithkline\b",
        r"\bbayer\b",
        r"\bmerck\b|\bmsd\b",
        r"\babbvie\b",
        r"\bjanssen\b|\bjohnson\s*&\s*johnson\b|\bjohnson and johnson\b",
        r"\bbristol-?myers\b|\bbms\b",
        r"\beli\s*lilly\b|\blilly\b",
        r"\btakeda\b",
        r"\bamgen\b",
        r"\boehringer\b",
    ]

    big_re = re.compile("|".join(BIG_PHARMA_PATTERNS), re.IGNORECASE)

    def is_big_pharma(row):
        lead = row.get("leadSponsor") or ""
        collabs = row.get("collaborators") or []
        if not isinstance(collabs, list):
            collabs = []
        text = " ".join([lead] + [c for c in collabs if isinstance(c, str)])
        return bool(big_re.search(text))

    df["is_big_pharma"] = df.apply(is_big_pharma, axis=1)

    return df


def make_long_tables(df: pd.DataFrame):
    df_countries = (
        df[["nctId", "countries"]]
        .explode("countries")
        .dropna(subset=["countries"])
        .rename(columns={"countries": "country"})
    )

    df_collabs = (
        df[["nctId", "collaborators"]]
        .explode("collaborators")
        .dropna(subset=["collaborators"])
        .rename(columns={"collaborators": "collaborator"})
    )

    return df_countries, df_collabs


def make_long_tables(df: pd.DataFrame):
    # Tabla larga por país
    df_countries = (
        df[["nctId", "countries"]]
        .explode("countries")
        .dropna(subset=["countries"])
        .rename(columns={"countries": "country"})
    )

    # Tabla larga por colaborador
    df_collabs = (
        df[["nctId", "collaborators"]]
        .explode("collaborators")
        .dropna(subset=["collaborators"])
        .rename(columns={"collaborators": "collaborator"})
    )

    # 👇 NUEVO: tabla larga por condición (todas)
    df_conditions = (
        df[["nctId", "conditions"]]
        .explode("conditions")
        .dropna(subset=["conditions"])
        .rename(columns={"conditions": "condition"})
    )

    return df_countries, df_collabs, df_conditions



def load_gsk_pipeline(path="gsk_pipeline_scraped_20251205_185707.csv") -> pd.DataFrame:
    df = pd.read_csv(path, sep=";")

    # Normaliza nombres de columnas (por si vienen con espacios o mayúsculas)
    df.columns = [c.strip() for c in df.columns]

    # Forzamos las columnas esperadas (si alguna falta, la creamos)
    expected = ["Name", "Therapy Area", "Indication", "Phase", "Mode of Action", "Notes", "Reason", "Company"]
    for c in expected:
        if c not in df.columns:
            df[c] = None

    # Limpieza básica de texto
    def norm(x):
        if not isinstance(x, str):
            return x
        x = x.strip()
        x = re.sub(r"\s+", " ", x)
        return x

    for c in expected:
        df[c] = df[c].apply(norm)

    # Normaliza fase a un estándar simple
    def normalize_phase(p):
        if not isinstance(p, str) or not p.strip():
            return "N/A"
        s = p.lower()
        if "early" in s and "1" in s:
            return "Early Phase 1"
        if re.search(r"\bphase\s*1\b", s) or re.search(r"\b1\b", s):
            return "Phase 1"
        if re.search(r"\bphase\s*2\b", s) or re.search(r"\b2\b", s):
            return "Phase 2"
        if re.search(r"\bphase\s*3\b", s) or re.search(r"\b3\b", s):
            return "Phase 3"
        if re.search(r"\bphase\s*4\b", s) or re.search(r"\b4\b", s):
            return "Phase 4"
        return "N/A"

    df["phase_std"] = df["Phase"].apply(normalize_phase)

    # Mapeo a tus áreas terapéuticas (las mismas que usas en trials)
    def map_to_therapeutic_area(therapy_area, indication):
        text = f"{therapy_area or ''} {indication or ''}".lower()

        if any(k in text for k in ["oncolog", "cancer", "tumor", "carcinoma", "neoplasm", "lymphoma", "leukemia"]):
            return "Oncology"
        if any(k in text for k in ["cardio", "heart", "coronary", "myocard", "hypertension", "stroke"]):
            return "Cardiology"
        if any(k in text for k in ["neuro", "alzheimer", "parkinson", "epile", "multiple sclerosis", "migraine", "dementia"]):
            return "Neurology"
        if any(k in text for k in ["immun", "rheumatoid", "lupus", "psoriasis", "crohn", "colitis", "asthma", "eczema"]):
            return "Immunology"
        if any(k in text for k in ["infect", "covid", "hiv", "hepatitis", "influenza", "tuberculosis", "malaria"]):
            return "Infectious"
        if any(k in text for k in ["diabetes", "obesity", "thyroid", "metabolic", "hyperlipid", "cholesterol"]):
            return "Endocrine/Metabolic"
        if any(k in text for k in ["depression", "anxiety", "schizophrenia", "bipolar", "addiction", "opioid"]):
            return "Psychiatry"
        return "Other"

    df["therapeutic_area_std"] = df.apply(
        lambda r: map_to_therapeutic_area(r["Therapy Area"], r["Indication"]),
        axis=1
    )

    # Company limpia
    df["company_std"] = df["Company"].fillna("GSK").astype(str).str.strip()

    return df

