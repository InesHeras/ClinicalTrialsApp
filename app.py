import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px

from data_loader import load_raw_data, clean_trials_df, make_long_tables, load_gsk_pipeline


st.set_page_config(page_title="Clinical Trials Dashboard", layout="wide")

@st.cache_data(ttl=24 * 3600)
def load_clean_data():
    df_raw = load_raw_data("trials_last_5_years.csv")
    df = clean_trials_df(df_raw)
    df_countries, df_collabs, df_conditions = make_long_tables(df)

    gsk = load_gsk_pipeline("gsk_pipeline_scraped_20251214_113943.csv")
    return df, df_countries, df_collabs, df_conditions, gsk


df, df_countries, df_collabs, df_conditions, gsk = load_clean_data()


st.title("Ensayos activos (últimos 5 años)")

# -----------------
# SIDEBAR FILTROS
# -----------------
st.sidebar.header("Filtros")

min_year = int(df["start_year"].min()) if pd.notna(df["start_year"].min()) else 2020
max_year = int(df["start_year"].max()) if pd.notna(df["start_year"].max()) else 2025

year_range = st.sidebar.slider(
    "Año de inicio del ensayo",
    min_value=min_year,
    max_value=max_year,
    value=(max(min_year, max_year - 4), max_year),
)

areas = sorted(df["therapeutic_area"].dropna().unique().tolist())
area_sel = st.sidebar.multiselect("Área terapéutica", areas, default=areas)

phase_options = sorted(df["phase"].dropna().unique().tolist())
phase_sel = st.sidebar.multiselect("Fase", phase_options, default=phase_options)

only_big = st.sidebar.checkbox("Solo Big Pharma", value=False)

sponsor_options = sorted(df["leadSponsor_clean"].dropna().unique().tolist())
sponsor_sel = st.sidebar.multiselect("Lead sponsor", sponsor_options, default=[])

country_options = sorted(df_countries["country"].dropna().unique().tolist())
country_sel = st.sidebar.multiselect("País", country_options, default=[])

# -----------------
# APLICAR FILTROS
# -----------------
mask = (
    df["start_year"].between(year_range[0], year_range[1], inclusive="both")
    & df["therapeutic_area"].isin(area_sel)
    & df["phase"].isin(phase_sel)
)

df_f = df[mask].copy()

if only_big:
    df_f = df_f[df_f["is_big_pharma"] == True]

if sponsor_sel:
    df_f = df_f[df_f["leadSponsor_clean"].isin(sponsor_sel)]

if country_sel:
    ids_in_countries = df_countries[df_countries["country"].isin(country_sel)]["nctId"].unique()
    df_f = df_f[df_f["nctId"].isin(ids_in_countries)]

df_countries_f = df_countries[df_countries["nctId"].isin(df_f["nctId"])].copy()
df_conditions_f = df_conditions[df_conditions["nctId"].isin(df_f["nctId"])].copy()

# -----------------
# KPIs
# -----------------
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Ensayos", f"{len(df_f):,}")
with k2:
    pct_big = (df_f["is_big_pharma"].mean() * 100) if len(df_f) else 0
    st.metric("% Big Pharma", f"{pct_big:.1f}%")
with k3:
    st.metric("Países", f"{df_countries_f['country'].nunique():,}")
with k4:
    top_area = df_f["therapeutic_area"].value_counts().idxmax() if len(df_f) else "-"
    st.metric("Área top", top_area)

st.divider()

tab1, tab2, tab3 = st.tabs(["Panorama", "Mapa", "Enfermedades"])

# =========================
# TAB 1: PANORAMA
# =========================
with tab1:
    c1, c2 = st.columns(2)

    area_counts = (
        df_f["therapeutic_area"].value_counts()
        .rename_axis("therapeutic_area")
        .reset_index(name="n_trials")
    )

    chart_area = (
        alt.Chart(area_counts)
        .mark_bar()
        .encode(
            x=alt.X("n_trials:Q", title="Nº ensayos"),
            y=alt.Y("therapeutic_area:N", sort="-x", title=None),
            tooltip=["therapeutic_area:N", "n_trials:Q"],
        )
        .properties(height=360, title="Ensayos por área terapéutica")
    )

    with c1:
        st.altair_chart(chart_area, use_container_width=True)

    stack = (
        df_f.groupby(["therapeutic_area", "is_big_pharma"])
        .size()
        .reset_index(name="n_trials")
    )
    stack["sponsor_group"] = stack["is_big_pharma"].map({True: "Big Pharma", False: "No Big Pharma"})

    chart_stack = (
        alt.Chart(stack)
        .mark_bar()
        .encode(
            x=alt.X("n_trials:Q", title="Nº ensayos"),
            y=alt.Y("therapeutic_area:N", sort="-x", title=None),
            color=alt.Color("sponsor_group:N", title="Sponsor"),
            tooltip=["therapeutic_area:N", "sponsor_group:N", "n_trials:Q"],
        )
        .properties(height=360, title="Big Pharma vs No Big Pharma por área")
    )

    with c2:
        st.altair_chart(chart_stack, use_container_width=True)

    st.subheader("Evolución temporal (por mes de inicio)")
    ts = (
        df_f.dropna(subset=["start_month"])
        .groupby("start_month")
        .size()
        .reset_index(name="n_trials")
        .sort_values("start_month")
    )
    chart_ts = (
        alt.Chart(ts)
        .mark_line(point=True)
        .encode(
            x=alt.X("start_month:N", title="Mes", axis=alt.Axis(labelAngle=-45)),
            y=alt.Y("n_trials:Q", title="Nº ensayos"),
            tooltip=["start_month:N", "n_trials:Q"],
        )
        .properties(height=320)
    )
    st.altair_chart(chart_ts, use_container_width=True)

# =========================
# TAB 2: MAPA
# =========================
with tab2:
    st.subheader("Distribución geográfica (por país)")
    country_counts = (
        df_countries_f["country"].value_counts()
        .rename_axis("country")
        .reset_index(name="n_trials")
    )

    fig = px.choropleth(
        country_counts,
        locations="country",
        locationmode="country names",
        color="n_trials",
        hover_name="country",
        hover_data={"n_trials": True},
        title="Ensayos por país (ubicaciones)",
    )
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top 20 países")
    st.dataframe(country_counts.head(20), use_container_width=True)

# =========================
# TAB 3: ENFERMEDADES
# =========================
with tab3:
    st.subheader("Enfermedades más investigadas según el nº de ensayos activos")
    top_n = st.slider("Top N", 10, 50, 20)

    cond_counts = (
        df_conditions_f["condition"]
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .value_counts()
        .head(top_n)
        .rename_axis("condition")
        .reset_index(name="n_trials")
    )

    chart_cond = (
        alt.Chart(cond_counts)
        .mark_bar()
        .encode(
            x=alt.X("n_trials:Q", title="Nº ensayos"),
            y=alt.Y("condition:N", sort="-x", title=None),
            tooltip=["condition:N", "n_trials:Q"],
        )
        .properties(height=500, title=f"Top {top_n} condiciones")
    )
    st.altair_chart(chart_cond, use_container_width=True)

st.divider()
st.subheader("GSK: estrategia declarada vs actividad en ensayos")

colA, colB = st.columns(2)

gsk_area = (
    gsk["therapeutic_area_std"]
    .value_counts()
    .rename_axis("therapeutic_area")
    .reset_index(name="n_assets")
)

with colA:
    st.altair_chart(
        alt.Chart(gsk_area)
        .mark_bar()
        .encode(
            x="n_assets:Q",
            y=alt.Y("therapeutic_area:N", sort="-x"),
        ),
        use_container_width=True,
    )


compare_mode = st.radio(
    "Comparar ensayos contra:",
    ["Todos los ensayos", "Solo Big Pharma"],
    horizontal=True
)

df_compare = df_f.copy()
if compare_mode == "Solo Big Pharma":
    df_compare = df_compare[df_compare["is_big_pharma"] == True]

trials_area = (
    df_compare["therapeutic_area"]
    .value_counts()
    .rename_axis("therapeutic_area")
    .reset_index(name="n_trials")
)

with colB:
    st.altair_chart(
        alt.Chart(trials_area)
        .mark_bar()
        .encode(
            x="n_trials:Q",
            y=alt.Y("therapeutic_area:N", sort="-x"),
        ),
        use_container_width=True,
    )
