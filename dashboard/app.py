"""
dashboard/app.py
=================
Streamlit dashboard for geospatial data quality monitoring.
Displays quality scores, check results, and trend analysis
for all ingested humanitarian datasets.

Run:
    streamlit run dashboard/app.py
"""

import json
import sys
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.ingest import run_ingestion
from pipeline.validators import validate_all
import yaml

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Geodata Quality Monitor",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── STYLES ────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
  .metric-card {
    background: #f8f9fa;
    border-radius: 8px;
    padding: 1rem 1.25rem;
    border: 1px solid #e9ecef;
    margin-bottom: 0.75rem;
  }
  .status-pass { color: #1a7a4a; font-weight: 600; }
  .status-warn { color: #b45309; font-weight: 600; }
  .status-fail { color: #c0392b; font-weight: 600; }
  .dimension-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
    margin: 2px;
  }
</style>
""", unsafe_allow_html=True)

# ── HELPERS ───────────────────────────────────────────────────────────────────

STATUS_COLORS = {"pass": "#1a7a4a", "warn": "#b45309", "fail": "#c0392b", "error": "#6c757d"}
STATUS_ICONS  = {"pass": "✅", "warn": "⚠️", "fail": "❌", "error": "⚙️"}
DIM_COLORS = {
    "Completeness": "#3B82F6",
    "Validity":     "#8B5CF6",
    "Consistency":  "#06B6D4",
    "Uniqueness":   "#10B981",
    "Timeliness":   "#F59E0B",
    "Accuracy":     "#EF4444",
}

@st.cache_data(ttl=300, show_spinner=False)
def load_data():
    """Loads pipeline config, runs ingestion and validation."""
    with open("config/pipeline.yaml") as f:
        config = yaml.safe_load(f)

    with st.spinner("Ingesting data from APIs..."):
        datasets = run_ingestion("config/pipeline.yaml")

    with st.spinner("Running quality checks..."):
        reports = validate_all(datasets, config, config.get("sources", []))

    return config, datasets, reports


def score_gauge(score: float, label: str) -> go.Figure:
    color = "#1a7a4a" if score >= 0.90 else ("#b45309" if score >= 0.75 else "#c0392b")
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=round(score * 100, 1),
        number={"suffix": "%", "font": {"size": 28}},
        title={"text": label, "font": {"size": 13}},
        gauge={
            "axis": {"range": [0, 100], "tickfont": {"size": 10}},
            "bar": {"color": color, "thickness": 0.25},
            "steps": [
                {"range": [0, 75],  "color": "#fee2e2"},
                {"range": [75, 90], "color": "#fef3c7"},
                {"range": [90, 100],"color": "#d1fae5"},
            ],
            "threshold": {"line": {"color": "#374151", "width": 2}, "thickness": 0.75, "value": 80}
        }
    ))
    fig.update_layout(height=200, margin=dict(t=40, b=10, l=20, r=20), paper_bgcolor="rgba(0,0,0,0)")
    return fig


def dimension_radar(dimension_scores: dict, source_id: str) -> go.Figure:
    dims = list(dimension_scores.keys())
    scores = [round(v * 100, 1) for v in dimension_scores.values()]
    if not dims:
        return None
    fig = go.Figure(go.Scatterpolar(
        r=scores + [scores[0]],
        theta=dims + [dims[0]],
        fill="toself",
        fillcolor="rgba(59,130,246,0.15)",
        line=dict(color="#3B82F6", width=2),
        name=source_id
    ))
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], tickfont=dict(size=9)),
            angularaxis=dict(tickfont=dict(size=11))
        ),
        showlegend=False,
        height=280,
        margin=dict(t=20, b=20, l=40, r=40),
        paper_bgcolor="rgba(0,0,0,0)"
    )
    return fig


# ── SIDEBAR ───────────────────────────────────────────────────────────────────

def render_sidebar(config, reports):
    st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/1/1a/24x24x_geospatial_icon_solid.svg/120px-24x24x_geospatial_icon_solid.svg.png", width=40)
    st.sidebar.title("Geodata Quality Monitor")
    st.sidebar.caption("Humanitarian GIS Pipeline — v1.0")
    st.sidebar.divider()

    all_sources = list(reports.keys())
    selected = st.sidebar.multiselect(
        "Datasets", all_sources, default=all_sources,
        format_func=lambda x: f"{STATUS_ICONS.get(reports[x].status, '?')} {x}"
    )

    st.sidebar.divider()
    st.sidebar.markdown("**Quality thresholds**")
    pass_threshold = st.sidebar.slider("Pass threshold", 0.70, 1.0, 0.90, 0.01, format="%.0f%%")
    warn_threshold = st.sidebar.slider("Warn threshold", 0.50, 0.89, 0.75, 0.01, format="%.0f%%")

    st.sidebar.divider()
    if st.sidebar.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.caption(f"Last run: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC")

    return selected


# ── MAIN DASHBOARD ────────────────────────────────────────────────────────────

def render_overview(reports, selected):
    st.subheader("Pipeline overview")

    selected_reports = {k: v for k, v in reports.items() if k in selected}
    n_total = len(selected_reports)
    n_pass = sum(1 for r in selected_reports.values() if r.status == "pass")
    n_warn = sum(1 for r in selected_reports.values() if r.status == "warn")
    n_fail = sum(1 for r in selected_reports.values() if r.status == "fail")
    avg_score = sum(r.overall_score for r in selected_reports.values()) / n_total if n_total else 0
    total_features = sum(r.feature_count for r in selected_reports.values())

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Datasets monitored", n_total)
    c2.metric("Passing", n_pass, delta=None)
    c3.metric("Warnings", n_warn)
    c4.metric("Failing", n_fail)
    c5.metric("Avg quality score", f"{avg_score*100:.1f}%")

    st.divider()

    # Score bar chart
    data = []
    for sid, r in selected_reports.items():
        data.append({
            "Dataset": sid,
            "Score": round(r.overall_score * 100, 1),
            "Status": r.status,
            "Features": r.feature_count,
        })
    df = pd.DataFrame(data).sort_values("Score", ascending=True)

    color_map = {"pass": "#1a7a4a", "warn": "#b45309", "fail": "#c0392b"}
    fig = px.bar(
        df, x="Score", y="Dataset", orientation="h",
        color="Status", color_discrete_map=color_map,
        text="Score", range_x=[0, 105],
        labels={"Score": "Quality score (%)"},
        height=max(200, len(df) * 55 + 60)
    )
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(
        margin=dict(t=10, b=10, l=10, r=40),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02)
    )
    fig.add_vline(x=90, line_dash="dash", line_color="#1a7a4a", opacity=0.5, annotation_text="Pass (90%)")
    fig.add_vline(x=75, line_dash="dash", line_color="#b45309", opacity=0.5, annotation_text="Warn (75%)")
    st.plotly_chart(fig, use_container_width=True)


def render_dataset_detail(report):
    col1, col2, col3 = st.columns([1.5, 2, 3])

    with col1:
        st.plotly_chart(score_gauge(report.overall_score, "Overall quality"), use_container_width=True)
        st.markdown(f"""
        **Features:** {report.feature_count:,}
        **Geometry:** {report.geometry_type}
        **CRS:** {report.crs}
        **Domain:** {report.domain}
        """)

    with col2:
        radar = dimension_radar(report.dimension_scores, report.source_id)
        if radar:
            st.plotly_chart(radar, use_container_width=True)

    with col3:
        st.markdown("**Check results**")
        check_data = []
        for c in report.checks:
            check_data.append({
                "Status": STATUS_ICONS.get(c.status, "?"),
                "Check": c.check_name,
                "Dimension": c.dimension,
                "Score": f"{c.score*100:.0f}%",
                "Issues": c.affected_count,
                "Severity": c.severity.upper(),
            })
        if check_data:
            df_checks = pd.DataFrame(check_data)
            st.dataframe(
                df_checks,
                hide_index=True,
                use_container_width=True,
                height=min(400, len(check_data) * 38 + 40),
                column_config={
                    "Status": st.column_config.TextColumn(width="small"),
                    "Score": st.column_config.TextColumn(width="small"),
                    "Issues": st.column_config.NumberColumn(width="small"),
                }
            )


def render_checks_breakdown(reports, selected):
    st.subheader("Quality checks breakdown")

    all_checks = []
    for sid in selected:
        r = reports[sid]
        for c in r.checks:
            all_checks.append({
                "Dataset": sid,
                "Dimension": c.dimension,
                "Check": c.check_name,
                "Status": c.status,
                "Score": round(c.score * 100, 1),
                "Issues": c.affected_count,
                "Total": c.total_count,
                "Severity": c.severity,
            })

    df = pd.DataFrame(all_checks)

    # Dimension heatmap
    if len(df) > 0:
        pivot = df.pivot_table(values="Score", index="Dataset", columns="Dimension", aggfunc="mean")
        fig = px.imshow(
            pivot.round(1),
            color_continuous_scale=[[0, "#fee2e2"], [0.75, "#fef3c7"], [1, "#d1fae5"]],
            range_color=[0, 100],
            text_auto=".0f",
            aspect="auto",
            labels=dict(color="Score (%)"),
        )
        fig.update_layout(
            height=max(200, len(pivot) * 50 + 80),
            margin=dict(t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
            coloraxis_showscale=False,
        )
        fig.update_traces(textfont_size=12)
        st.plotly_chart(fig, use_container_width=True)

        # Failing checks
        failing = df[df["Status"].isin(["fail", "warn"])].sort_values(["Severity", "Score"])
        if len(failing) > 0:
            st.markdown(f"**{len(failing)} checks need attention**")
            st.dataframe(failing[["Dataset","Dimension","Check","Status","Score","Issues","Severity"]],
                         hide_index=True, use_container_width=True)
        else:
            st.success("All checks passing across selected datasets.")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    st.title("🗺️ Humanitarian Geodata Quality Monitor")
    st.caption("Automated quality monitoring — DAMA-DMBOK v2 | ISO 19115")

    try:
        config, datasets, reports = load_data()
    except Exception as e:
        st.error(f"Pipeline error: {e}")
        st.info("Make sure `config/pipeline.yaml` exists and dependencies are installed.")
        return

    selected = render_sidebar(config, reports)

    if not selected:
        st.warning("No datasets selected. Use the sidebar to select datasets.")
        return

    tab1, tab2, tab3 = st.tabs(["📊 Overview", "🔍 Dataset detail", "📋 Checks breakdown"])

    with tab1:
        render_overview(reports, selected)

    with tab2:
        source_choice = st.selectbox("Select dataset", selected)
        if source_choice:
            render_dataset_detail(reports[source_choice])

    with tab3:
        render_checks_breakdown(reports, selected)

    # Footer
    st.divider()
    st.caption(
        "github.com/sanyamsin/geodata-quality-pipeline  |  "
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC"
    )


if __name__ == "__main__":
    main()
