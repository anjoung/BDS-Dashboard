"""
Streamlit Dashboard for Census Business Dynamics Statistics.

Displays interactive visualizations of:
- Startup trends over time
- Firm births and deaths
- Job creation and destruction
- State-level comparisons

Styled after The Economist and academic research visualizations.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import sqlite3

# Page configuration
st.set_page_config(
    page_title="U.S. Business Dynamics Dashboard",
    page_icon=None,
    layout="wide",
)

# Custom CSS for Economist-style aesthetics
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Source+Sans+Pro:wght@400;600;700&family=Source+Serif+Pro:wght@400;600&display=swap');

    /* Main typography */
    html, body, [class*="css"] {
        font-family: 'Source Sans Pro', sans-serif;
    }

    h1, h2, h3 {
        font-family: 'Source Serif Pro', Georgia, serif;
        font-weight: 600;
        color: #1a1a1a;
    }

    h1 {
        font-size: 2.2rem;
        border-bottom: 3px solid #e3120b;
        padding-bottom: 0.5rem;
        margin-bottom: 1rem;
    }

    h2 {
        font-size: 1.4rem;
        color: #333;
        margin-top: 1.5rem;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #f7f7f7;
    }

    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        font-family: 'Source Sans Pro', sans-serif;
        font-size: 1rem;
        font-weight: 600;
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background-color: #f7f7f7;
        padding: 1rem;
        border-left: 3px solid #e3120b;
    }

    [data-testid="stMetricLabel"] {
        font-size: 0.85rem;
        color: #666;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    [data-testid="stMetricValue"] {
        font-family: 'Source Sans Pro', sans-serif;
        font-weight: 700;
        color: #1a1a1a;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        border-bottom: 1px solid #ddd;
    }

    .stTabs [data-baseweb="tab"] {
        font-family: 'Source Sans Pro', sans-serif;
        font-weight: 600;
        color: #666;
        border-bottom: 2px solid transparent;
        padding: 0.75rem 1.5rem;
    }

    .stTabs [aria-selected="true"] {
        color: #1a1a1a;
        border-bottom: 2px solid #e3120b;
    }

    /* Divider/horizontal rule styling */
    hr {
        border-color: #ddd !important;
    }

    /* Slider styling - light red */
    .stSlider [data-baseweb="slider"] [data-testid="stTickBarMin"],
    .stSlider [data-baseweb="slider"] [data-testid="stTickBarMax"] {
        color: #e57373;
    }

    /* Slider thumb and track */
    .stSlider [data-baseweb="slider"] div[role="slider"] {
        background-color: #e57373 !important;
    }

    .stSlider [data-baseweb="slider"] div[data-testid="stTickBar"] > div {
        background-color: #e57373 !important;
    }

    /* Slider track fill */
    .stSlider div[data-baseweb="slider"] > div > div > div {
        background-color: #e57373 !important;
    }

    /* Definition box */
    .definition-box {
        background-color: #f7f7f7;
        border-left: 3px solid #4a7c94;
        padding: 1rem 1.25rem;
        margin: 1rem 0;
        font-size: 0.9rem;
        color: #333;
    }

    .definition-box strong {
        color: #1a1a1a;
    }

    /* Footer */
    .footer {
        font-size: 0.8rem;
        color: #666;
        border-top: 1px solid #ddd;
        padding-top: 1rem;
        margin-top: 2rem;
    }

    .footer a {
        color: #4a7c94;
    }

    /* Mobile-friendly styles */
    @media (max-width: 768px) {
        h1 {
            font-size: 1.5rem;
            padding-bottom: 0.4rem;
        }

        h2 {
            font-size: 1.1rem;
            margin-top: 1rem;
        }

        /* Smaller tab text on mobile */
        .stTabs [data-baseweb="tab"] {
            padding: 0.5rem 0.75rem;
            font-size: 0.85rem;
        }

        /* Metric cards */
        [data-testid="stMetric"] {
            padding: 0.75rem;
        }

        [data-testid="stMetricLabel"] {
            font-size: 0.75rem;
        }

        /* Definition box */
        .definition-box {
            padding: 0.75rem 1rem;
            font-size: 0.85rem;
        }

        /* Footer */
        .footer {
            font-size: 0.75rem;
        }
    }

    /* Extra small screens */
    @media (max-width: 480px) {
        h1 {
            font-size: 1.3rem;
        }

        .stTabs [data-baseweb="tab"] {
            padding: 0.4rem 0.5rem;
            font-size: 0.75rem;
        }
    }
</style>
""", unsafe_allow_html=True)

# Chart color palette (Economist-inspired)
COLORS = {
    "primary": "#e3120b",      # Economist red
    "blue_dark": "#2c5270",    # Dark blue
    "blue_light": "#6a9ab8",   # Light blue
    "green": "#6b8e23",        # Muted green
    "maize": "#d4a017",        # Maize/gold
    "negative": "#c23b22",     # Muted red for negative values
    "neutral": "#666666",      # Gray
    "muted_blue": "#7a9bb5",   # Muted grayish blue for UI elements
    "light": "#cccccc",        # Light gray
    "background": "#ffffff",
}

# Plotly chart template
CHART_TEMPLATE = {
    "layout": {
        "font": {"family": "Source Sans Pro, sans-serif", "color": "#333"},
        "title": {"font": {"family": "Source Serif Pro, Georgia, serif", "size": 18, "color": "#1a1a1a"}, "x": 0, "xanchor": "left"},
        "paper_bgcolor": "white",
        "plot_bgcolor": "white",
        "xaxis": {
            "showgrid": False,
            "linecolor": "#333",
            "tickfont": {"size": 11},
            "title": {"font": {"size": 12}},
        },
        "yaxis": {
            "showgrid": True,
            "gridcolor": "#e5e5e5",
            "gridwidth": 1,
            "linecolor": "#333",
            "tickfont": {"size": 11},
            "title": {"font": {"size": 12}},
        },
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "left",
            "x": 0,
            "font": {"size": 11},
        },
        "margin": {"l": 60, "r": 20, "t": 80, "b": 60},
        "hovermode": "x unified",
    }
}


def apply_chart_style(fig, title=None, subtitle=None):
    """Apply consistent Economist-style formatting to a Plotly figure."""
    fig.update_layout(
        font=dict(family="Source Sans Pro, sans-serif", color="#1a1a1a", size=12),
        paper_bgcolor="white",
        plot_bgcolor="white",
        margin=dict(l=40, r=15, t=80, b=40),  # Smaller margins for mobile
        hovermode="x unified",
        autosize=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10, color="#1a1a1a"),
        ),
    )

    fig.update_xaxes(
        showgrid=False,
        linecolor="#1a1a1a",
        tickfont=dict(size=10, color="#1a1a1a"),
        title_font=dict(color="#1a1a1a"),
    )

    fig.update_yaxes(
        showgrid=True,
        gridcolor="#e5e5e5",
        gridwidth=1,
        linecolor="#1a1a1a",
        tickfont=dict(size=10, color="#1a1a1a"),
        title_font=dict(color="#1a1a1a"),
        zeroline=True,
        zerolinecolor="#1a1a1a",
        zerolinewidth=1,
    )

    if title:
        title_text = f"<b>{title}</b>"
        if subtitle:
            title_text += f"<br><span style='font-size:11px;color:#444;font-weight:normal'>{subtitle}</span>"
        fig.update_layout(
            title=dict(
                text=title_text,
                font=dict(family="Source Serif Pro, Georgia, serif", size=15, color="#1a1a1a"),
                x=0,
                xanchor="left",
            )
        )

    return fig


# Database path
DATA_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DATA_DIR / "bds.db"


@st.cache_data
def load_data():
    """Load data from SQLite database."""
    conn = sqlite3.connect(DB_PATH)

    national = pd.read_sql("SELECT * FROM national ORDER BY YEAR", conn)
    by_firm_age = pd.read_sql("SELECT * FROM by_firm_age ORDER BY YEAR, FAGE", conn)
    by_state = pd.read_sql("SELECT * FROM by_state ORDER BY YEAR, state", conn)

    conn.close()

    return national, by_firm_age, by_state


def main():
    # Title
    st.title("U.S. Business Dynamics")

    # Load data
    try:
        national, by_firm_age, by_state = load_data()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Please run the ETL pipeline first: `python run_pipeline.py`")
        return

    # Get year range for filters
    min_year = int(national["YEAR"].min())
    max_year = int(national["YEAR"].max())

    # Sidebar filters
    st.sidebar.markdown("### Filters")
    year_range = st.sidebar.slider(
        "Year Range",
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year),
    )

    # Filter data by year
    national_filtered = national[
        (national["YEAR"] >= year_range[0]) & (national["YEAR"] <= year_range[1])
    ]
    by_firm_age_filtered = by_firm_age[
        (by_firm_age["YEAR"] >= year_range[0]) & (by_firm_age["YEAR"] <= year_range[1])
    ]
    by_state_filtered = by_state[
        (by_state["YEAR"] >= year_range[0]) & (by_state["YEAR"] <= year_range[1])
    ]

    # Tab layout for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "National Trends",
        "Firm Dynamics",
        "Job Dynamics",
        "State Comparison"
    ])

    with tab1:

        # Firm births vs deaths
        fig_births_deaths = go.Figure()
        fig_births_deaths.add_trace(go.Scatter(
            x=national_filtered["YEAR"],
            y=national_filtered["ESTABS_ENTRY"],
            name="Entries (births)",
            line=dict(color=COLORS["blue_dark"], width=2),
            mode="lines",
            hovertemplate="%{y:,.0f}<extra></extra>",
        ))
        fig_births_deaths.add_trace(go.Scatter(
            x=national_filtered["YEAR"],
            y=national_filtered["ESTABS_EXIT"],
            name="Exits (deaths)",
            line=dict(color=COLORS["blue_light"], width=2),
            mode="lines",
            hovertemplate="%{y:,.0f}<extra></extra>",
        ))
        apply_chart_style(
            fig_births_deaths,
            title="Establishment Entries and Exits",
        )
        fig_births_deaths.update_yaxes(title_text="Establishments")
        st.plotly_chart(fig_births_deaths, use_container_width=True, config={'displayModeBar': False})

        # Total firms and employment over time
        col1, col2 = st.columns(2)

        with col1:
            fig_firms = go.Figure()
            fig_firms.add_trace(go.Scatter(
                x=national_filtered["YEAR"],
                y=national_filtered["FIRM"],
                line=dict(color=COLORS["blue_dark"], width=2),
                mode="lines",
                showlegend=False,
                hovertemplate="%{y:,.0f}<extra></extra>",
            ))
            apply_chart_style(fig_firms, title="Total Firms")
            fig_firms.update_yaxes(title_text="Number of firms")
            st.plotly_chart(fig_firms, use_container_width=True, config={'displayModeBar': False})

        with col2:
            fig_emp = go.Figure()
            fig_emp.add_trace(go.Scatter(
                x=national_filtered["YEAR"],
                y=national_filtered["EMP"] / 1e6,
                line=dict(color=COLORS["blue_light"], width=2),
                mode="lines",
                showlegend=False,
                hovertemplate="%{y:,.0f}M<extra></extra>",
            ))
            apply_chart_style(fig_emp, title="Total Employment")
            fig_emp.update_yaxes(title_text="Millions of employees")
            st.plotly_chart(fig_emp, use_container_width=True, config={'displayModeBar': False})

    with tab2:

        # Filter to just startups (age 0)
        startups = by_firm_age_filtered[by_firm_age_filtered["FAGE"] == "010"]

        # Firm birth and death rates over time
        if "FIRM_BIRTH_RATE" in national_filtered.columns and "FIRM_DEATH_RATE" in national_filtered.columns:
            fig_firm_rates = go.Figure()
            fig_firm_rates.add_trace(go.Scatter(
                x=national_filtered["YEAR"],
                y=national_filtered["FIRM_BIRTH_RATE"],
                name="Firm birth rate",
                line=dict(color=COLORS["blue_dark"], width=2),
                mode="lines",
                hovertemplate="%{y:.1f}%<extra></extra>",
            ))
            fig_firm_rates.add_trace(go.Scatter(
                x=national_filtered["YEAR"],
                y=national_filtered["FIRM_DEATH_RATE"],
                name="Firm death rate",
                line=dict(color=COLORS["blue_light"], width=2),
                mode="lines",
                hovertemplate="%{y:.1f}%<extra></extra>",
            ))
            apply_chart_style(
                fig_firm_rates,
                title="Firm Birth and Death Rates",
                subtitle="New firms and firm exits as a percentage of total firms"
            )
            fig_firm_rates.update_yaxes(title_text="Percent", ticksuffix="%")
            st.plotly_chart(fig_firm_rates, use_container_width=True, config={'displayModeBar': False})

        # Employment by firm age for latest year
        latest_year = by_firm_age_filtered["YEAR"].max()
        age_latest = by_firm_age_filtered[
            (by_firm_age_filtered["YEAR"] == latest_year) &
            (by_firm_age_filtered["FAGE"] != "001")  # Exclude "All Firms" aggregate
        ].copy()

        if "FIRM_AGE_LABEL" in age_latest.columns:
            
            # Order the categories properly
            age_order = ["0 (Startups)", "1-5 years", "6-10 years", "11+ years"]
            age_latest = age_latest[age_latest["FIRM_AGE_LABEL"].isin(age_order)]
            age_latest["FIRM_AGE_LABEL"] = pd.Categorical(
                age_latest["FIRM_AGE_LABEL"],
                categories=age_order,
                ordered=True
            )
            age_latest = age_latest.sort_values("FIRM_AGE_LABEL")

            fig_age_dist = go.Figure()
            fig_age_dist.add_trace(go.Bar(
                x=age_latest["FIRM_AGE_LABEL"],
                y=age_latest["EMP"] / 1e6,
                marker_color=COLORS["blue_dark"],
                showlegend=False,
                hovertemplate="%{y:.1f}M<extra></extra>",
            ))
            apply_chart_style(
                fig_age_dist,
                title=f"Employment by Firm Age ({int(latest_year)})",
                subtitle="Distribution of employment across firms of different ages"
            )
            fig_age_dist.update_yaxes(title_text="Millions of employees")
            fig_age_dist.update_xaxes(tickangle=45)
            st.plotly_chart(fig_age_dist, use_container_width=True, config={'displayModeBar': False})

    with tab3:

        # Job creation vs destruction
        fig_jobs = go.Figure()
        fig_jobs.add_trace(go.Scatter(
            x=national_filtered["YEAR"],
            y=national_filtered["JOB_CREATION"] / 1e6,
            name="Job creation",
            line=dict(color=COLORS["blue_dark"], width=2),
            mode="lines",
            hovertemplate="%{y:.1f}M<extra></extra>",
        ))
        fig_jobs.add_trace(go.Scatter(
            x=national_filtered["YEAR"],
            y=national_filtered["JOB_DESTRUCTION"] / 1e6,
            name="Job destruction",
            line=dict(color=COLORS["blue_light"], width=2),
            mode="lines",
            hovertemplate="%{y:.1f}M<extra></extra>",
        ))
        apply_chart_style(
            fig_jobs,
            title="Gross Job Flows",
            subtitle="Annual job creation and destruction"
        )
        fig_jobs.update_yaxes(title_text="Millions of jobs")
        st.plotly_chart(fig_jobs, use_container_width=True, config={'displayModeBar': False})

        # Net job creation
        colors = [COLORS["green"] if x >= 0 else COLORS["negative"]
                  for x in national_filtered["NET_JOB_CREATION"]]

        fig_net = go.Figure()
        fig_net.add_trace(go.Bar(
            x=national_filtered["YEAR"],
            y=national_filtered["NET_JOB_CREATION"] / 1e6,
            marker_color=colors,
            showlegend=False,
            hovertemplate="%{y:.1f}M<extra></extra>",
        ))
        apply_chart_style(
            fig_net,
            title="Net Job Creation",
            subtitle="Job creation minus job destruction"
        )
        fig_net.update_yaxes(title_text="Millions of jobs")
        st.plotly_chart(fig_net, use_container_width=True, config={'displayModeBar': False})

    with tab4:

        # Select year for state comparison
        selected_year = st.selectbox(
            "Select year",
            options=sorted(by_state_filtered["YEAR"].unique(), reverse=True),
        )

        state_year = by_state_filtered[by_state_filtered["YEAR"] == selected_year]

        # Top states by employment
        col1, col2 = st.columns(2)

        with col1:
            top_emp = state_year.nlargest(10, "EMP").sort_values("EMP", ascending=True)
            fig_top_emp = go.Figure()
            fig_top_emp.add_trace(go.Bar(
                y=top_emp["STATE_NAME"],
                x=top_emp["EMP"] / 1e6,
                orientation="h",
                marker_color=COLORS["blue_dark"],
                showlegend=False,
                hovertemplate="%{x:.1f}M<extra></extra>",
            ))
            apply_chart_style(
                fig_top_emp,
                title=f"Top 10 States by Employment ({int(selected_year)})",
            )
            fig_top_emp.update_xaxes(title_text="Millions of employees")
            fig_top_emp.update_layout(height=400)
            st.plotly_chart(fig_top_emp, use_container_width=True, config={'displayModeBar': False})

        with col2:
            top_firms = state_year.nlargest(10, "FIRM").sort_values("FIRM", ascending=True)
            fig_top_firms = go.Figure()
            fig_top_firms.add_trace(go.Bar(
                y=top_firms["STATE_NAME"],
                x=top_firms["FIRM"] / 1000,
                orientation="h",
                marker_color=COLORS["blue_light"],
                showlegend=False,
                hovertemplate="%{x:.1f}K<extra></extra>",
            ))
            apply_chart_style(
                fig_top_firms,
                title=f"Top 10 States by Number of Firms ({int(selected_year)})",
            )
            fig_top_firms.update_xaxes(title_text="Thousands of firms")
            fig_top_firms.update_layout(height=400)
            st.plotly_chart(fig_top_firms, use_container_width=True, config={'displayModeBar': False})

    # Footer
    st.markdown("""
    <div class="footer">
    <strong>Data source:</strong> U.S. Census Bureau,
    <a href="https://www.census.gov/programs-surveys/bds.html" target="_blank">Business Dynamics Statistics</a> (BDS)<br>
    <strong>Coverage:</strong> 1978-2023 | <strong>Updates:</strong> Annually (typically December)<br>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
