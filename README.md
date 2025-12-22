# Census Business Dynamics Statistics (BDS) Dashboard

An automated ETL (Extract-Transform-Load) pipeline and interactive dashboard for analyzing U.S. business dynamics, including startup trends, firm births/deaths, and job creation patterns.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture and DAG](#architecture-and-dag)
- [Folder Structure](#folder-structure)
- [Code Files Explained](#code-files-explained)
- [Data Schema](#data-schema)
- [Setup and Installation](#setup-and-installation)
- [Running the Project](#running-the-project)
- [Deployment](#deployment)
- [Extending the Project](#extending-the-project)

---

## Project Overview

### What This Project Does

This project pulls data from the U.S. Census Bureau's Business Dynamics Statistics (BDS) API, cleans and transforms it, stores it in a SQLite database, and displays it in an interactive web dashboard. The entire pipeline is automated to run annually via GitHub Actions.

### What is BDS Data?

The Business Dynamics Statistics (BDS) tracks the lifecycle of U.S. businesses:
- **Firm births**: New businesses entering the market
- **Firm deaths**: Businesses exiting the market
- **Job creation**: New jobs added by expanding or new firms
- **Job destruction**: Jobs lost from contracting or closing firms
- **Firm age**: How old businesses are (startups = age 0)

The data covers 1978 to present and is released annually (typically December).

### Key Concepts

| Term | Definition |
|------|------------|
| **Firm** | A business entity (may have multiple establishments) |
| **Establishment** | A single physical location of a firm |
| **Startup** | A firm in its first year of existence (age 0) |
| **Firm birth** | A new firm identifier appearing in the data |
| **Firm death** | A firm identifier disappearing from the data |

---

## Architecture and DAG

### Pipeline DAG (Directed Acyclic Graph)

The pipeline follows a linear ETL pattern. Each step must complete before the next begins:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           GITHUB ACTIONS TRIGGER                            │
│                    (Scheduled: Dec 15 annually, or manual)                  │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              1. EXTRACT                                     │
│                            (src/extract.py)                                 │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                     │
│  │  National   │    │  By Firm    │    │  By State   │                     │
│  │   Data      │    │    Age      │    │   Data      │                     │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                     │
│         │                  │                  │                             │
│         ▼                  ▼                  ▼                             │
│  raw_national.csv   raw_by_firm_age.csv   raw_by_state.csv                 │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                             2. TRANSFORM                                    │
│                           (src/transform.py)                                │
│                                                                             │
│  - Convert strings to numeric types                                         │
│  - Handle missing values (Census suppression codes)                         │
│  - Calculate derived metrics (startup rate, job creation rate)              │
│  - Add human-readable labels (state names, firm age categories)             │
│                                                                             │
│         ▼                  ▼                  ▼                             │
│  clean_national.csv  clean_by_firm_age.csv  clean_by_state.csv             │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                               3. LOAD                                       │
│                            (src/load.py)                                    │
│                                                                             │
│  - Create SQLite database                                                   │
│  - Load CSV files into tables                                               │
│  - Create indexes for query performance                                     │
│                                                                             │
│                              ▼                                              │
│                          data/bds.db                                        │
│                    ┌──────────┴──────────┐                                  │
│                    │   Tables:           │                                  │
│                    │   - national        │                                  │
│                    │   - by_firm_age     │                                  │
│                    │   - by_state        │                                  │
│                    └─────────────────────┘                                  │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            4. DASHBOARD                                     │
│                          (dashboard/app.py)                                 │
│                                                                             │
│  - Reads from SQLite database                                               │
│  - Renders interactive Plotly charts                                        │
│  - Hosted on Streamlit Cloud                                                │
│                                                                             │
│                         http://localhost:8501                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow Summary

```
Census API  ──►  Raw CSVs  ──►  Clean CSVs  ──►  SQLite DB  ──►  Dashboard
   (web)         (data/)        (data/)         (data/)        (browser)
```

---

## Folder Structure

```
DataPulls/
│
├── .github/
│   └── workflows/
│       └── etl_pipeline.yml      # GitHub Actions workflow definition
│
├── src/                          # Source code for ETL pipeline
│   ├── __init__.py               # Makes src a Python package
│   ├── extract.py                # Step 1: Pull data from Census API
│   ├── transform.py              # Step 2: Clean and enrich data
│   └── load.py                   # Step 3: Load data into SQLite
│
├── dashboard/                    # Streamlit web application
│   └── app.py                    # Main dashboard application
│
├── data/                         # Data storage (generated, not in repo initially)
│   ├── raw_national.csv          # Raw API response - national level
│   ├── raw_by_firm_age.csv       # Raw API response - by firm age
│   ├── raw_by_state.csv          # Raw API response - by state
│   ├── clean_national.csv        # Cleaned national data
│   ├── clean_by_firm_age.csv     # Cleaned firm age data
│   ├── clean_by_state.csv        # Cleaned state data
│   └── bds.db                    # SQLite database (final output)
│
├── tests/                        # Unit tests (placeholder)
│   └── __init__.py
│
├── run_pipeline.py               # Main entry point - runs full ETL
├── requirements.txt              # Python dependencies
├── .gitignore                    # Files to exclude from git
└── README.md                     # This file
```

---

## Code Files Explained

### `run_pipeline.py` - Main Entry Point

**Purpose**: Orchestrates the entire ETL pipeline by calling extract, transform, and load in sequence.

**When to modify**: If you need to add pre/post-processing steps, logging, or error handling.

```python
# Simplified structure:
def main():
    extract_all()   # Step 1: Get data from API
    transform_all() # Step 2: Clean data
    load_all()      # Step 3: Store in database
```

---

### `src/extract.py` - Data Extraction

**Purpose**: Pulls raw data from the Census BDS API.

**Key functions**:
| Function | Description |
|----------|-------------|
| `fetch_national_time_series()` | Gets aggregate U.S. data for all years |
| `fetch_by_firm_age()` | Gets data broken down by firm age (0, 1, 2, ... years) |
| `fetch_by_state()` | Gets data for each U.S. state |
| `save_raw_data()` | Saves API response to CSV |
| `extract_all()` | Runs all extractions and saves files |

**API variables pulled**:
- `FIRM` - Number of firms
- `ESTAB` - Number of establishments
- `EMP` - Total employment
- `FIRMDEATH_FIRMS` - Number of firm deaths
- `ESTABS_ENTRY` - Establishment entries (births)
- `ESTABS_EXIT` - Establishment exits (deaths)
- `JOB_CREATION` - Gross job creation
- `JOB_DESTRUCTION` - Gross job destruction
- `NET_JOB_CREATION` - Net job creation

**When to modify**:
- To add new variables from the API
- To change geographic granularity (e.g., add county-level data)
- To filter by industry (NAICS codes)

---

### `src/transform.py` - Data Transformation

**Purpose**: Cleans raw data and calculates derived metrics.

**Key functions**:
| Function | Description |
|----------|-------------|
| `clean_numeric_columns()` | Converts string values to numbers, handles Census suppression codes |
| `calculate_rates()` | Computes startup rate, exit rate, job creation rate |
| `transform_national()` | Processes national data |
| `transform_by_firm_age()` | Processes firm age data, adds readable labels |
| `transform_by_state()` | Processes state data, adds state names from FIPS codes |
| `transform_all()` | Runs all transformations |

**Derived metrics calculated**:
| Metric | Formula |
|--------|---------|
| `STARTUP_RATE` | (ESTABS_ENTRY / ESTAB) * 100 |
| `EXIT_RATE` | (ESTABS_EXIT / ESTAB) * 100 |
| `JOB_CREATION_RATE` | (JOB_CREATION / EMP) * 100 |
| `JOB_DESTRUCTION_RATE` | (JOB_DESTRUCTION / EMP) * 100 |

**Firm age labels** (FAGE codes):
| Code | Label |
|------|-------|
| 010 | 0 (Startups) |
| 020 | 1 year |
| 030 | 2 years |
| ... | ... |
| 090 | 26+ years |

**When to modify**:
- To add new derived metrics
- To change data cleaning logic
- To add new label mappings

---

### `src/load.py` - Database Loading

**Purpose**: Loads cleaned CSV data into a SQLite database.

**Key functions**:
| Function | Description |
|----------|-------------|
| `create_connection()` | Opens connection to SQLite database |
| `load_table()` | Loads a DataFrame into a database table |
| `create_indexes()` | Creates indexes for faster queries |
| `load_all()` | Loads all tables and creates indexes |

**Database tables created**:
| Table | Description | Key columns |
|-------|-------------|-------------|
| `national` | U.S. aggregate data by year | YEAR, FIRM, EMP, ... |
| `by_firm_age` | Data by firm age and year | YEAR, FAGE, FIRM, ... |
| `by_state` | Data by state and year | YEAR, state, FIRM, ... |

**Indexes created** (for query performance):
- `idx_national_year` on national(YEAR)
- `idx_firm_age_year` on by_firm_age(YEAR)
- `idx_firm_age_fage` on by_firm_age(FAGE)
- `idx_state_year` on by_state(YEAR)
- `idx_state_state` on by_state(state)

**When to modify**:
- To add new tables
- To change indexing strategy
- To switch to a different database (PostgreSQL, etc.)

---

### `dashboard/app.py` - Streamlit Dashboard

**Purpose**: Renders an interactive web dashboard for data visualization.

**Key components**:
| Component | Description |
|-----------|-------------|
| Custom CSS | Economist-style typography and colors |
| `load_data()` | Cached function to read from SQLite |
| `apply_chart_style()` | Applies consistent styling to Plotly charts |
| `main()` | Main application with 4 tabs |

**Dashboard tabs**:
1. **National Trends** - Establishment births/deaths, total firms, employment
2. **Startup Analysis** - Startup rate over time, employment by firm age
3. **Job Dynamics** - Job creation vs destruction, net job creation
4. **State Comparison** - Top states by employment, firms, startup rate

**When to modify**:
- To add new visualizations
- To change chart styling
- To add new filters or interactivity

---

### `.github/workflows/etl_pipeline.yml` - GitHub Actions

**Purpose**: Automates the ETL pipeline to run on a schedule.

**Trigger conditions**:
- **Scheduled**: Runs December 15 at midnight UTC (cron: `0 0 15 12 *`)
- **Manual**: Can be triggered from GitHub Actions tab

**Workflow steps**:
1. Checkout repository
2. Set up Python 3.11
3. Install dependencies
4. Run `python run_pipeline.py`
5. Commit updated data files
6. Push changes to repository

**When to modify**:
- To change the schedule
- To add notifications (Slack, email)
- To add tests before deployment

---

## Data Schema

### Table: `national`

| Column | Type | Description |
|--------|------|-------------|
| YEAR | INTEGER | Year (1978-2023) |
| FIRM | INTEGER | Number of firms |
| ESTAB | INTEGER | Number of establishments |
| EMP | INTEGER | Total employment |
| FIRMDEATH_FIRMS | INTEGER | Number of firm deaths |
| ESTABS_ENTRY | INTEGER | Establishment entries |
| ESTABS_EXIT | INTEGER | Establishment exits |
| JOB_CREATION | INTEGER | Gross job creation |
| JOB_DESTRUCTION | INTEGER | Gross job destruction |
| NET_JOB_CREATION | INTEGER | Net job creation |
| STARTUP_RATE | REAL | Establishment entry rate (%) |
| EXIT_RATE | REAL | Establishment exit rate (%) |
| JOB_CREATION_RATE | REAL | Job creation rate (%) |
| JOB_DESTRUCTION_RATE | REAL | Job destruction rate (%) |

### Table: `by_firm_age`

Same columns as `national`, plus:
| Column | Type | Description |
|--------|------|-------------|
| FAGE | TEXT | Firm age code (010, 020, ...) |
| FIRM_AGE_LABEL | TEXT | Human-readable age label |

### Table: `by_state`

Same columns as `national`, plus:
| Column | Type | Description |
|--------|------|-------------|
| state | TEXT | State FIPS code (01, 02, ...) |
| STATE_NAME | TEXT | State name (Alabama, Alaska, ...) |

---

## Setup and Installation

### Prerequisites

- Python 3.9 or higher
- pip (Python package manager)
- Git

### Step-by-Step Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/DataPulls.git
   cd DataPulls
   ```

2. **Create a virtual environment** (recommended):
   ```bash
   # Create virtual environment
   python -m venv venv

   # Activate it
   # On Windows:
   venv\Scripts\activate
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| requests | >=2.31.0 | HTTP requests to Census API |
| pandas | >=2.0.0 | Data manipulation |
| streamlit | >=1.28.0 | Web dashboard framework |
| plotly | >=5.18.0 | Interactive charts |

---

## Running the Project

### Run the Full Pipeline

```bash
python run_pipeline.py
```

This will:
1. Fetch data from Census API (~30 seconds)
2. Clean and transform data (~5 seconds)
3. Load into SQLite database (~2 seconds)

### Run Individual Steps

```bash
# Extract only
python -m src.extract

# Transform only (requires extract to have run first)
python -m src.transform

# Load only (requires transform to have run first)
python -m src.load
```

### Launch the Dashboard

```bash
streamlit run dashboard/app.py
```

Then open http://localhost:8501 in your browser.

### Query the Database Directly

```bash
# Open SQLite CLI
sqlite3 data/bds.db

# Example queries:
SELECT * FROM national WHERE YEAR = 2023;
SELECT STATE_NAME, STARTUP_RATE FROM by_state WHERE YEAR = 2023 ORDER BY STARTUP_RATE DESC LIMIT 10;
```

---

## Deployment

### GitHub Actions (Automated Updates)

1. Push your code to GitHub
2. Go to repository Settings > Actions > General
3. Enable "Read and write permissions" for workflows
4. The pipeline will run automatically every December 15

To trigger manually:
1. Go to Actions tab
2. Select "BDS ETL Pipeline"
3. Click "Run workflow"

### Streamlit Cloud (Dashboard Hosting)

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with GitHub
3. Click "New app"
4. Select your repository
5. Set main file path: `dashboard/app.py`
6. Click "Deploy"

The dashboard will be available at `https://your-app-name.streamlit.app`

---

## Extending the Project

### Add a New Data Source

1. Create a new fetch function in `src/extract.py`
2. Add a transform function in `src/transform.py`
3. Add table loading in `src/load.py`
4. Update `run_pipeline.py` if needed

### Add a New Visualization

1. Open `dashboard/app.py`
2. Add a new chart in the appropriate tab (or create a new tab)
3. Use `apply_chart_style()` for consistent formatting

### Add Industry Breakdown

The BDS API supports NAICS industry codes. To add industry data:

1. In `src/extract.py`, add:
   ```python
   def fetch_by_industry():
       params = {
           "get": ",".join(VARIABLES) + ",NAICS",
           "for": "us:*",
           "YEAR": "*",
       }
       # ... rest of fetch logic
   ```

2. Add corresponding transform and load functions
3. Create industry visualizations in the dashboard

### Switch to PostgreSQL

1. Install psycopg2: `pip install psycopg2-binary`
2. Modify `src/load.py` to use PostgreSQL connection
3. Update connection strings and SQL syntax as needed

---

## License

MIT License

## Acknowledgments

- U.S. Census Bureau for providing the BDS API
- Built as a data engineering portfolio project
