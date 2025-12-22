"""
Extract data from Census Bureau Business Dynamics Statistics (BDS) API.

API Documentation: https://www.census.gov/data/developers/data-sets/business-dynamics.html
API Endpoint: https://api.census.gov/data/timeseries/bds
"""

import requests
import pandas as pd
from pathlib import Path
import json

# API Configuration
BASE_URL = "https://api.census.gov/data/timeseries/bds"

# Key variables for our analysis
VARIABLES = [
    "FIRM",           # Number of firms
    "ESTAB",          # Number of establishments
    "EMP",            # Employment
    "FIRMDEATH_FIRMS", # Firm deaths
    "ESTABS_ENTRY",   # Establishment entries (births)
    "ESTABS_EXIT",    # Establishment exits (deaths)
    "JOB_CREATION",   # Gross job creation
    "JOB_DESTRUCTION",# Gross job destruction
    "NET_JOB_CREATION", # Net job creation
]

# Data directory
DATA_DIR = Path(__file__).parent.parent / "data"


def fetch_national_time_series() -> pd.DataFrame:
    """
    Fetch national-level BDS data across all available years.

    Returns:
        DataFrame with yearly national business dynamics statistics.
    """
    # Build the API request
    variables_str = ",".join(VARIABLES)
    params = {
        "get": variables_str,
        "for": "us:*",
        "YEAR": "*",  # All available years
    }

    print(f"Fetching national time series data from {BASE_URL}...")
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()

    # First row is headers, rest is data
    headers = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=headers)
    print(f"Retrieved {len(df)} rows of national data")

    return df


def fetch_by_firm_age() -> pd.DataFrame:
    """
    Fetch BDS data broken down by firm age (for startup analysis).

    FAGE categories:
    - 001: All firms (aggregated)
    - 010: Age 0 (startups/births)
    - 020: Age 1
    - 030: Age 2
    - 040: Age 3
    - 050: Age 4
    - 060: Age 5
    - 065: Age 6-10
    - 070: Age 11-15
    - 075: Age 16-20
    - 080: Age 21-25
    - 090: Age 26+

    Returns:
        DataFrame with yearly data by firm age.
    """
    variables_str = ",".join(VARIABLES)
    params = {
        "get": f"{variables_str},FAGE",
        "for": "us:*",
        "YEAR": "*",
    }

    print(f"Fetching data by firm age from {BASE_URL}...")
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()
    headers = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=headers)
    print(f"Retrieved {len(df)} rows of firm age data")

    return df


def fetch_by_state() -> pd.DataFrame:
    """
    Fetch BDS data by state for geographic analysis.

    Returns:
        DataFrame with yearly data by state.
    """
    variables_str = ",".join(VARIABLES)
    params = {
        "get": variables_str,
        "for": "state:*",
        "YEAR": "*",
    }

    print(f"Fetching state-level data from {BASE_URL}...")
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()
    headers = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=headers)
    print(f"Retrieved {len(df)} rows of state data")

    return df


def save_raw_data(df: pd.DataFrame, filename: str) -> Path:
    """Save raw extracted data to CSV."""
    DATA_DIR.mkdir(exist_ok=True)
    filepath = DATA_DIR / filename
    df.to_csv(filepath, index=False)
    print(f"Saved raw data to {filepath}")
    return filepath


def extract_all() -> dict[str, pd.DataFrame]:
    """
    Run full extraction pipeline.

    Returns:
        Dictionary of DataFrames with all extracted data.
    """
    print("=" * 50)
    print("Starting BDS Data Extraction")
    print("=" * 50)

    datasets = {}

    # Extract national time series
    datasets["national"] = fetch_national_time_series()
    save_raw_data(datasets["national"], "raw_national.csv")

    # Extract by firm age
    datasets["by_firm_age"] = fetch_by_firm_age()
    save_raw_data(datasets["by_firm_age"], "raw_by_firm_age.csv")

    # Extract by state
    datasets["by_state"] = fetch_by_state()
    save_raw_data(datasets["by_state"], "raw_by_state.csv")

    print("=" * 50)
    print("Extraction Complete!")
    print("=" * 50)

    return datasets


if __name__ == "__main__":
    extract_all()
