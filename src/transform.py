"""
Transform and clean BDS data for analysis.

This module handles:
- Data type conversions
- Missing value handling
- Calculating derived metrics (rates, percentages)
- Data validation
"""

import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"

# Firm age category labels (FAGE codes)
# Note: FAGE comes as strings from API but converts to int when saved to CSV
# Source: https://api.census.gov/data/timeseries/bds?get=FAGE,FAGE_LABEL
FIRM_AGE_LABELS = {
    1: "Total",
    10: "0 (Startups)",
    20: "1 year",
    30: "2 years",
    40: "3 years",
    50: "4 years",
    60: "5 years",
    65: "1-5 years",      # Aggregate category
    70: "6-10 years",
    75: "11+ years",      # Aggregate category (all firms 11+)
    80: "11-15 years",
    90: "16-20 years",
    100: "21-25 years",
    110: "26+ years",
    150: "Left Censored",
}

# US State FIPS codes to names
# Note: FIPS codes come as strings from API but convert to int when saved to CSV
STATE_FIPS = {
    1: "Alabama", 2: "Alaska", 4: "Arizona", 5: "Arkansas",
    6: "California", 8: "Colorado", 9: "Connecticut", 10: "Delaware",
    11: "District of Columbia", 12: "Florida", 13: "Georgia", 15: "Hawaii",
    16: "Idaho", 17: "Illinois", 18: "Indiana", 19: "Iowa",
    20: "Kansas", 21: "Kentucky", 22: "Louisiana", 23: "Maine",
    24: "Maryland", 25: "Massachusetts", 26: "Michigan", 27: "Minnesota",
    28: "Mississippi", 29: "Missouri", 30: "Montana", 31: "Nebraska",
    32: "Nevada", 33: "New Hampshire", 34: "New Jersey", 35: "New Mexico",
    36: "New York", 37: "North Carolina", 38: "North Dakota", 39: "Ohio",
    40: "Oklahoma", 41: "Oregon", 42: "Pennsylvania", 44: "Rhode Island",
    45: "South Carolina", 46: "South Dakota", 47: "Tennessee", 48: "Texas",
    49: "Utah", 50: "Vermont", 51: "Virginia", 53: "Washington",
    54: "West Virginia", 55: "Wisconsin", 56: "Wyoming",
}

# Numeric columns to convert
NUMERIC_COLS = [
    "FIRM", "ESTAB", "EMP", "FIRMDEATH_FIRMS",
    "ESTABS_ENTRY", "ESTABS_EXIT", "JOB_CREATION",
    "JOB_DESTRUCTION", "NET_JOB_CREATION", "YEAR"
]


def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert numeric columns from strings to proper numeric types."""
    df = df.copy()

    for col in NUMERIC_COLS:
        if col in df.columns:
            # Handle special Census codes (D = suppressed, S = suppressed, etc.)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def calculate_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate derived rate metrics."""
    df = df.copy()

    # Only calculate if we have the necessary columns
    if "FIRM" in df.columns and "ESTABS_ENTRY" in df.columns:
        # Startup rate (establishment entries / total establishments)
        df["STARTUP_RATE"] = (df["ESTABS_ENTRY"] / df["ESTAB"] * 100).round(2)

    if "FIRM" in df.columns and "ESTABS_EXIT" in df.columns:
        # Exit rate (establishment exits / total establishments)
        df["EXIT_RATE"] = (df["ESTABS_EXIT"] / df["ESTAB"] * 100).round(2)

    if "EMP" in df.columns and "JOB_CREATION" in df.columns:
        # Job creation rate
        df["JOB_CREATION_RATE"] = (df["JOB_CREATION"] / df["EMP"] * 100).round(2)

    if "EMP" in df.columns and "JOB_DESTRUCTION" in df.columns:
        # Job destruction rate
        df["JOB_DESTRUCTION_RATE"] = (df["JOB_DESTRUCTION"] / df["EMP"] * 100).round(2)

    # Firm death rate (firm deaths / total firms)
    if "FIRM" in df.columns and "FIRMDEATH_FIRMS" in df.columns:
        df["FIRM_DEATH_RATE"] = (df["FIRMDEATH_FIRMS"] / df["FIRM"] * 100).round(2)

    return df


def extract_firm_births(firm_age_df: pd.DataFrame) -> pd.DataFrame:
    """Extract firm birth counts from firm age data (FAGE=10 represents new firms)."""
    # FAGE=10 is "0 (Startups)" - firms born in the current year
    startups = firm_age_df[firm_age_df["FAGE"] == 10][["YEAR", "FIRM"]].copy()
    startups = startups.rename(columns={"FIRM": "FIRM_BIRTHS"})
    return startups


def add_firm_birth_rate(national_df: pd.DataFrame, firm_age_df: pd.DataFrame) -> pd.DataFrame:
    """Add firm birth rate to national data using firm age data."""
    df = national_df.copy()

    # Extract firm births from firm age data
    firm_births = extract_firm_births(firm_age_df)

    # Merge with national data
    df = df.merge(firm_births, on="YEAR", how="left")

    # Calculate firm birth rate
    df["FIRM_BIRTH_RATE"] = (df["FIRM_BIRTHS"] / df["FIRM"] * 100).round(2)

    return df


def transform_national(df: pd.DataFrame) -> pd.DataFrame:
    """Transform national time series data."""
    print("Transforming national data...")

    df = clean_numeric_columns(df)
    df = calculate_rates(df)

    # Sort by year
    df = df.sort_values("YEAR").reset_index(drop=True)

    # Drop the 'us' column if present (it's just "1" for all rows)
    if "us" in df.columns:
        df = df.drop(columns=["us"])

    print(f"National data: {len(df)} rows, years {df['YEAR'].min()}-{df['YEAR'].max()}")

    return df


def transform_by_firm_age(df: pd.DataFrame) -> pd.DataFrame:
    """Transform firm age breakdown data."""
    print("Transforming firm age data...")

    df = clean_numeric_columns(df)
    df = calculate_rates(df)

    # Add readable firm age labels
    if "FAGE" in df.columns:
        df["FIRM_AGE_LABEL"] = df["FAGE"].map(FIRM_AGE_LABELS)

    # Sort by year and firm age
    df = df.sort_values(["YEAR", "FAGE"]).reset_index(drop=True)

    # Drop the 'us' column if present
    if "us" in df.columns:
        df = df.drop(columns=["us"])

    print(f"Firm age data: {len(df)} rows")

    return df


def transform_by_state(df: pd.DataFrame) -> pd.DataFrame:
    """Transform state-level data."""
    print("Transforming state data...")

    df = clean_numeric_columns(df)
    df = calculate_rates(df)

    # Add state names
    if "state" in df.columns:
        df["STATE_NAME"] = df["state"].map(STATE_FIPS)

    # Sort by year and state
    df = df.sort_values(["YEAR", "state"]).reset_index(drop=True)

    print(f"State data: {len(df)} rows, {df['state'].nunique()} states")

    return df


def save_clean_data(df: pd.DataFrame, filename: str) -> Path:
    """Save cleaned data to CSV."""
    filepath = DATA_DIR / filename
    df.to_csv(filepath, index=False)
    print(f"Saved clean data to {filepath}")
    return filepath


def transform_all() -> dict[str, pd.DataFrame]:
    """
    Run full transformation pipeline on extracted data.

    Returns:
        Dictionary of cleaned DataFrames.
    """
    print("=" * 50)
    print("Starting BDS Data Transformation")
    print("=" * 50)

    datasets = {}

    # Transform firm age data first (needed to extract firm births)
    raw_firm_age = pd.read_csv(DATA_DIR / "raw_by_firm_age.csv")
    raw_firm_age = clean_numeric_columns(raw_firm_age)  # Clean for firm birth extraction
    datasets["by_firm_age"] = transform_by_firm_age(raw_firm_age)
    save_clean_data(datasets["by_firm_age"], "clean_by_firm_age.csv")

    # Transform national data and add firm birth rate from firm age data
    raw_national = pd.read_csv(DATA_DIR / "raw_national.csv")
    datasets["national"] = transform_national(raw_national)
    # Add firm birth rate using firm age data
    datasets["national"] = add_firm_birth_rate(datasets["national"], raw_firm_age)
    save_clean_data(datasets["national"], "clean_national.csv")

    # Transform state data
    raw_state = pd.read_csv(DATA_DIR / "raw_by_state.csv")
    datasets["by_state"] = transform_by_state(raw_state)
    save_clean_data(datasets["by_state"], "clean_by_state.csv")

    print("=" * 50)
    print("Transformation Complete!")
    print("=" * 50)

    return datasets


if __name__ == "__main__":
    transform_all()
