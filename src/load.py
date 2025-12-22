"""
Load cleaned BDS data into SQLite database.

This module handles:
- Creating database tables
- Loading data from CSVs
- Creating indexes for query performance
"""

import sqlite3
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DATA_DIR / "bds.db"


def create_connection() -> sqlite3.Connection:
    """Create a connection to the SQLite database."""
    return sqlite3.connect(DB_PATH)


def load_table(conn: sqlite3.Connection, df: pd.DataFrame, table_name: str) -> None:
    """Load a DataFrame into a SQLite table."""
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"Loaded {len(df)} rows into table '{table_name}'")


def create_indexes(conn: sqlite3.Connection) -> None:
    """Create indexes for better query performance."""
    cursor = conn.cursor()

    # Index on year for all tables
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_national_year ON national(YEAR)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_firm_age_year ON by_firm_age(YEAR)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_state_year ON by_state(YEAR)")

    # Index on firm age for age analysis
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_firm_age_fage ON by_firm_age(FAGE)")

    # Index on state for geographic analysis
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_state_state ON by_state(state)")

    conn.commit()
    print("Created database indexes")


def load_all() -> None:
    """
    Run full load pipeline.

    Loads all cleaned CSV files into SQLite database.
    """
    print("=" * 50)
    print("Starting BDS Data Load")
    print("=" * 50)

    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)

    # Create database connection
    conn = create_connection()
    print(f"Connected to database: {DB_PATH}")

    try:
        # Load national data
        national_df = pd.read_csv(DATA_DIR / "clean_national.csv")
        load_table(conn, national_df, "national")

        # Load firm age data
        firm_age_df = pd.read_csv(DATA_DIR / "clean_by_firm_age.csv")
        load_table(conn, firm_age_df, "by_firm_age")

        # Load state data
        state_df = pd.read_csv(DATA_DIR / "clean_by_state.csv")
        load_table(conn, state_df, "by_state")

        # Create indexes
        create_indexes(conn)

        # Verify load
        cursor = conn.cursor()
        for table in ["national", "by_firm_age", "by_state"]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"Verified: {table} has {count} rows")

    finally:
        conn.close()

    print("=" * 50)
    print("Load Complete!")
    print(f"Database saved to: {DB_PATH}")
    print("=" * 50)


if __name__ == "__main__":
    load_all()
