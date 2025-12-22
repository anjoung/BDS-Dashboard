"""
Main pipeline runner script.

Executes the full ETL pipeline:
1. Extract data from Census BDS API
2. Transform and clean the data
3. Load into SQLite database
"""

from src.extract import extract_all
from src.transform import transform_all
from src.load import load_all


def main():
    """Run the complete ETL pipeline."""
    print("\n" + "=" * 60)
    print("CENSUS BDS DATA PIPELINE")
    print("=" * 60 + "\n")

    # Step 1: Extract
    print("\n[STEP 1/3] EXTRACTION\n")
    extract_all()

    # Step 2: Transform
    print("\n[STEP 2/3] TRANSFORMATION\n")
    transform_all()

    # Step 3: Load
    print("\n[STEP 3/3] LOADING\n")
    load_all()

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
