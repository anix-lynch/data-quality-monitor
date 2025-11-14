#!/usr/bin/env python3
"""
Data Quality & Monitoring Dashboard - Sample Data Generator

🎯 PURPOSE: Automated generation of synthetic training datasets for data quality monitoring system testing and demonstration
📊 FEATURES: Schema definition creation, clean historical data generation, intentional quality issue injection, realistic data distributions, CSV export
🏗️ ARCHITECTURE: Configuration → Data generation functions → Quality issue injection → File output → Validation reporting
⚡ STATUS: Production data generator, creates realistic ML training datasets, handles multiple data types, configurable quality issues, reproducible results
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from pathlib import Path
import random

def generate_schema():
    """Generate the schema definition."""
    schema = {
        "fields": {
            "user_id": {
                "dtype": "string",
                "required": True
            },
            "age": {
                "dtype": "int",
                "min": 0,
                "max": 120,
                "required": True
            },
            "country": {
                "dtype": "string",
                "required": False
            },
            "signup_date": {
                "dtype": "datetime",
                "required": True
            },
            "spend": {
                "dtype": "float",
                "min": 0,
                "required": False
            }
        }
    }
    return schema

def generate_historical_reference(n_rows=1000):
    """Generate clean historical reference data."""
    np.random.seed(42)  # For reproducibility

    # Generate user IDs
    user_ids = [f"USER_{i:04d}" for i in range(1, n_rows + 1)]

    # Generate ages (normal distribution around 35)
    ages = np.random.normal(35, 12, n_rows).astype(int)
    ages = np.clip(ages, 18, 80)  # Reasonable age range

    # Generate countries
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Australia', 'Japan', 'Brazil']
    country_weights = [0.4, 0.15, 0.1, 0.08, 0.07, 0.06, 0.08, 0.06]
    countries_list = np.random.choice(countries, n_rows, p=country_weights)

    # Generate signup dates (last 2 years)
    base_date = datetime.now() - timedelta(days=730)
    signup_dates = [
        (base_date + timedelta(days=np.random.randint(0, 730))).strftime('%Y-%m-%d')
        for _ in range(n_rows)
    ]

    # Generate spend amounts (exponential distribution)
    spends = np.random.exponential(50, n_rows)
    spends = np.round(spends, 2)

    # Create DataFrame
    df = pd.DataFrame({
        'user_id': user_ids,
        'age': ages,
        'country': countries_list,
        'signup_date': signup_dates,
        'spend': spends
    })

    return df

def generate_messy_input(n_rows=800):
    """Generate messy input data with various quality issues."""
    np.random.seed(123)  # Different seed for variety

    # Start with clean data similar to historical
    base_df = generate_historical_reference(n_rows)

    # Introduce missing values randomly
    for col in base_df.columns:
        if col != 'user_id':  # Keep user_id mostly complete
            missing_rate = np.random.uniform(0.05, 0.15)  # 5-15% missing
            mask = np.random.random(n_rows) < missing_rate
            base_df.loc[mask, col] = None

    # Type inconsistencies in age column (strings mixed with numbers)
    age_indices = np.random.choice(n_rows, size=int(n_rows * 0.1), replace=False)
    for idx in age_indices:
        if np.random.random() < 0.5:
            base_df.loc[idx, 'age'] = str(base_df.loc[idx, 'age'])
        else:
            base_df.loc[idx, 'age'] = "invalid_age"

    # Invalid dates
    date_indices = np.random.choice(n_rows, size=int(n_rows * 0.08), replace=False)
    invalid_dates = ["2023-13-45", "not-a-date", "00/00/0000", ""]
    for idx in date_indices:
        base_df.loc[idx, 'signup_date'] = np.random.choice(invalid_dates)

    # Outliers in spend
    spend_indices = np.random.choice(n_rows, size=int(n_rows * 0.05), replace=False)
    for idx in spend_indices:
        if np.random.random() < 0.5:
            base_df.loc[idx, 'spend'] = np.random.uniform(10000, 50000)  # Very high spends
        else:
            base_df.loc[idx, 'spend'] = np.random.uniform(-1000, -100)  # Negative spends

    # Invalid countries
    country_indices = np.random.choice(n_rows, size=int(n_rows * 0.07), replace=False)
    invalid_countries = ["Atlantis", "123", "", "NotACountry"]
    for idx in country_indices:
        base_df.loc[idx, 'country'] = np.random.choice(invalid_countries)

    # Duplicate user_ids (introduce some duplicates)
    duplicate_indices = np.random.choice(n_rows, size=int(n_rows * 0.03), replace=False)
    existing_ids = base_df['user_id'].dropna().unique()
    for idx in duplicate_indices:
        if len(existing_ids) > 0:
            base_df.loc[idx, 'user_id'] = np.random.choice(existing_ids)

    # Ages outside valid range
    age_outlier_indices = np.random.choice(n_rows, size=int(n_rows * 0.04), replace=False)
    for idx in age_outlier_indices:
        if np.random.random() < 0.5:
            base_df.loc[idx, 'age'] = np.random.randint(150, 200)  # Too old
        else:
            base_df.loc[idx, 'age'] = np.random.randint(-10, 0)  # Negative age

    return base_df

def main():
    """Main function to generate all sample data files."""
    # Define paths
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)

    print("Generating sample data files...")

    # Generate and save schema
    schema = generate_schema()
    schema_path = data_dir / "schema.json"
    with open(schema_path, 'w') as f:
        json.dump(schema, f, indent=2)
    print(f"✓ Created schema.json at {schema_path}")

    # Generate and save historical reference data
    historical_df = generate_historical_reference(1000)
    historical_path = data_dir / "historical_reference.csv"
    historical_df.to_csv(historical_path, index=False)
    print(f"✓ Created historical_reference.csv ({len(historical_df)} rows) at {historical_path}")

    # Generate and save messy input data
    messy_df = generate_messy_input(800)
    messy_path = data_dir / "messy_input.csv"
    messy_df.to_csv(messy_path, index=False)
    print(f"✓ Created messy_input.csv ({len(messy_df)} rows) at {messy_path}")

    # Print some statistics
    print("\nData generation summary:")
    print(f"Schema fields: {list(schema['fields'].keys())}")
    print(f"Historical data completeness: {historical_df.notna().mean().mean():.1%}")
    print(f"Input data completeness: {messy_df.notna().mean().mean():.1%}")

    # Show some example issues in messy data
    print("\nExample issues in messy_input.csv:")
    print(f"- Null values by column: {messy_df.isnull().sum().to_dict()}")
    print(f"- Age type distribution: {messy_df['age'].dtype} (should be int64)")
    print(f"- Unique countries: {messy_df['country'].nunique()} (including invalid ones)")

    print("\nSample data generation complete!")

if __name__ == "__main__":
    main()
