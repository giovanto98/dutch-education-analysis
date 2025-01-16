import pandas as pd
import numpy as np
from pathlib import Path

def load_and_check_data(file_path: str) -> pd.DataFrame:
    """Load data and perform initial checks."""
    print("\n=== Loading and Initial Checks ===")
    
    df = pd.read_csv(file_path)
    print(f"Total records: {len(df)}")
    print(f"\nColumns in dataset:")
    for col in df.columns:
        print(f"- {col}: {df[col].dtype}")
    
    print("\nMissing values by column:")
    print(df.isnull().sum())
    
    return df

def check_coordinates(df: pd.DataFrame) -> None:
    """Analyze spatial coordinates."""
    print("\n=== Coordinate Analysis ===")
    
    # Check coordinate ranges
    print("\nCoordinate ranges:")
    print(f"Latitude range: {df['latitude'].min():.4f} to {df['latitude'].max():.4f}")
    print(f"Longitude range: {df['longitude'].min():.4f} to {df['longitude'].max():.4f}")
    
    # Check for invalid coordinates
    invalid_coords = df[
        (df['latitude'].isnull()) | 
        (df['longitude'].isnull()) |
        (df['latitude'] < 50) | (df['latitude'] > 54) |  # Netherlands latitude range
        (df['longitude'] < 3) | (df['longitude'] > 8)    # Netherlands longitude range
    ]
    print(f"\nInvalid coordinates found: {len(invalid_coords)}")
    if len(invalid_coords) > 0:
        print("\nSample of invalid coordinates:")
        print(invalid_coords[['full_address', 'latitude', 'longitude']].head())

def check_duplicates(df: pd.DataFrame) -> None:
    """Analyze duplicate records."""
    print("\n=== Duplicate Analysis ===")
    
    # Check exact duplicates
    exact_dupes = df.duplicated().sum()
    print(f"Exact duplicate rows: {exact_dupes}")
    
    # Check location duplicates
    location_dupes = df.groupby(['latitude', 'longitude']).size()
    print("\nLocations by frequency:")
    print(location_dupes.value_counts().sort_index())
    
    if len(location_dupes[location_dupes > 1]) > 0:
        print("\nSample of duplicate locations:")
        dup_coords = location_dupes[location_dupes > 1].index[0]
        sample = df[
            (df['latitude'] == dup_coords[0]) & 
            (df['longitude'] == dup_coords[1])
        ]
        print(sample[['NAAM_VOLLEDIG', 'full_address', 'DT_IN_BEDRIJF', 'DT_UIT_BEDRIJF']].head())

def check_temporal_coverage(df: pd.DataFrame) -> None:
    """Analyze temporal patterns."""
    print("\n=== Temporal Analysis ===")
    
    # Convert date columns
    df['DT_IN_BEDRIJF'] = pd.to_datetime(df['DT_IN_BEDRIJF'], errors='coerce')
    df['DT_UIT_BEDRIJF'] = pd.to_datetime(df['DT_UIT_BEDRIJF'], errors='coerce')
    
    print("\nDate ranges:")
    print(f"Earliest start date: {df['DT_IN_BEDRIJF'].min()}")
    print(f"Latest start date: {df['DT_IN_BEDRIJF'].max()}")
    print(f"Earliest end date: {df['DT_UIT_BEDRIJF'].min()}")
    print(f"Latest end date: {df['DT_UIT_BEDRIJF'].max()}")
    
    # Count active locations
    current_date = pd.Timestamp.now()
    active = df[
        (df['DT_IN_BEDRIJF'] <= current_date) & 
        (df['DT_UIT_BEDRIJF'].isnull() | (df['DT_UIT_BEDRIJF'] > current_date))
    ]
    print(f"\nCurrently active locations: {len(active)}")

def check_address_consistency(df: pd.DataFrame) -> None:
    """Check consistency between address fields."""
    print("\n=== Address Consistency Analysis ===")
    
    # Check postcode format
    invalid_postcodes = df[
        ~df['POSTCODE_VEST'].str.match(r'^\d{4}\s?[A-Z]{2}$', na=True)
    ]
    print(f"Invalid postcode format count: {len(invalid_postcodes)}")
    
    # Check address components
    print("\nMissing address components:")
    address_columns = ['full_address', 'POSTCODE_VEST', 'NAAM_PLAATS_VEST', 'PROVINCIE_VEST']
    for col in address_columns:
        missing = df[col].isnull().sum()
        print(f"- {col}: {missing} missing values")

def generate_summary_report(df: pd.DataFrame) -> None:
    """Generate summary statistics."""
    print("\n=== Summary Report ===")
    
    print("\nDistribution by province:")
    print(df['PROVINCIE_VEST'].value_counts())
    
    print("\nDistribution by record status:")
    print(df['CODE_STAND_RECORD'].value_counts())
    
    print(f"\nTotal unique locations: {len(df['latitude'].unique())}")
    print(f"Total records: {len(df)}")

def main():
    """Main debug function."""
    file_path = 'cleaned_MBO_locations.csv'
    print(f"Analyzing file: {file_path}")
    
    try:
        df = load_and_check_data(file_path)
        check_coordinates(df)
        check_duplicates(df)
        check_temporal_coverage(df)
        check_address_consistency(df)
        generate_summary_report(df)
        
        print("\n=== Debug Analysis Complete ===")
        print("\nRecommendations for QGIS preparation:")
        print("1. Handle duplicate locations: one record per location")
        print("2. Remove invalid coordinates")
        print("3. Standardize address formats")
        print("4. Resolve temporal overlaps")
        print("5. Create status field for current activity")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")

if __name__ == "__main__":
    main()