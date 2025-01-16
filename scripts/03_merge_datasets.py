import pandas as pd
from pathlib import Path
import os

def debug_dataset(df, name):
    if df is None or df.empty:
        print(f"Debug: {name} - Dataset is empty or None.")
    else:
        print(f"Debug: {name} - Dataset contains {len(df)} records.")
        print(f"Debug: {name} - Columns: {list(df.columns)}")

def merge_datasets():
    """Merge and validate organizations and education datasets."""
    
    # Paths
    data_dir = Path(os.getcwd()) / 'data'
    orgs_file = data_dir / 'processed' / 'intermediate' / 'clean_organizations.csv'
    edu_file = data_dir / 'processed' / 'intermediate' / 'clean_education.csv'
    output_dir = data_dir / 'processed' / 'final'
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # Check if files exist
    if not orgs_file.exists():
        print(f"Debug: Organizations file does not exist at {orgs_file}")
        return
    if not edu_file.exists():
        print(f"Debug: Education file does not exist at {edu_file}")
        return

    # Read processed data
    orgs_df = pd.read_csv(orgs_file)
    debug_dataset(orgs_df, "Organizations Data")

    edu_df = pd.read_csv(edu_file)
    debug_dataset(edu_df, "Education Data")
    
    # Create lookup tables for validation
    edu_types = edu_df[['Onderwijssector', 'Onderwijstype']].drop_duplicates()
    debug_dataset(edu_types, "Education Types Lookup")

    # Define CODE_SOORT values for each sector
    sector_codes = {
        'primary': ['BAS', 'SBAS', 'SPEC', 'VST'],
        'secondary': ['VOS', 'PROS', 'AOCV', 'VSTS', 'VSTZ'],
        'vocational': ['MBO', 'VE', 'ILOC'],   # Updated vocational codes
        'higher': ['HBO', 'WO', 'LOC']         # Updated higher education codes
    }

    # Process each education sector
    sectors = ['primary', 'secondary', 'vocational', 'higher']
    
    for sector in sectors:
        print(f"\nProcessing {sector} education:")

        # Filter organizations
        codes = sector_codes.get(sector, [])
        sector_orgs = orgs_df[orgs_df['CODE_SOORT'].isin(codes)].copy()
        debug_dataset(sector_orgs, f"{sector.capitalize()} Organizations")
        
        # Add education type information
        sector_edu = edu_df[edu_df['education_sector'] == sector].copy()
        debug_dataset(sector_edu, f"{sector.capitalize()} Education Data")
        
        # Save sector-specific data
        output_file = output_dir / f'{sector}_education_locations.csv'
        sector_orgs.to_csv(output_file, index=False)
        print(f"Saved {len(sector_orgs)} locations to: {output_file}")

        # Open and check saved file content
        if output_file.exists():
            saved_data = pd.read_csv(output_file)
            debug_dataset(saved_data, f"Saved {sector.capitalize()} Education Locations")
        else:
            print(f"Debug: Output file {output_file} does not exist after saving.")

if __name__ == "__main__":
    merge_datasets()