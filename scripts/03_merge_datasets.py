import pandas as pd
from pathlib import Path
import os

# Set working directory to the project root
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
os.chdir(project_root)

def debug_dataset(df, name):
    """Debug helper function to print dataset information."""
    if df is None or df.empty:
        print(f"Debug: {name} - Dataset is empty or None.")
    else:
        print(f"Debug: {name} - Dataset contains {len(df)} records.")
        print(f"Debug: {name} - Columns: {list(df.columns)}")
        if 'CODE_SOORT' in df.columns:
            print(f"\nDebug: {name} - CODE_SOORT distribution:")
            print(df['CODE_SOORT'].value_counts())
        if 'CODE_FUNCTIE' in df.columns:
            print(f"\nDebug: {name} - CODE_FUNCTIE distribution:")
            print(df['CODE_FUNCTIE'].value_counts())

def clean_and_standardize(df):
    """Clean and standardize dataset columns."""
    # Convert dates
    date_columns = ['DT_IN_BEDRIJF', 'DT_UIT_BEDRIJF', 'DT_BEGIN_RECORD', 'DT_EINDE_RECORD']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Clean address fields
    if 'full_address' not in df.columns:
        df['full_address'] = df['NAAM_STRAAT_VEST'].astype(str).fillna('') + ' ' + \
                            df['NR_HUIS_VEST'].astype(str).fillna('') + ' ' + \
                            df['NR_HUIS_TOEV_VEST'].astype(str).fillna('')
    
    # Clean postcode
    if 'POSTCODE_VEST' in df.columns:
        df['postcode_cleaned'] = df['POSTCODE_VEST'].astype(str).fillna('').str.lower().str.replace(' ', '')
    
    # Extract house number
    if 'NR_HUIS_VEST' in df.columns:
        df['house_number'] = df['NR_HUIS_VEST'].astype(str).str.extract(r'(\d+)')
    
    return df

def merge_datasets():
    """Merge and validate organizations and education datasets."""
    
    # Define correct data paths
    data_dir = project_root / 'data'
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
    print("\nLoading datasets...")
    orgs_df = pd.read_csv(orgs_file)
    debug_dataset(orgs_df, "Organizations Data")

    edu_df = pd.read_csv(edu_file)
    debug_dataset(edu_df, "Education Data")
    
    # Create lookup tables for validation
    edu_types = edu_df[['Onderwijssector', 'Onderwijstype']].drop_duplicates()
    debug_dataset(edu_types, "Education Types Lookup")

    # Define sector codes (including ALL possible codes)
    sector_codes = {
        'primary': ['BAS', 'SBAS', 'SPEC', 'VST'],
        'secondary': ['VOS', 'PROS', 'AOCV', 'VSTS', 'VSTZ'],
        'vocational': ['MBO', 'VE', 'ILOC'],
        'higher': ['HBO', 'WO', 'LOC']
    }

    # Process each education sector
    sectors = ['primary', 'secondary', 'vocational', 'higher']
    
    for sector in sectors:
        print(f"\nProcessing {sector} education:")
        
        # Filter organizations by sector codes but keep all function types
        codes = sector_codes.get(sector, [])
        sector_orgs = orgs_df[orgs_df['CODE_SOORT'].isin(codes)].copy()
        debug_dataset(sector_orgs, f"{sector.capitalize()} Organizations")
        
        # Clean and standardize data
        sector_orgs = clean_and_standardize(sector_orgs)
        
        # Add education sector information
        sector_orgs['education_sector'] = sector
        
        # Add categorization fields
        sector_orgs['location_type'] = sector_orgs['CODE_SOORT']
        sector_orgs['function_type'] = sector_orgs['CODE_FUNCTIE']
        
        # Determine temporal status
        sector_orgs['temporal_status'] = sector_orgs.apply(
            lambda x: 'Active' if pd.isna(x['DT_UIT_BEDRIJF']) or 
                     x['DT_UIT_BEDRIJF'] > pd.Timestamp.now() else 'Historical',
            axis=1
        )
        
        # Save complete sector data with all codes
        output_file = output_dir / f'{sector}_education_locations.csv'
        sector_orgs.to_csv(output_file, index=False)
        print(f"Saved {len(sector_orgs)} locations to: {output_file}")
        
        # Print summary of saved data
        print("\nSaved data summary:")
        print("Location types distribution:")
        print(sector_orgs['location_type'].value_counts())
        print("\nFunction types distribution:")
        print(sector_orgs['function_type'].value_counts())
        print("\nTemporal status distribution:")
        print(sector_orgs['temporal_status'].value_counts())
       

        df = pd.read_csv("data/processed/final/primary_education_locations.csv")
        print(df.info())
        print(df.head(100))
if __name__ == "__main__":
    merge_datasets()