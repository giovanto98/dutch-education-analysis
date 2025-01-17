import pandas as pd
from pathlib import Path
import os

def process_organizations():
    """Process DUO organizations dataset with location information."""
    
    # Paths
    data_dir = Path(os.getcwd()) / 'data'
    input_file = data_dir / 'raw' / 'ORGANISATIES_20250113.csv'
    output_dir = data_dir / 'processed' / 'intermediate'
    output_dir.mkdir(exist_ok=True, parents=True)
    
    print(f"Processing organizations data from: {input_file}")
    
    # Read data
    df = pd.read_csv(input_file, encoding='utf-8', low_memory=False)
    
    print("\n🔎 Raw CODE_SOORT Distribution BEFORE Processing:")
    print(df['CODE_SOORT'].value_counts())

    # Create clean location fields
    df['full_address'] = df['NAAM_STRAAT_VEST'].fillna('') + ' ' + \
                        df['NR_HUIS_VEST'].fillna('').astype(str) + ' ' + \
                        df['NR_HUIS_TOEV_VEST'].fillna('')
    
    # Convert dates
    date_columns = ['DT_IN_BEDRIJF', 'DT_UIT_BEDRIJF', 'DT_BEGIN_RECORD', 'DT_EINDE_RECORD']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')
    
    # Select relevant columns
    columns = [
        'NR_ADMINISTRATIE', 'CODE_FUNCTIE', 'CODE_SOORT', 
        'NAAM_VOLLEDIG', 'full_address',
        'POSTCODE_VEST', 'NAAM_PLAATS_VEST', 'PROVINCIE_VEST',
        'DT_IN_BEDRIJF', 'DT_UIT_BEDRIJF',
        'CODE_STAND_RECORD', 'IND_OPGEHEVEN'
    ]
    
    clean_df = df[columns].copy()
    
    # Save processed data
    output_file = output_dir / 'clean_organizations.csv'
    clean_df.to_csv(output_file, index=False)
    print(f"Saved processed organizations to: {output_file}")
    
    # Print summary
    print("\nSummary of organizations data:")
    print(f"Total records: {len(clean_df)}")
    print("\nUnique organization types:")
    print(df['CODE_SOORT'].value_counts().head())

if __name__ == "__main__":
    process_organizations()