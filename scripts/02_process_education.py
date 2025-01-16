import pandas as pd
from pathlib import Path
import os

def process_education_data():
    """Process DUO education programs dataset."""
    
    # Paths
    data_dir = Path(os.getcwd()) / 'data'
    input_file = data_dir / 'raw' / 'basisgegevens_opleidingen_20240520.csv'
    output_dir = data_dir / 'processed' / 'intermediate'
    output_dir.mkdir(exist_ok=True, parents=True)
    
    print(f"Processing education data from: {input_file}")
    
    # Read data with correct encoding
    df = pd.read_csv(input_file, encoding='cp1252', low_memory=False)
    
    # Convert dates
    date_columns = ['Begindatum', 'Einddatum', 'Datum laatste wijziging']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')
    
    # Create sector groupings
    sector_mapping = {
        'PO': 'primary',
        'VO': 'secondary',
        'MBO': 'vocational',
        'HO': 'higher'
    }
    
    df['education_sector'] = df['Onderwijssector'].map(sector_mapping)
    
    # Save processed data
    output_file = output_dir / 'clean_education.csv'
    df.to_csv(output_file, index=False)
    print(f"Saved processed education data to: {output_file}")
    
    # Print summary
    print("\nSummary of education data:")
    print(f"Total records: {len(df)}")
    print("\nEducation sectors distribution:")
    print(df['education_sector'].value_counts())

if __name__ == "__main__":
    process_education_data()