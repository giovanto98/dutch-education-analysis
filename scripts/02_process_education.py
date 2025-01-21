import pandas as pd
import numpy as np
from pathlib import Path
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')

class EducationProcessor:
    """Process and validate DUO education program data."""
    
    # Sector mapping with Dutch names
    SECTOR_MAPPING = {
        'PO': 'primary',      # Primair onderwijs
        'VO': 'secondary',    # Voortgezet onderwijs
        'MBO': 'vocational',  # Middelbaar beroepsonderwijs
        'HO': 'higher',       # Hoger onderwijs
        'SO': 'special',      # Speciaal onderwijs (will be merged with primary)
        'VE': 'adult'         # Volwasseneneducatie (will be merged with vocational)
    }
    
    def __init__(self, data_dir: Path):
        """Initialize with data directory path."""
        self.data_dir = data_dir
        self.input_file = data_dir / 'raw' / 'basisgegevens_opleidingen_20240520.csv'
        self.output_dir = data_dir / 'processed' / 'intermediate'
        self.output_dir.mkdir(exist_ok=True, parents=True)

    def load_data(self) -> pd.DataFrame:
        """Load and perform initial data validation."""
        logging.info(f"Loading education data from: {self.input_file}")
        
        # Read data with correct encoding (cp1252 for Dutch characters)
        df = pd.read_csv(self.input_file, encoding='cp1252', low_memory=False)
        logging.info(f"Loaded {len(df)} records")
        
        # Log initial distribution
        logging.info("\nInitial sector distribution:")
        logging.info(df['Onderwijssector'].value_counts().to_string())
        
        return df
    
    def clean_and_map_sectors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and map education sectors."""
        # Create education_sector mapping
        df['education_sector'] = df['Onderwijssector'].map(self.SECTOR_MAPPING)
        
        # Handle special cases
        # Move special education to primary
        df.loc[df['Onderwijssector'] == 'SO', 'education_sector'] = 'primary'
        # Move adult education to vocational
        df.loc[df['Onderwijssector'] == 'VE', 'education_sector'] = 'vocational'
        
        # Log mapping results
        logging.info("\nSector mapping results:")
        mapping_results = pd.crosstab(df['Onderwijssector'], df['education_sector'])
        logging.info("\n" + mapping_results.to_string())
        
        return df

    def process_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and validate date fields."""
        date_columns = ['Begindatum', 'Einddatum', 'Datum laatste wijziging']
        
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')
        
        # Add active status
        current_date = pd.Timestamp.now()
        df['is_active'] = df['Einddatum'].isna() | (df['Einddatum'] > current_date)
        
        return df

    def add_education_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add education type classification."""
        # Create education type mapping based on Onderwijstype and Soort opleiding
        df['education_type'] = df['Onderwijstype']
        
        # Add training type for vocational education
        mask = df['education_sector'] == 'vocational'
        df.loc[mask, 'education_type'] = df.loc[mask, 'Soort opleiding MBO']
        
        # Log education types by sector
        logging.info("\nEducation types by sector:")
        type_dist = pd.crosstab(df['education_sector'], df['education_type'])
        logging.info("\n" + type_dist.to_string())
        
        return df

    def select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and rename relevant columns."""
        columns = [
            'Onderwijssector',
            'education_sector',
            'education_type',
            'Onderwijstype',
            'Opleidingscode',
            'Naam',
            'Omschrijving',
            'Begindatum',
            'Einddatum',
            'is_active',
            'Niveau',
            'Leerweg'
        ]
        
        return df[columns].copy()

    def process(self) -> pd.DataFrame:
        """Main processing function."""
        # Load data
        df = self.load_data()
        
        # Process
        df = self.clean_and_map_sectors(df)
        df = self.process_dates(df)
        df = self.add_education_type(df)
        df = self.select_columns(df)
        
        # Save processed data
        output_file = self.output_dir / 'clean_education.csv'
        df.to_csv(output_file, index=False)
        logging.info(f"\nSaved processed data to: {output_file}")
        
        # Log summary statistics
        self.log_summary_statistics(df)
        
        return df

    def log_summary_statistics(self, df: pd.DataFrame):
        """Log summary statistics about the processed data."""
        logging.info("\nSummary Statistics:")
        logging.info(f"Total records: {len(df)}")
        
        logging.info("\nRecords by education sector:")
        sector_counts = df.groupby('education_sector').size()
        logging.info(sector_counts.to_string())
        
        logging.info("\nActive vs Historical records:")
        status_counts = df.groupby(['education_sector', 'is_active']).size()
        logging.info(status_counts.to_string())
        
        logging.info("\nEducation types per sector:")
        type_counts = df.groupby(['education_sector', 'education_type']).size()
        logging.info(type_counts.to_string())

def main():
    """Main execution function."""
    # Set up paths
    project_root = Path(os.getcwd())
    processor = EducationProcessor(project_root / 'data')
    
    # Process data
    df = processor.process()
    return df

if __name__ == "__main__":
    main()