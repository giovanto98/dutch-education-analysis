import pandas as pd
import numpy as np
from pathlib import Path
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')

class OrganizationProcessor:
    """Process and validate DUO organization data."""
    
    # Valid institution codes by sector
    VALID_CODES = {
        'higher': {
            'main': ['HBOS', 'UNIV'],
            'locations': ['LOC', 'ULOC'],
            'international': ['ILOC']
        },
        'vocational': {
            'main': ['ROC', 'ROCV'],
            'specialized': ['VAK', 'VAKV']
        },
        'secondary': {
            'main': ['VOS'],
            'specialized': ['PROS', 'AOCV'],
            'locations': ['VSTS', 'VSTZ']
        },
        'primary': {
            'main': ['BAS', 'SBAS', 'SPEC'],
            'locations': ['VST']
        }
    }
    
    # Function types
    FUNCTION_TYPES = {
        'U': 'Institution',
        'D': 'Location',
        'B': 'Board',
        'O': 'Organization',
        'S': 'Collaboration'
    }

    def __init__(self, data_dir: Path):
        """Initialize with data directory path."""
        self.data_dir = data_dir
        self.input_file = data_dir / 'raw' / 'ORGANISATIES_20250113.csv'
        self.output_dir = data_dir / 'processed' / 'intermediate'
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Create flat list of all valid codes
        self.all_valid_codes = []
        for sector in self.VALID_CODES.values():
            for code_list in sector.values():
                self.all_valid_codes.extend(code_list)
    
    def load_data(self) -> pd.DataFrame:
        """Load and perform initial data validation."""
        logging.info(f"Loading data from: {self.input_file}")
        
        df = pd.read_csv(self.input_file, encoding='utf-8', low_memory=False)
        logging.info(f"Loaded {len(df)} records")
        
        # Log initial code distribution
        logging.info("\nInitial CODE_SOORT distribution:")
        logging.info(df['CODE_SOORT'].value_counts().to_string())
        
        return df
    
    def clean_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize address information."""
        # Create clean location fields
        df['full_address'] = (
            df['NAAM_STRAAT_VEST'].fillna('').astype(str).str.strip() + ' ' +
            df['NR_HUIS_VEST'].fillna('').astype(str).str.strip() + ' ' +
            df['NR_HUIS_TOEV_VEST'].fillna('').astype(str).str.strip()
        )
        
        # Clean postcode
        df['POSTCODE_VEST'] = (
            df['POSTCODE_VEST']
            .fillna('')
            .astype(str)
            .str.strip()
            .str.upper()
            .str.replace(' ', '')
        )
        
        return df
    
    def process_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and validate date fields."""
        date_columns = [
            'DT_IN_BEDRIJF', 
            'DT_UIT_BEDRIJF', 
            'DT_BEGIN_RECORD', 
            'DT_EINDE_RECORD'
        ]
        
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce')
        
        # Add active status
        current_date = pd.Timestamp.now()
        df['is_active'] = (
            (df['DT_UIT_BEDRIJF'].isna()) | 
            (df['DT_UIT_BEDRIJF'] > current_date)
        )
        
        return df
    
    def validate_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate institution codes and flag invalid ones."""
        # Check for unknown codes
        unknown_codes = set(df['CODE_SOORT'].unique()) - set(self.all_valid_codes)
        if unknown_codes:
            logging.warning(f"Found unknown institution codes: {unknown_codes}")
        
        # Add sector and type information
        df['education_sector'] = ''
        df['institution_type'] = ''
        
        for sector, types in self.VALID_CODES.items():
            for type_name, codes in types.items():
                mask = df['CODE_SOORT'].isin(codes)
                df.loc[mask, 'education_sector'] = sector
                df.loc[mask, 'institution_type'] = type_name
        
        # Log code distribution by sector
        logging.info("\nCode distribution by sector:")
        for sector in df['education_sector'].unique():
            if pd.isna(sector) or sector == '':
                continue
            sector_data = df[df['education_sector'] == sector]
            logging.info(f"\n{sector.upper()} Education:")
            logging.info(sector_data.groupby(['CODE_SOORT', 'CODE_FUNCTIE']).size())
        
        return df
    
    def select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and rename relevant columns."""
        columns = [
            'NR_ADMINISTRATIE', 
            'CODE_FUNCTIE',
            'CODE_SOORT',
            'education_sector',
            'institution_type',
            'NAAM_VOLLEDIG',
            'full_address',
            'POSTCODE_VEST',
            'NAAM_PLAATS_VEST',
            'PROVINCIE_VEST',
            'DT_IN_BEDRIJF',
            'DT_UIT_BEDRIJF',
            'is_active',
            'CODE_STAND_RECORD'
        ]
        
        return df[columns].copy()
    
    def process(self) -> pd.DataFrame:
        """Main processing function."""
        # Load data
        df = self.load_data()
        
        # Clean and process
        df = self.clean_addresses(df)
        df = self.process_dates(df)
        df = self.validate_codes(df)
        df = self.select_columns(df)
        
        # Save processed data
        output_file = self.output_dir / 'clean_organizations.csv'
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
        
        logging.info("\nMissing values in key fields:")
        key_fields = ['NR_ADMINISTRATIE', 'CODE_SOORT', 'POSTCODE_VEST']
        missing_counts = df[key_fields].isna().sum()
        logging.info(missing_counts.to_string())

def main():
    """Main execution function."""
    # Set up paths
    project_root = Path(os.getcwd())
    processor = OrganizationProcessor(project_root / 'data')
    
    # Process data
    df = processor.process()
    return df

if __name__ == "__main__":
    main()