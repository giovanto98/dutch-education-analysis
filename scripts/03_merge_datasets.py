import pandas as pd
import numpy as np
from pathlib import Path
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')

class DatasetMerger:
    """Merge and validate organizations and education datasets."""
    
    def __init__(self, project_root: Path):
        """Initialize with project root path."""
        self.data_dir = project_root / 'data'
        self.input_dir = self.data_dir / 'processed' / 'intermediate'
        self.output_dir = self.data_dir / 'processed' / 'final'
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Define sector codes
        self.sector_codes = {
            'primary': {
                'main': ['BAS', 'SBAS', 'SPEC'],
                'locations': ['VST']
            },
            'secondary': {
                'main': ['VOS', 'PROS', 'AOCV'],
                'locations': ['VSTS', 'VSTZ']
            },
            'vocational': {
                'main': ['ROC', 'ROCV', 'VAK', 'VAKV'],
                'shared_locations': ['ILOC']
            },
            'higher': {
                'main': ['HBOS', 'UNIV'],
                'locations': ['LOC', 'ULOC'],
                'shared_locations': ['ILOC']
            }
        }
    
    def load_data(self):
        """Load processed organization and education data."""
        orgs_file = self.input_dir / 'clean_organizations.csv'
        edu_file = self.input_dir / 'clean_education.csv'
        
        logging.info(f"Loading data from:\n  {orgs_file}\n  {edu_file}")
        
        orgs_df = pd.read_csv(orgs_file)
        edu_df = pd.read_csv(edu_file)
        
        # Convert date columns
        date_cols = ['DT_IN_BEDRIJF', 'DT_UIT_BEDRIJF']
        for df in [orgs_df, edu_df]:
            for col in df.columns:
                if any(date_str in col.lower() for date_str in ['date', 'dt_', 'datum']):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return orgs_df, edu_df
    
    def process_sector(self, orgs_df: pd.DataFrame, edu_df: pd.DataFrame, 
                      sector: str) -> pd.DataFrame:
        """Process a specific education sector."""
        logging.info(f"\nProcessing {sector.upper()} education sector")
        
        # Get relevant codes for this sector
        codes = self.sector_codes[sector]
        all_codes = []
        for code_list in codes.values():
            all_codes.extend(code_list)
        
        # Filter organizations
        sector_orgs = orgs_df[orgs_df['CODE_SOORT'].isin(all_codes)].copy()
        
        # Filter education data
        sector_edu = edu_df[edu_df['education_sector'] == sector].copy()
        
        logging.info(f"Found {len(sector_orgs)} organizations and {len(sector_edu)} education programs")
        
        # Add categorization
        sector_orgs['location_type'] = sector_orgs['CODE_SOORT']
        sector_orgs['function_type'] = sector_orgs['CODE_FUNCTIE']
        sector_orgs['education_sector'] = sector
        
        # Add education type mapping
        type_mapping = self.get_education_type_mapping(sector_edu, sector)
        sector_orgs['education_type'] = sector_orgs['CODE_SOORT'].map(type_mapping)
        
        # Determine temporal status
        current_date = pd.Timestamp.now()
        sector_orgs['temporal_status'] = sector_orgs.apply(
            lambda x: 'Active' if pd.isna(x['DT_UIT_BEDRIJF']) or 
                     x['DT_UIT_BEDRIJF'] > current_date else 'Historical',
            axis=1
        )
        
        # Calculate operational metrics
        sector_orgs['years_of_operation'] = sector_orgs.apply(
            self.calculate_operation_years, axis=1
        )
        
        # Remove exact duplicates
        sector_orgs = sector_orgs.drop_duplicates()
        
        # Group by location and keep most relevant record
        if 'latitude' in sector_orgs.columns and 'longitude' in sector_orgs.columns:
            location_cols = ['latitude', 'longitude']
            sector_orgs = sector_orgs.groupby(location_cols, as_index=False).apply(
                self.get_most_relevant_record
            )
        
        return sector_orgs
    
    def get_education_type_mapping(self, edu_df: pd.DataFrame, sector: str) -> dict:
        """Create mapping between organization codes and education types."""
        if sector == 'primary':
            return {
                'BAS': 'Regular Primary School',
                'SBAS': 'Special Primary Education',
                'SPEC': 'Special Education',
                'VST': 'Primary Branch Location'
            }
        elif sector == 'secondary':
            return {
                'VOS': 'Regular Secondary School',
                'PROS': 'Practical Education',
                'AOCV': 'Agricultural Education',
                'VSTS': 'Secondary Branch Location',
                'VSTZ': 'Secondary Care Location'
            }
        elif sector == 'vocational':
            return {
                'ROC': 'Regional Education Center',
                'ROCV': 'Regional Education Branch',
                'VAK': 'Vocational School',
                'VAKV': 'Vocational Branch',
                'ILOC': 'International Location'
            }
        else:  # higher
            return {
                'HBOS': 'University of Applied Sciences',
                'UNIV': 'Research University',
                'LOC': 'Higher Education Location',
                'ULOC': 'University Location',
                'ILOC': 'International Location'
            }
    
    def calculate_operation_years(self, row):
        """Calculate years of operation for a location."""
        start_date = row['DT_IN_BEDRIJF']
        if pd.isna(row['DT_UIT_BEDRIJF']):
            end_date = pd.Timestamp.now()
        else:
            end_date = row['DT_UIT_BEDRIJF']
        
        if pd.isna(start_date):
            return np.nan
        
        years = (end_date - start_date).days / 365.25
        return round(years, 1)
    
    def get_most_relevant_record(self, group):
        """Select most relevant record from a group."""
        # Prioritize active records
        active_records = group[group['temporal_status'] == 'Active']
        if not active_records.empty:
            return active_records.iloc[0]
        
        # If no active records, take most recent based on start date
        return group.sort_values('DT_IN_BEDRIJF', ascending=False).iloc[0]
    
    def process(self):
        """Main processing function."""
        # Load data
        orgs_df, edu_df = self.load_data()
        
        # Process each sector
        sectors = ['primary', 'secondary', 'vocational', 'higher']
        
        for sector in sectors:
            # Process sector
            sector_df = self.process_sector(orgs_df, edu_df, sector)
            
            # Save sector data
            output_file = self.output_dir / f'{sector}_education_locations.csv'
            sector_df.to_csv(output_file, index=False)
            
            # Log summary
            self.log_sector_summary(sector_df, sector)
    
    def log_sector_summary(self, df: pd.DataFrame, sector: str):
        """Log summary statistics for a sector."""
        logging.info(f"\nSummary for {sector.upper()} education:")
        logging.info(f"Total locations: {len(df)}")
        
        logging.info("\nLocation type distribution:")
        logging.info(df['location_type'].value_counts().to_string())
        
        logging.info("\nFunction type distribution:")
        logging.info(df['function_type'].value_counts().to_string())
        
        logging.info("\nTemporal status distribution:")
        logging.info(df['temporal_status'].value_counts().to_string())
        
        logging.info(f"\nAverage years of operation: {df['years_of_operation'].mean():.1f}")

def main():
    """Main execution function."""
    project_root = Path(os.getcwd())
    merger = DatasetMerger(project_root)
    merger.process()

if __name__ == "__main__":
    main()