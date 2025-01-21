import pandas as pd
import numpy as np
import logging
from pathlib import Path
import os
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class LocationRefiner:
    """Combines and refines education facility location data with temporal analysis."""
    
    def __init__(self, sector):
        """Initialize refiner for a specific education sector."""
        self.sector = sector.lower()
        
        # Set up project paths
        self.script_dir = Path(__file__).resolve().parent
        self.project_root = self.script_dir.parent
        
        # Define directory structure
        self.data_dir = self.project_root / 'data'
        self.final_dir = self.data_dir / 'processed' / 'final'
        self.geocoded_dir = self.data_dir / 'geocoded'
        
        # Define input/output files
        self.base_file = self.final_dir / f'{sector}_education_locations.csv'
        self.geocoded_file = self.geocoded_dir / f'geocoded_{sector}_locations.csv'
        self.output_file = self.final_dir / f'cleaned_{sector}_for_qgis.csv'
        
        # Define facility categories
        self.facility_categories = {
            'primary': {
                'BAS': 'Regular Primary School',
                'SBAS': 'Special Primary Education',
                'SPEC': 'Special Education',
                'VST': 'Primary Branch Location'
            },
            'secondary': {
                'VOS': 'Regular Secondary School',
                'PROS': 'Practical Education',
                'AOCV': 'Agricultural Education',
                'VSTS': 'Secondary Technical School',
                'VSTZ': 'Secondary Branch Location'
            },
            'vocational': {
                'ROC': 'Regional Education Center',
                'ROCV': 'Specialized Regional Center',
                'VAK': 'Vocational School',
                'VAKV': 'Advanced Vocational',
                'ILOC': 'International Location'
            },
            'higher': {
                'HBOS': 'University of Applied Sciences',
                'UNIV': 'Research University',
                'LOC': 'Higher Education Branch',
                'ULOC': 'University Branch'
            }
        }

    def calculate_operation_years(self, row):
        """Calculate years of operation for a location."""
        start_date = pd.to_datetime(row['DT_IN_BEDRIJF'])
        if pd.isna(row['DT_UIT_BEDRIJF']):
            end_date = pd.Timestamp.now()
        else:
            end_date = pd.to_datetime(row['DT_UIT_BEDRIJF'])
        
        years = (end_date - start_date).days / 365.25
        return round(years, 1)

    def get_decade(self, date):
        """Get the decade from a date (e.g., 1990s, 2000s)."""
        if pd.isna(date):
            return None
        year = pd.to_datetime(date).year
        decade = (year // 10) * 10
        return f"{decade}s"

    def categorize_facility(self, row):
        """Categorize facility based on CODE_SOORT and sector-specific categories."""
        code = row['CODE_SOORT']
        categories = self.facility_categories.get(self.sector, {})
        return categories.get(code, 'Other')

    def determine_location_status(self, row):
        """Determine if location is active, closed, or historical."""
        try:
            start_date = pd.to_datetime(row['DT_IN_BEDRIJF'])
            end_date = pd.to_datetime(row['DT_UIT_BEDRIJF']) if pd.notna(row['DT_UIT_BEDRIJF']) else pd.NaT
            current_date = pd.Timestamp.now()
            
            if pd.isna(end_date):
                return 'Active'
            elif end_date > current_date:
                return 'Active'
            else:
                return 'Closed'
        except:
            return 'Unknown'

    def load_and_merge_data(self):
        """Load and merge base and geocoded datasets if available."""
        logging.info(f"\nLoading {self.sector} education data...")
        
        # Load base dataset
        base_data = pd.read_csv(self.base_file)
        logging.info(f"Loaded {len(base_data)} records from base dataset")
        
        # Check if geocoded data exists
        if self.geocoded_file.exists() and self.sector != 'primary':
            logging.info("Found geocoded data, merging...")
            geocoded_data = pd.read_csv(self.geocoded_file)
            
            # Merge datasets
            merged_data = base_data.merge(
                geocoded_data[['full_address', 'latitude', 'longitude']],
                on='full_address',
                how='left'
            )
            logging.info(f"Merged data contains {len(merged_data)} records")
        else:
            logging.info("No geocoded data available/needed")
            merged_data = base_data
            if self.sector != 'primary':
                merged_data['latitude'] = np.nan
                merged_data['longitude'] = np.nan
        
        return merged_data

    def add_temporal_analysis(self, df):
        """Add temporal analysis fields to dataset."""
        logging.info("Adding temporal analysis fields...")
        
        # Process dates
        df['DT_IN_BEDRIJF'] = pd.to_datetime(df['DT_IN_BEDRIJF'])
        df['DT_UIT_BEDRIJF'] = pd.to_datetime(df['DT_UIT_BEDRIJF'])
        
        # Add analysis fields
        df['years_of_operation'] = df.apply(self.calculate_operation_years, axis=1)
        df['decade_opened'] = df['DT_IN_BEDRIJF'].apply(self.get_decade)
        df['decade_closed'] = df['DT_UIT_BEDRIJF'].apply(self.get_decade)
        df['facility_type'] = df.apply(self.categorize_facility, axis=1)
        df['location_status'] = df.apply(self.determine_location_status, axis=1)
        
        # Add temporal summary
        df['operation_period'] = df.apply(
            lambda x: f"{x['DT_IN_BEDRIJF'].strftime('%Y-%m-%d')} to " + 
                     (x['DT_UIT_BEDRIJF'].strftime('%Y-%m-%d') if pd.notna(x['DT_UIT_BEDRIJF']) else 'Present'),
            axis=1
        )
        
        return df

    def process(self):
        """Main processing function."""
        try:
            # Load and merge data
            merged_data = self.load_and_merge_data()
            
            # Add temporal analysis
            final_data = self.add_temporal_analysis(merged_data)
            
            # Prepare final columns
            keep_columns = [
                'NR_ADMINISTRATIE', 'CODE_FUNCTIE', 'CODE_SOORT', 'NAAM_VOLLEDIG',
                'full_address', 'POSTCODE_VEST', 'NAAM_PLAATS_VEST', 'PROVINCIE_VEST',
                'location_status', 'facility_type', 'years_of_operation',
                'decade_opened', 'decade_closed', 'operation_period',
                'DT_IN_BEDRIJF', 'DT_UIT_BEDRIJF'
            ]
            
            # Add geocoding columns if available
            if 'latitude' in final_data.columns:
                keep_columns.extend(['latitude', 'longitude'])
            
            final_data = final_data[keep_columns]
            
            # Save the final dataset
            final_data.to_csv(self.output_file, index=False)
            logging.info(f"\nSaved QGIS-ready dataset to: {self.output_file}")
            
            # Log summary statistics
            self.log_summary_statistics(final_data)
            
            return final_data
            
        except Exception as e:
            logging.error(f"Error during processing: {str(e)}", exc_info=True)
            raise

    def log_summary_statistics(self, df):
        """Log summary statistics about the refined data."""
        logging.info("\nDataset Summary:")
        logging.info(f"Total records: {len(df)}")
        
        logging.info("\nFacility type distribution:")
        logging.info(df['facility_type'].value_counts().to_string())
        
        logging.info("\nLocation status distribution:")
        logging.info(df['location_status'].value_counts().to_string())
        
        logging.info("\nDecade opened distribution:")
        logging.info(df['decade_opened'].value_counts().sort_index().to_string())
        
        if 'latitude' in df.columns:
            geocoded = df['latitude'].notna().sum()
            logging.info(f"\nGeocoding coverage: {geocoded}/{len(df)} ({geocoded/len(df)*100:.1f}%)")
        
        logging.info(f"\nAverage years of operation: {df['years_of_operation'].mean():.1f}")

def main():
    """Main execution function."""
    sectors = ['primary', 'secondary', 'vocational', 'higher']
    
    for sector in sectors:
        logging.info(f"\n{'='*80}")
        logging.info(f"Processing {sector.upper()} education sector")
        logging.info(f"{'='*80}")
        
        try:
            refiner = LocationRefiner(sector)
            final_data = refiner.process()
            if final_data is not None:
                logging.info(f"Successfully processed {sector} education locations")
        except Exception as e:
            logging.error(f"Error processing {sector} sector: {e}")

if __name__ == "__main__":
    main()