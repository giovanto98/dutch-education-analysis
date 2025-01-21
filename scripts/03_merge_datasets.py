import pandas as pd
from pathlib import Path
import os
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DatasetMerger:
    """Handles initial merging and cleaning of education datasets."""
    
    def __init__(self):
        """Initialize with project directory structure."""
        # Set working directory to the project root
        self.script_dir = Path(__file__).resolve().parent
        self.project_root = self.script_dir.parent
        os.chdir(self.project_root)
        
        # Define paths
        self.data_dir = self.project_root / 'data'
        self.processed_dir = self.data_dir / 'processed'
        self.intermediate_dir = self.processed_dir / 'intermediate'
        self.final_dir = self.processed_dir / 'final'
        
        # Ensure directories exist
        self.final_dir.mkdir(exist_ok=True, parents=True)
        
        # Define sector codes
        self.sector_codes = {
            'primary': ['BAS', 'SBAS', 'SPEC', 'VST'],
            'secondary': ['VOS', 'PROS', 'AOCV', 'VSTS', 'VSTZ'],
            'vocational': ['ROC', 'ROCV', 'VAK', 'VAKV', 'ILOC'],
            'higher': ['HBOS', 'UNIV', 'LOC', 'ULOC']
        }

    def clean_and_standardize(self, df):
        """Clean and standardize dataset columns."""
        # Convert dates
        date_columns = ['DT_IN_BEDRIJF', 'DT_UIT_BEDRIJF', 'DT_BEGIN_RECORD', 'DT_EINDE_RECORD']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Create full address if not present
        if 'full_address' not in df.columns:
            df['full_address'] = (
                df['NAAM_STRAAT_VEST'].astype(str).fillna('') + ' ' +
                df['NR_HUIS_VEST'].astype(str).fillna('') + ' ' +
                df['NR_HUIS_TOEV_VEST'].astype(str).fillna('')
            ).str.strip()
        
        # Clean postcode
        if 'POSTCODE_VEST' in df.columns:
            df['postcode_cleaned'] = (
                df['POSTCODE_VEST']
                .astype(str)
                .fillna('')
                .str.lower()
                .str.replace(' ', '')
            )
        
        return df

    def remove_duplicates(self, df):
        """Remove duplicate records based on address and maintain most recent."""
        if df.empty:
            return df
        
        # Sort by date and active status (keep most recent)
        df = df.sort_values(
            ['DT_IN_BEDRIJF', 'DT_UIT_BEDRIJF'],
            ascending=[False, True]
        )
        
        # Drop duplicates keeping first (most recent) occurrence
        return df.drop_duplicates(
            subset=['full_address', 'postcode_cleaned'],
            keep='first'
        )

    def process_sector(self, sector, df_orgs):
        """Process data for a specific education sector."""
        logging.info(f"\nProcessing {sector} education sector")
        
        # Filter for sector
        sector_codes = self.sector_codes[sector]
        sector_data = df_orgs[df_orgs['CODE_SOORT'].isin(sector_codes)].copy()
        
        if sector_data.empty:
            logging.warning(f"No data found for {sector} sector")
            return None
        
        # Clean and standardize
        sector_data = self.clean_and_standardize(sector_data)
        
        # Remove duplicates
        initial_count = len(sector_data)
        sector_data = self.remove_duplicates(sector_data)
        duplicate_count = initial_count - len(sector_data)
        
        logging.info(f"Removed {duplicate_count} duplicates from {sector} sector")
        
        # Add sector information
        sector_data['education_sector'] = sector
        sector_data['location_type'] = sector_data['CODE_SOORT']
        sector_data['function_type'] = sector_data['CODE_FUNCTIE']
        
        # Add temporal status
        sector_data['temporal_status'] = sector_data.apply(
            lambda x: 'Active' if pd.isna(x['DT_UIT_BEDRIJF']) or 
                     x['DT_UIT_BEDRIJF'] > pd.Timestamp.now() else 'Historical',
            axis=1
        )
        
        return sector_data

    def process(self):
        """Main processing function."""
        try:
            # Load organization data
            orgs_file = self.intermediate_dir / 'clean_organizations.csv'
            df_orgs = pd.read_csv(orgs_file)
            logging.info(f"Loaded {len(df_orgs)} organization records")
            
            # Process each sector
            for sector in self.sector_codes.keys():
                sector_data = self.process_sector(sector, df_orgs)
                
                if sector_data is not None:
                    # Save to final directory
                    output_file = self.final_dir / f'{sector}_education_locations.csv'
                    sector_data.to_csv(output_file, index=False)
                    logging.info(f"Saved {len(sector_data)} {sector} locations to {output_file}")
                    
                    # Log summary
                    logging.info("\nSector Summary:")
                    logging.info(f"Location types: {sector_data['location_type'].value_counts().to_dict()}")
                    logging.info(f"Function types: {sector_data['function_type'].value_counts().to_dict()}")
                    logging.info(f"Temporal status: {sector_data['temporal_status'].value_counts().to_dict()}")
        
        except Exception as e:
            logging.error(f"Error during processing: {str(e)}", exc_info=True)
            raise

def main():
    """Main execution function."""
    try:
        merger = DatasetMerger()
        merger.process()
        logging.info("Dataset merging completed successfully")
    except Exception as e:
        logging.error(f"Error during execution: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()