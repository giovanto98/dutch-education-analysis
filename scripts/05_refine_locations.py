import pandas as pd
import numpy as np
import re
import os
from pathlib import Path
from datetime import datetime

# Set working directory to the script's directory
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
os.chdir(project_root)

class LocationRefiner:
    """Handles refinement of education facility location data."""
    
    def __init__(self, sector):
        """Initialize refiner for a specific education sector."""
        self.sector = sector.lower()
        
        # Define sector-specific categorizations
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
                'MBO': 'Regular MBO Institution',
                'VE': 'Adult Education Center',
                'ILOC': 'International Education Center'
            },
            'higher': {
                'HBO': 'University of Applied Sciences',
                'WO': 'Research University',
                'LOC': 'Higher Education Branch Location'
            }
        }
        
        # Define paths
        self.data_dir = project_root / 'data' / 'processed' / 'final'
        self.base_file = self.data_dir / f'{sector}_education_locations.csv'
        self.geocoded_file = self.data_dir / f'geocoded_{sector}_locations.csv'
        self.output_file = self.data_dir / f'cleaned_{sector}_locations_for_qgis.csv'

    def extract_house_number(self, address):
        """Extract house number from full address."""
        if pd.isna(address):
            return ''
        match = re.search(r'\b\d+\b', str(address))
        return match.group(0) if match else ''

    def extract_postcode(self, address):
        """Extract postcode from full address."""
        if pd.isna(address):
            return ''
        match = re.search(r'\b\d{4}[A-Z]{2}\b', str(address))
        return match.group(0).lower() if match else ''

    def clean_postcode(self, postcode):
        """Normalize postcodes."""
        if pd.isna(postcode):
            return ''
        return str(postcode).strip().replace(' ', '').lower()

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

    def get_most_recent_record(self, group):
        """Get most recent record from a group of duplicate locations."""
        # Convert dates to datetime
        group['DT_IN_BEDRIJF'] = pd.to_datetime(group['DT_IN_BEDRIJF'])
        group['DT_UIT_BEDRIJF'] = pd.to_datetime(group['DT_UIT_BEDRIJF'])
        
        # Prioritize active records
        active_records = group[group['location_status'] == 'Active']
        if not active_records.empty:
            return active_records.iloc[0]
        
        # If no active records, take the most recent one based on start date
        return group.sort_values('DT_IN_BEDRIJF', ascending=False).iloc[0]

    def load_and_merge_data(self):
        """Load and merge base and geocoded datasets if available."""
        print(f"\nLoading {self.sector} education data...")
        
        # Load base dataset
        base_data = pd.read_csv(self.base_file)
        print(f"Loaded {len(base_data)} records from base dataset")
        
        # Prepare base dataset
        base_data['postcode_cleaned'] = base_data['POSTCODE_VEST'].apply(self.clean_postcode)
        base_data['house_number'] = base_data['full_address'].apply(self.extract_house_number)
        
        # Check if geocoded data exists
        if self.geocoded_file.exists():
            print("Found geocoded data, merging...")
            geocoded_data = pd.read_csv(self.geocoded_file)
            geocoded_data['postcode_cleaned'] = geocoded_data['full_address'].apply(self.extract_postcode)
            geocoded_data['house_number'] = geocoded_data['full_address'].apply(self.extract_house_number)
            
            # Merge datasets
            merged_data = pd.merge(
                base_data,
                geocoded_data[['postcode_cleaned', 'house_number', 'latitude', 'longitude']],
                on=['postcode_cleaned', 'house_number'],
                how='left'
            )
            print(f"Merged data contains {len(merged_data)} records")
        else:
            print("No geocoded data found, proceeding with base dataset only")
            merged_data = base_data
            merged_data['latitude'] = np.nan
            merged_data['longitude'] = np.nan
        
        return merged_data

    def process(self):
        """Main processing function."""
        # Load and merge data
        merged_data = self.load_and_merge_data()
        
        # Add location status
        print("\nDetermining location statuses...")
        merged_data['location_status'] = merged_data.apply(self.determine_location_status, axis=1)
        
        # Process temporal information
        print("\nProcessing temporal information...")
        merged_data['DT_IN_BEDRIJF'] = pd.to_datetime(merged_data['DT_IN_BEDRIJF'])
        merged_data['DT_UIT_BEDRIJF'] = pd.to_datetime(merged_data['DT_UIT_BEDRIJF'])
        
        # Remove exact duplicates
        print("\nRemoving exact duplicates...")
        merged_data = merged_data.drop_duplicates()
        
        # Keep most recent/relevant record per location
        print("\nKeeping most relevant records per location...")
        unique_locations = merged_data.groupby(['latitude', 'longitude'], group_keys=False).apply(self.get_most_recent_record)
        unique_locations = unique_locations.reset_index(drop=True)
        
        # Add temporal analysis fields
        print("\nAdding analysis fields...")
        unique_locations['years_of_operation'] = unique_locations.apply(self.calculate_operation_years, axis=1)
        unique_locations['decade_opened'] = unique_locations['DT_IN_BEDRIJF'].apply(self.get_decade)
        unique_locations['decade_closed'] = unique_locations['DT_UIT_BEDRIJF'].apply(self.get_decade)
        unique_locations['facility_type'] = unique_locations.apply(self.categorize_facility, axis=1)
        
        # Add temporal summary
        unique_locations['operation_period'] = unique_locations.apply(
            lambda x: f"{x['DT_IN_BEDRIJF'].strftime('%Y-%m-%d')} to " + 
                     (x['DT_UIT_BEDRIJF'].strftime('%Y-%m-%d') if pd.notna(x['DT_UIT_BEDRIJF']) else 'Present'),
            axis=1
        )
        
        # Prepare final columns
        final_columns = [
            'NR_ADMINISTRATIE', 'CODE_FUNCTIE', 'CODE_SOORT', 'NAAM_VOLLEDIG',
            'full_address', 'postcode_cleaned', 'house_number', 'latitude', 'longitude',
            'POSTCODE_VEST', 'NAAM_PLAATS_VEST', 'PROVINCIE_VEST', 'location_status',
            'facility_type', 'years_of_operation', 'decade_opened', 'decade_closed',
            'operation_period', 'DT_IN_BEDRIJF', 'DT_UIT_BEDRIJF', 'CODE_STAND_RECORD'
        ]
        
        final_data = unique_locations[final_columns]
        
        # Print summary
        print("\nDataset Summary:")
        print(f"Total unique locations: {len(final_data)}")
        print("\nLocation status distribution:")
        print(final_data['location_status'].value_counts())
        print("\nFacility type distribution:")
        print(final_data['facility_type'].value_counts())
        print("\nDecade opened distribution:")
        print(final_data['decade_opened'].value_counts().sort_index())
        print("\nAverage years of operation: {:.1f}".format(final_data['years_of_operation'].mean()))
        
        # Save the final dataset
        final_data.to_csv(self.output_file, index=False)
        print(f"\nSaved QGIS-ready dataset to: {self.output_file}")
        
        return final_data

def main():
    """Main execution function."""
    sectors = ['primary', 'secondary', 'vocational', 'higher']
    
    for sector in sectors:
        print(f"\n{'='*80}")
        print(f"Processing {sector.upper()} education sector")
        print(f"{'='*80}")
        
        try:
            refiner = LocationRefiner(sector)
            final_data = refiner.process()
            if final_data is not None:
                print(f"Successfully processed {sector} education locations")
        except Exception as e:
            print(f"Error processing {sector} sector: {e}")

if __name__ == "__main__":
    main()