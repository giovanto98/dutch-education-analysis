import pandas as pd
import time
import os
from pathlib import Path
from geopy.geocoders import GoogleV3
from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class LocationGeocoder:
    """Handles geocoding for education facility locations."""
    
    def __init__(self, sector):
        """Initialize geocoder for a specific education sector."""
        self.sector = sector.lower()
        self.api_key = os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("API Key not found! Please set GOOGLE_API_KEY in .env file")
        
        # Set up project paths
        self.script_dir = Path(__file__).resolve().parent
        self.project_root = self.script_dir.parent
        
        # Define input/output paths
        self.data_dir = self.project_root / 'data'
        self.processed_dir = self.data_dir / 'processed'
        self.final_dir = self.processed_dir / 'final'
        self.geocoded_dir = self.data_dir / 'geocoded'
        
        # Ensure geocoded directory exists
        self.geocoded_dir.mkdir(exist_ok=True, parents=True)
        
        # Define input/output files
        self.input_file = self.final_dir / f'{self.sector}_education_locations.csv'
        self.output_file = self.geocoded_dir / f'geocoded_{self.sector}_locations.csv'

    def load_data(self):
        """Load and validate the dataset."""
        try:
            data = pd.read_csv(self.input_file)
            print(f"Loaded {len(data)} records from {self.input_file}")
            return data
        except Exception as e:
            print(f"Error loading data: {e}")
            return None

    def prepare_addresses(self, data):
        """Prepare addresses for geocoding."""
        if data.empty:
            print("Empty dataset. Geocoding will not proceed.")
            return data, False
        
        # Clean and combine address fields
        data['full_address'] = (
            data['full_address'].fillna('') + ", " +
            data['POSTCODE_VEST'].fillna('') + ", " +
            data['NAAM_PLAATS_VEST'].fillna('') + ", " +
            data['PROVINCIE_VEST'].fillna('') + ", Netherlands"
        )
        
        return data, True

    def geocode_with_retries(self, geolocator, address, max_retries=3):
        """Geocode address with retry logic."""
        for attempt in range(max_retries):
            try:
                location = geolocator.geocode(address)
                if location:
                    return location
            except GeocoderTimedOut:
                if attempt < max_retries - 1:
                    print(f"Timeout for {address}. Retrying...")
                    time.sleep(1)
            except GeocoderQuotaExceeded:
                print("API quota exceeded. Waiting 60 seconds...")
                time.sleep(60)
            except Exception as e:
                print(f"Error geocoding {address}: {e}")
                return None
        return None

    def geocode_dataset(self, data, batch_size=100):
        """Geocode all addresses in dataset."""
        geolocator = GoogleV3(api_key=self.api_key)
        geocoded_results = []
        total_addresses = len(data)
        
        print(f"\nStarting geocoding for {total_addresses} addresses...")
        start_time = time.time()
        
        for index, row in data.iterrows():
            address = row['full_address']
            location = self.geocode_with_retries(geolocator, address)
            
            geocoded_results.append({
                'full_address': address,
                'latitude': location.latitude if location else None,
                'longitude': location.longitude if location else None
            })
            
            if (index + 1) % batch_size == 0:
                completion = (index + 1) / total_addresses * 100
                print(f"Progress: {completion:.1f}% ({index + 1}/{total_addresses})")
        
        print(f"\nGeocoding completed in {time.time() - start_time:.2f} seconds")
        return pd.DataFrame(geocoded_results)

    def process(self):
        """Main processing function."""
        print(f"\nProcessing {self.sector} education locations...")
        
        # Load and prepare data
        data = self.load_data()
        if data is None:
            return False
        
        data, is_valid = self.prepare_addresses(data)
        if not is_valid:
            return False
        
        # Perform geocoding
        geocoded_data = self.geocode_dataset(data)
        
        # Save results
        if not geocoded_data.empty:
            geocoded_data.to_csv(self.output_file, index=False)
            print(f"\nSaved geocoded data to: {self.output_file}")
            print(f"Successfully geocoded: {geocoded_data['latitude'].notna().sum()} locations")
            print(f"Failed to geocode: {geocoded_data['latitude'].isna().sum()} locations")
            return True
        
        return False

def main():
    """Main execution function."""
    sectors = ['higher', 'secondary', 'vocational']  # Primary excluded as per requirements
    
    for sector in sectors:
        print(f"\n{'='*80}")
        print(f"Processing {sector.upper()} education sector")
        print(f"{'='*80}")
        
        try:
            geocoder = LocationGeocoder(sector)
            success = geocoder.process()
            if success:
                print(f"Successfully processed {sector} education locations")
            else:
                print(f"Failed to process {sector} education locations")
        except Exception as e:
            print(f"Error processing {sector} sector: {e}")

if __name__ == "__main__":
    main()