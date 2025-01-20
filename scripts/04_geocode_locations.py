import pandas as pd
import time
import os
from pathlib import Path
from geopy.geocoders import GoogleV3
from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set working directory to the script's directory
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
os.chdir(project_root)

class LocationGeocoder:
    """Handles geocoding for education facility locations."""
    
    def __init__(self, sector):
        """Initialize geocoder for a specific education sector."""
        self.sector = sector.lower()
        self.api_key = os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("API Key not found! Please set GOOGLE_API_KEY in the .env file.")
        
        # Define paths
        self.input_file = f"data/processed/final/{self.sector}_education_locations.csv"
        self.output_file = f"data/processed/final/geocoded_{self.sector}_locations.csv"
        
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
        """Prepare and clean addresses for geocoding."""
        if data.empty:
            print("The dataset is empty. Geocoding will not proceed.")
            return data, False

        # Clean up address formatting
        if 'full_address' in data.columns:
            data['full_address'] = (
                data['full_address']
                .str.replace(r'\.0\b', '', regex=True)  # Remove trailing .0 from numbers
            )

        # Combine fields into a clean full address
        data['full_address'] = (
            data['full_address'].fillna('') + ", " +
            data['POSTCODE_VEST'].fillna('') + ", " +
            data['NAAM_PLAATS_VEST'].fillna('') + ", " +
            data['PROVINCIE_VEST'].fillna('') + ", Netherlands"
        )

        print("\nExample constructed addresses:")
        print(data['full_address'].head())
        return data, True

    def geocode_with_retries(self, geolocator, address, max_retries=3):
        """Geocode address with retry logic."""
        for attempt in range(max_retries):
            try:
                location = geolocator.geocode(address)
                if location:
                    print(f"Successfully geocoded: {address}")
                return location
            except GeocoderTimedOut:
                if attempt < max_retries - 1:
                    print(f"Timeout for address: {address}. Retrying...")
                    time.sleep(1)
                else:
                    print(f"Failed to geocode after {max_retries} retries: {address}")
                    return None
            except GeocoderQuotaExceeded:
                print("API quota exceeded. Waiting 60 seconds...")
                time.sleep(60)
                continue
            except Exception as e:
                print(f"Error geocoding {address}: {e}")
                return None

    def geocode_dataset(self, data, batch_size=100):
        """Geocode all addresses in the dataset."""
        geolocator = GoogleV3(api_key=self.api_key)
        geocoded_results = []
        total_addresses = len(data)
        
        print(f"\nStarting geocoding for {total_addresses} addresses...")
        start_time = time.time()

        for index, row in data.iterrows():
            address = row['full_address']
            try:
                location = self.geocode_with_retries(geolocator, address)
                geocoded_results.append({
                    'full_address': address,
                    'latitude': location.latitude if location else None,
                    'longitude': location.longitude if location else None
                })
                
                # Progress reporting
                if (index + 1) % batch_size == 0:
                    completion = (index + 1) / total_addresses * 100
                    print(f"Progress: {completion:.1f}% ({index + 1}/{total_addresses})")
                    
            except Exception as e:
                print(f"Error processing address {address}: {e}")
                geocoded_results.append({
                    'full_address': address,
                    'latitude': None,
                    'longitude': None
                })

        total_time = time.time() - start_time
        print(f"\nGeocoding completed in {total_time:.2f} seconds")
        return pd.DataFrame(geocoded_results)

    def process(self):
        """Main processing function."""
        print(f"\nProcessing {self.sector} education locations...")
        
        # Load data
        data = self.load_data()
        if data is None:
            return False

        # Prepare addresses
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
        else:
            print("No valid geocoded data was produced.")
            return False

def main():
    """Main execution function."""
    sectors = ['primary', 'secondary', 'vocational', 'higher']
    
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