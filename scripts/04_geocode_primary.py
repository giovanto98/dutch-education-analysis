import pandas as pd
import time
import os
from geopy.geocoders import GoogleV3
from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve the API key from the environment
api_key = os.getenv("GOOGLE_API_KEY")

if not api_key:
    print("API Key not found! Please make sure to set GOOGLE_API_KEY in the .env file.")
    exit()

# Set working directory to the file location
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Paths to input and output files
input_file = "../data/processed/final/primary_education_locations.csv"
geocoded_output_file = "../data/processed/final/geocoded_primary_locations.csv"

def load_data(file_path):
    """Load and validate the dataset."""
    try:
        resolved_path = os.path.abspath(file_path)
        print("Debug: Trying to load file from resolved path:", resolved_path)
        if not os.path.exists(resolved_path):
            print(f"Debug: File does not exist at resolved path: {resolved_path}")
            return None

        data = pd.read_csv(file_path)
        print(f"Loaded {len(data)} records from {file_path}.")
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def analyze_data(data):
    """Analyze and prepare addresses for geocoding."""
    if data.empty:
        print("The dataset is empty. Geocoding will not proceed.")
        return False

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

    print("Example constructed addresses:")
    print(data['full_address'].head())
    return True

def geocode_with_retries(geolocator, address, retries=3):
    """Geocode address with retry logic."""
    for attempt in range(retries):
        try:
            print(f"Debug: Attempting to geocode address: {address}")
            location = geolocator.geocode(address)
            if location:
                print(f"Debug: Geocoded address: {address} to {location.latitude}, {location.longitude}")
            return location
        except GeocoderTimedOut:
            if attempt < retries - 1:
                print(f"Timeout for address: {address}. Retrying in 1 second...")
                time.sleep(1)
            else:
                print(f"Failed to geocode after {retries} retries: {address}")
                return None
        except GeocoderQuotaExceeded:
            print(f"Quota exceeded for address {address}. Sleeping for 60 seconds...")
            time.sleep(60)

def geocode_data(data, geolocator):
    """Geocode all addresses in the dataset."""
    geocoded_results = []
    start_time = time.time()

    for index, row in data.iterrows():
        address = row["full_address"]
        try:
            location = geocode_with_retries(geolocator, address)
            if location:
                geocoded_results.append({
                    "full_address": address,
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                })
            else:
                geocoded_results.append({
                    "full_address": address,
                    "latitude": None,
                    "longitude": None,
                })
        except Exception as e:
            print(f"Error geocoding address {address}: {e}")
            geocoded_results.append({
                "full_address": address,
                "latitude": None,
                "longitude": None,
            })

        # Progress reporting
        if (index + 1) % 100 == 0:
            print(f"Processed {index + 1}/{len(data)} addresses...")

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Geocoding completed in {total_time:.2f} seconds")
    return pd.DataFrame(geocoded_results), total_time

def main():
    """Main execution function."""
    print("Debug: Starting the geocoding script for primary education...")
    data = load_data(input_file)
    if data is None:
        print("Debug: Failed to load data. Exiting.")
        return

    if analyze_data(data):
        # Initialize the geolocator with your Google API key
        geolocator = GoogleV3(api_key=api_key)

        # Perform geocoding
        geocoded_data, total_time = geocode_data(data, geolocator)

        # Check if any geocoded data exists
        if geocoded_data.empty:
            print("No valid geocoded data was produced.")
        else:
            # Save results
            geocoded_data.to_csv(geocoded_output_file, index=False)
            print(f"Geocoded data saved to {geocoded_output_file}.")

if __name__ == "__main__":
    main()