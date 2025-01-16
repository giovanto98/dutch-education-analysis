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
input_file = "../data/processed/final/vocational_education_locations.csv"
geocoded_output_file = "../data/processed/final/geocoded_vocational_locations.csv"

# Load the dataset
def load_data(file_path):
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

# Analyze the dataset
def analyze_data(data):
    if data.empty:
        print("The dataset is empty. Geocoding will not proceed.")
        return False

    # Clean up house number formatting
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
    print("Dataset is valid for geocoding.")
    return True

# Geocode with retries and debug logging
def geocode_with_retries(geolocator, address, retries=3):
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

# Perform geocoding
def geocode_data(data, geolocator, batch_size=10):
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
    print(f"Geocoding completed for {len(geocoded_results)} addresses in {total_time:.2f} seconds.")
    return pd.DataFrame(geocoded_results), total_time

# Estimate geocoding time
def estimate_geocoding_time(record_count, time_per_record):
    total_time = record_count * time_per_record
    hours, remainder = divmod(total_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Estimated geocoding time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds.")

# Main function
def main():
    data = load_data(input_file)
    if data is None:
        print("Debug: Failed to load data. Exiting.")
        return

    if analyze_data(data):
        # Initialize the geolocator with your Google API key
        geolocator = GoogleV3(api_key=api_key)

        # Perform geocoding
        geocoded_data, total_time = geocode_data(data, geolocator)

        if geocoded_data.empty:
            print("No valid geocoded data was produced.")
        else:
            geocoded_data.to_csv(geocoded_output_file, index=False)
            print(f"Geocoded data saved to {geocoded_output_file}.")

if __name__ == "__main__":
    main()
