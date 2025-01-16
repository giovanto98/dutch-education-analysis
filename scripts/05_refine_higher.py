import pandas as pd
import numpy as np
import re
import os
from datetime import datetime

# Set working directory to the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

def extract_house_number(address):
    """Extract house number from full address."""
    if pd.isna(address):
        return ''
    match = re.search(r'\b\d+\b', str(address))
    return match.group(0) if match else ''

def extract_postcode(address):
    """Extract postcode from full address."""
    if pd.isna(address):
        return ''
    match = re.search(r'\b\d{4}[A-Z]{2}\b', str(address))
    return match.group(0).lower() if match else ''

def clean_postcode(postcode):
    """Normalize postcodes."""
    if pd.isna(postcode):
        return ''
    return str(postcode).strip().replace(' ', '').lower()

def calculate_operation_years(row):
    """Calculate years of operation for a location."""
    start_date = pd.to_datetime(row['DT_IN_BEDRIJF'])
    if pd.isna(row['DT_UIT_BEDRIJF']):
        end_date = pd.Timestamp.now()
    else:
        end_date = pd.to_datetime(row['DT_UIT_BEDRIJF'])
    
    years = (end_date - start_date).days / 365.25
    return round(years, 1)

def get_decade(date):
    """Get the decade from a date (e.g., 1990s, 2000s)."""
    if pd.isna(date):
        return None
    year = pd.to_datetime(date).year
    decade = (year // 10) * 10
    return f"{decade}s"

def categorize_facility(row):
    """Categorize facility based on CODE_SOORT and other attributes."""
    code = row['CODE_SOORT']
    
    # Define categories based on CODE_SOORT
    categories = {
        'HBO': 'University of Applied Sciences',
        'WO': 'Research University',
        'LOC': 'Higher Education Branch Location'
    }
    
    return categories.get(code, 'Other')

def determine_location_status(row):
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

def get_most_recent_record(group):
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

def process_datasets():
    """Process and clean the datasets."""
    print("Loading datasets...")
    
    # Load the datasets
    geocoded_data = pd.read_csv('../data/processed/final/geocoded_higher_locations.csv')
    higher_data = pd.read_csv('../data/processed/final/higher_education_locations.csv')
    
    print("\nPreparing geocoded dataset...")
    geocoded_data['postcode_cleaned'] = geocoded_data['full_address'].apply(extract_postcode)
    geocoded_data['house_number'] = geocoded_data['full_address'].apply(extract_house_number)
    
    print("\nPreparing higher education dataset...")
    higher_data['postcode_cleaned'] = higher_data['POSTCODE_VEST'].apply(clean_postcode)
    higher_data['house_number'] = higher_data['full_address'].apply(extract_house_number)
    
    print("\nMerging datasets...")
    merged_data = pd.merge(
        higher_data,
        geocoded_data[['postcode_cleaned', 'house_number', 'latitude', 'longitude']],
        on=['postcode_cleaned', 'house_number'],
        how='left'
    )
    
    # Add location status
    print("\nDetermining location statuses...")
    merged_data['location_status'] = merged_data.apply(determine_location_status, axis=1)
    
    # Process temporal information
    print("\nProcessing temporal information...")
    merged_data['DT_IN_BEDRIJF'] = pd.to_datetime(merged_data['DT_IN_BEDRIJF'])
    merged_data['DT_UIT_BEDRIJF'] = pd.to_datetime(merged_data['DT_UIT_BEDRIJF'])
    
    # Remove exact duplicates
    print("\nRemoving exact duplicates...")
    merged_data = merged_data.drop_duplicates()
    
    # Keep most recent/relevant record per location
    print("\nKeeping most relevant records per location...")
    unique_locations = merged_data.groupby(['latitude', 'longitude'], group_keys=False).apply(get_most_recent_record)
    unique_locations = unique_locations.reset_index(drop=True)
    
    # Add temporal analysis fields
    print("\nAdding analysis fields...")
    unique_locations['years_of_operation'] = unique_locations.apply(calculate_operation_years, axis=1)
    unique_locations['decade_opened'] = unique_locations['DT_IN_BEDRIJF'].apply(get_decade)
    unique_locations['decade_closed'] = unique_locations['DT_UIT_BEDRIJF'].apply(get_decade)
    unique_locations['facility_type'] = unique_locations.apply(categorize_facility, axis=1)
    
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
    output_path = '../data/processed/final/cleaned_higher_locations_for_qgis.csv'
    final_data.to_csv(output_path, index=False)
    print(f"\nSaved QGIS-ready dataset to: {output_path}")

if __name__ == "__main__":
    process_datasets()