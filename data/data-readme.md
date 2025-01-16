# Data Documentation

## Directory Structure
```
data/
├── raw/                  # Original DUO datasets
├── processed/
    ├── intermediate/     # Cleaned individual datasets
    └── final/           # QGIS-ready datasets
```

## Input Datasets

### ORGANISATIES_20250113.csv
Main organization dataset containing institution details.

Key fields:
- `NR_ADMINISTRATIE`: Unique institution identifier
- `CODE_FUNCTIE`: Function code
- `CODE_SOORT`: Institution type code
- `NAAM_VOLLEDIG`: Full institution name
- `NAAM_STRAAT_VEST`: Street name
- `NR_HUIS_VEST`: House number
- `POSTCODE_VEST`: Postal code
- `NAAM_PLAATS_VEST`: City
- `PROVINCIE_VEST`: Province
- `DT_IN_BEDRIJF`: Start date
- `DT_UIT_BEDRIJF`: End date (if applicable)

### BASISGEGEVENS_OPLEIDINGEN_20240520.csv
Educational programs information.

Key fields:
- Institution identifiers
- Program codes
- Education levels
- Start and end dates

## Processed Datasets

### Intermediate Files
- `clean_organizations.csv`: Cleaned organization data
- `clean_education.csv`: Processed education programs data

### Final Files
For each education level (primary, secondary, vocational, higher):
- `*_education_locations.csv`: Basic location data
- `geocoded_*_locations.csv`: Data with coordinates
- `cleaned_*_locations_for_qgis.csv`: Final QGIS-ready datasets

## QGIS Output Schema
Final datasets include:
- Geographic coordinates (latitude/longitude)
- Facility categorization
- Temporal information (operational periods)
- Status (active/closed)
- Years of operation
- Decade analysis
- Address information

## Data Processing Notes
- Dates are in YYYY-MM-DD format
- Coordinates in WGS84 (EPSG:4326)
- Postal codes in Dutch format (1234AB)
- Missing values as empty strings or NULL
- Duplicates removed based on location

## Data Quality Considerations
- Some addresses may be incomplete
- Historical records might have gaps
- Multiple entries possible for relocated facilities
- Geocoding accuracy varies by address quality

## Usage Guidelines
- Check geocoding quality flags
- Verify temporal overlaps
- Consider facility type categorization
- Review province distribution

## Privacy and Sensitivity
Handle according to DUO's data usage guidelines.