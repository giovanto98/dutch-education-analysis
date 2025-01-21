# Data Documentation

## Directory Structure
```
data/
├── raw/              # Original DUO datasets
├── geocoded/         # Geocoded location data
├── processed/
│   ├── intermediate/ # Cleaned individual datasets
│   └── final/       # QGIS-ready datasets
└── logs/            # Processing logs
```

## Input Datasets

### ORGANISATIES_20250113.csv
Main organization dataset containing institution details.

Key fields:
- `NR_ADMINISTRATIE`: Unique institution identifier
- `CODE_FUNCTIE`: Function code (U = Institution, D = Location)
- `CODE_SOORT`: Institution type code
- `NAAM_VOLLEDIG`: Full institution name
- `full_address`: Complete address
- `POSTCODE_VEST`: Postal code
- `NAAM_PLAATS_VEST`: City
- `PROVINCIE_VEST`: Province
- `DT_IN_BEDRIJF`: Start date
- `DT_UIT_BEDRIJF`: End date (if applicable)

## Institution Types (CODE_SOORT)

### Higher Education
- HBOS: Universities of Applied Sciences
- UNIV: Universities
- LOC: Standard locations
- ULOC: University locations

### Secondary Education
- VOS: Regular secondary schools
- PROS: Professional secondary schools
- AOCV: Agricultural education centers
- VSTS: Secondary locations - standard
- VSTZ: Secondary locations - special care

### Vocational Education
- ROC: Regional education centers
- ROCV: Specialized regional centers
- VAK: Vocational schools
- VAKV: Advanced vocational
- ILOC: International locations

### Primary Education
- BAS: Regular primary schools
- SBAS: Special primary schools
- SPEC: Special education
- VST: Primary locations

## Processing Pipeline Output

### Step 3: Merged Datasets
- Location in: `processed/final/{sector}_education_locations.csv`
- Contains: Base location data with cleaned addresses
- Key additions: 
  - education_sector
  - location_type
  - temporal_status
  - standardized addresses

### Step 4: Geocoded Data
- Location in: `geocoded/geocoded_{sector}_locations.csv`
- Contains: Geocoding results
- Fields:
  - full_address
  - latitude
  - longitude

### Step 5: Final Refined Data
- Location in: `processed/final/cleaned_{sector}_for_qgis.csv`
- Combines base data with geocoding
- Additional analysis fields:
  - years_of_operation
  - decade_opened
  - decade_closed
  - facility_type
  - location_status
  - operation_period

## Data Quality Notes

### Geocoding Coverage
- Higher Education: 97.3%
- Secondary Education: 99.9%
- Vocational Education: 98.9%
- Primary Education: Not geocoded

### Duplicate Handling
Original duplicates removed:
- Higher Education: 6,587
- Secondary Education: 4,440
- Vocational Education: 3,170
- Primary Education: 69,879

### Address Quality
- Nearly all records have complete addresses
- Only 1 incomplete address per sector
- Standardized format for matching

### Temporal Coverage
- Historical records preserved
- Active/inactive status accurately tracked
- Operation periods calculated
- Decade analysis included

## Usage Notes
- Check geocoding quality before spatial analysis
- Consider temporal overlaps in historical data
- Use facility categorization for sector analysis
- Active/inactive status reliable for current state

## Data Security
- No personal data included
- Public institution information only
- Handle according to DUO guidelines