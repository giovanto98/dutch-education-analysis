# Dutch Education Facilities Analysis

## Project Overview
This project processes and analyzes Dutch education facility locations using DUO (Dienst Uitvoering Onderwijs) datasets from 2012-2022. The analysis includes temporal changes in facility locations across primary, secondary, vocational, and higher education institutions.

## Features
- Processing of DUO education datasets
- Geocoding of facility locations
- Temporal analysis of operational status
- Facility type categorization
- QGIS-ready output for spatial analysis

## Project Structure
```
/dutch-education-analysis
│
├── data/
│   ├── raw/              # Original DUO datasets
│   ├── geocoded/         # Geocoded location data
│   ├── processed/        # Processed datasets
│   │   ├── intermediate/ # Intermediate processing results
│   │   └── final/       # Final QGIS-ready datasets
│   └── logs/            # Processing logs
├── scripts/             # Data processing scripts
├── requirements.txt     # Python dependencies
├── .gitignore          # Git ignore configuration
├── .env                # Environment variables
└── README.md           # Project documentation
```

## Processing Pipeline

The project follows a three-step processing pipeline:

1. **Dataset Merging** (Step 3)
   - Merges organization and education data
   - Removes duplicates
   - Standardizes addresses
   - Creates sector-specific datasets

2. **Geocoding** (Step 4)
   - Geocodes facility locations using Google Maps API
   - Handles rate limiting and retries
   - Saves geocoded coordinates

3. **Data Refinement** (Step 5)
   - Combines base data with geocoded locations
   - Performs temporal analysis
   - Categorizes facilities
   - Creates QGIS-ready outputs

## Setup and Installation

### 1. Clone the Repository
```bash
git clone https://github.com/giovanto98/dutch-education-analysis.git
cd dutch-education-analysis
```

### 2. Create Virtual Environment
```bash
python -m venv dutch_edu_env
source dutch_edu_env/bin/activate  # Unix
dutch_edu_env\Scripts\activate     # Windows
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment
Create a `.env` file in the project root with:
```
GOOGLE_API_KEY=your_google_api_key_here
```

### 5. Create Directory Structure
```bash
mkdir -p data/{raw,processed/{intermediate,final},geocoded,logs}
```

## Running the Analysis

Execute the scripts in order:

1. **Merge Datasets**
```bash
python scripts/03_merge_datasets.py
```

2. **Geocode Locations** (requires API key)
```bash
python scripts/04_geocode_locations.py
```

3. **Refine Locations**
```bash
python scripts/05_refine_locations.py
```

## Output Datasets

For each education sector (primary, secondary, vocational, higher):
- `{sector}_education_locations.csv`: Base location data
- `geocoded_{sector}_locations.csv`: Geocoded coordinates
- `cleaned_{sector}_for_qgis.csv`: Final QGIS-ready dataset

## Data Documentation
See `data/README.md` for detailed information about:
- Input dataset specifications
- Processing steps
- Output schema
- Data quality considerations

## License
This project is licensed under the MIT License - see the LICENSE file for details.