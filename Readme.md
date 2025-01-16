# Dutch Education Facilities Analysis

## Project Overview
This project processes and analyzes Dutch education facility locations using DUO (Dienst Uitvoering Onderwijs) datasets from 2012-2022. The analysis includes temporal changes in facility locations across primary, secondary, vocational, and higher education institutions.

## Features
- Processing of DUO education datasets
- Geocoding of facility locations
- Temporal analysis of operational status
- Facility type categorization
- QGIS-ready output formats

## Project Structure
```
dutch-education-analysis/
├── data/
│   ├── raw/                  # Original DUO datasets
│   ├── processed/
│   │   ├── intermediate/     # Cleaned individual datasets
│   │   └── final/           # QGIS-ready datasets
│   └── README.md            # Data documentation
├── scripts/
│   ├── config_template.py    # Configuration template
│   ├── 01_process_organizations.py
│   ├── 02_process_education.py
│   ├── 03_merge_datasets.py
│   ├── 04_geocode_*.py      # Geocoding scripts
│   └── 05_refine_*.py       # Data refinement scripts
├── requirements.txt
└── README.md
```

## Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/dutch-education-analysis.git
cd dutch-education-analysis
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Unix/macOS
venv\Scripts\activate     # Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure API key:
```bash
cp scripts/config_template.py scripts/config.py
# Edit config.py with your Google Maps API key
```

5. Create directory structure:
```bash
mkdir -p data/{raw,processed/{intermediate,final}}
```

## Data Sources
Required DUO datasets:
- ORGANISATIES_20250113.csv
- BASISGEGEVENS_OPLEIDINGEN_20240520.csv
- RELATIES_20250113.csv
- OVERGANGEN_20250113.csv

Place these files in the `data/raw/` directory.

## Usage

1. Process organization data:
```bash
python scripts/01_process_organizations.py
```

2. Process education data:
```bash
python scripts/02_process_education.py
```

3. Merge datasets:
```bash
python scripts/03_merge_datasets.py
```

4. Geocode locations:
```bash
python scripts/04_geocode_vocational.py
python scripts/04_geocode_secondary.py
python scripts/04_geocode_higher.py
python scripts/04_geocode_primary.py
```

5. Refine datasets for QGIS:
```bash
python scripts/05_refine_vocational.py
python scripts/05_refine_secondary.py
python scripts/05_refine_higher.py
python scripts/05_refine_primary.py
```

## Output Data
Final datasets (in `data/processed/final/`) include:
- Geographic coordinates
- Facility categorization
- Temporal information
- Operational status
- Historical analysis

## Requirements
- Python 3.8+
- Google Maps API key
- Required Python packages in requirements.txt

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments
- Data provided by DUO (Dienst Uitvoering Onderwijs)
- Geocoding services by Google Maps Platform