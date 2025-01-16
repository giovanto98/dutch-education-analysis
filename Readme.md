Certainly! Here’s the updated README.md file in a code format that you can easily copy and paste:

# Dutch Education Facilities Analysis

## Project Overview
This project processes and analyzes Dutch education facility locations using DUO (Dienst Uitvoering Onderwijs) datasets from 2012-2022. The analysis includes temporal changes in facility locations across primary, secondary, vocational, and higher education institutions.

## Features
- Processing of DUO education datasets
- Geocoding of facility locations
- Temporal analysis of operational status
- Facility type categorization
- QGIS-readable output for spatial analysis

## Setup and Installation

### 1. Clone the Repository
Clone the repository to your local machine using GitHub Desktop or the command line:
```bash
git clone https://github.com/giovanto98/dutch-education-analysis.git

2. Install Dependencies

Install Python dependencies using pip:

pip install -r requirements.txt

3. Create a .env File

Create a .env file in the root of the project directory. This file should contain sensitive information like your API key.

For security reasons, do not upload your .env file to GitHub. Ensure that it is included in .gitignore to prevent it from being tracked by Git.

Example .env file:

GOOGLE_API_KEY=your_google_api_key_here

4. Set Up the Virtual Environment

Create a virtual environment to isolate your project dependencies:

python -m venv venv

Activate the virtual environment:
	•	macOS/Linux:

source venv/bin/activate


	•	Windows:

.\venv\Scripts\activate



Install the dependencies in your virtual environment:

pip install -r requirements.txt

5. Running the Project

To run the project, simply execute the appropriate Python scripts for geocoding and processing datasets, as described in the repository’s scripts directory.

File Structure

/dutch-education-analysis
│
├── data/                       # Datasets used for analysis
├── scripts/                    # Python scripts for data processing
├── requirements.txt            # Python dependencies
├── .gitignore                  # Git ignore file (to avoid uploading sensitive files)
├── .env                        # Environment variables (e.g., API keys)
└── README.md                   # Project overview and setup instructions

License

This project is licensed under the MIT License - see the LICENSE file for details.
