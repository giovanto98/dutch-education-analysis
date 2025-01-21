import pandas as pd
from pathlib import Path
import os

def verify_institutional_codes(df):
    """Detailed verification of institutional codes and their characteristics."""
    print(f"\n{'='*80}")
    print("INSTITUTIONAL CODE VERIFICATION")
    print(f"{'='*80}")
    
    # Proposed code mappings to verify
    proposed_mappings = {
        'Higher Education': {
            'Main': ['HBOS', 'UNIV'],
            'Locations': ['LOC', 'ULOC'],
            'Related': ['ILOC']
        },
        'Vocational Education': {
            'Main': ['ROC', 'ROCV'],
            'Specialized': ['VAK', 'VAKV'],
            'Related': ['ILOC']
        },
        'Secondary Education': {
            'Main': ['VOS'],
            'Specialized': ['PROS', 'AOCV'],
            'Locations': ['VSTS', 'VSTZ']
        },
        'Primary Education': {
            'Main': ['BAS', 'SBAS', 'SPEC'],
            'Locations': ['VST']
        }
    }
    
    for sector, categories in proposed_mappings.items():
        print(f"\n{'-'*40}")
        print(f"{sector} Verification")
        print(f"{'-'*40}")
        
        all_sector_codes = [code for sublist in categories.values() 
                          for code in sublist]
        
        # Basic statistics
        sector_data = df[df['CODE_SOORT'].isin(all_sector_codes)]
        print(f"\n1. Basic Statistics:")
        print(f"Total institutions: {len(sector_data)}")
        
        # Code distribution with function types
        print(f"\n2. Code Distribution by Function Type:")
        dist = pd.crosstab(sector_data['CODE_SOORT'], 
                          sector_data['CODE_FUNCTIE'])
        print(dist)
        
        # Active vs Historical analysis
        if 'DT_UIT_BEDRIJF' in df.columns:
            sector_data['status'] = 'Active'
            sector_data.loc[sector_data['DT_UIT_BEDRIJF'].notna(), 
                          'status'] = 'Historical'
            print(f"\n3. Active vs Historical Status:")
            status_dist = pd.crosstab(sector_data['CODE_SOORT'], 
                                    sector_data['status'])
            print(status_dist)
        
        # Location analysis
        print(f"\n4. Geographic Distribution:")
        if 'PROVINCIE_VEST' in sector_data.columns:
            geo_dist = pd.crosstab(sector_data['CODE_SOORT'], 
                                 sector_data['PROVINCIE_VEST'])
            print("\nProvincial Distribution:")
            print(geo_dist)

def analyze_relationships(df):
    """Analyze relationships between different institution types."""
    print(f"\n{'='*80}")
    print("INSTITUTIONAL RELATIONSHIPS ANALYSIS")
    print(f"{'='*80}")
    
    # Define main institution types
    main_institutions = {
        'Higher': ['HBOS', 'UNIV'],
        'Vocational': ['ROC', 'ROCV', 'VAK', 'VAKV'],
        'Secondary': ['VOS', 'PROS', 'AOCV'],
        'Primary': ['BAS', 'SBAS', 'SPEC']
    }
    
    location_types = ['VST', 'VSTS', 'VSTZ', 'LOC', 'ULOC', 'ILOC']
    
    # Analyze institution-location relationships
    print("\n1. Institution-Location Relationships:")
    for sector, inst_codes in main_institutions.items():
        sector_data = df[df['CODE_SOORT'].isin(inst_codes)]
        print(f"\n{sector} Education:")
        print(f"Main institutions: {len(sector_data)}")
        
        # Look for related locations
        for loc_type in location_types:
            loc_count = len(df[df['CODE_SOORT'] == loc_type])
            print(f"- {loc_type}: {loc_count} locations")
    
    # Check for inconsistencies
    print("\n2. Potential Inconsistencies:")
    for sector, inst_codes in main_institutions.items():
        # Check for institutions without locations
        inst_ids = df[df['CODE_SOORT'].isin(inst_codes)]['NR_ADMINISTRATIE']
        locs = df[df['CODE_SOORT'].isin(location_types)]['NR_ADMINISTRATIE']
        no_locs = len(set(inst_ids) - set(locs))
        if no_locs > 0:
            print(f"{sector}: {no_locs} institutions without registered locations")

def analyze_temporal_evolution(df):
    """Analyze how institution types have evolved over time."""
    print(f"\n{'='*80}")
    print("TEMPORAL EVOLUTION ANALYSIS")
    print(f"{'='*80}")
    
    # Convert date columns
    date_cols = ['DT_IN_BEDRIJF', 'DT_UIT_BEDRIJF']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Add decade information
    df['decade'] = (df['DT_IN_BEDRIJF'].dt.year // 10 * 10).fillna(-1)
    
    # Analyze temporal patterns by sector
    print("\n1. Institution Creation by Decade:")
    decade_dist = pd.crosstab(df['decade'], df['CODE_SOORT'])
    print(decade_dist)
    
    # Analyze closure patterns
    print("\n2. Institution Closure Patterns:")
    df['closure_decade'] = (df['DT_UIT_BEDRIJF'].dt.year // 10 * 10).fillna(-1)
    closure_dist = pd.crosstab(df['closure_decade'], df['CODE_SOORT'])
    print(closure_dist)

def verify_codes_complete():
    """Main verification function."""
    # Load data
    project_root = Path(os.getcwd())
    raw_data_path = project_root / 'data' / 'raw' / 'ORGANISATIES_20250113.csv'
    
    print(f"Loading raw data from: {raw_data_path}")
    df = pd.read_csv(raw_data_path, encoding='utf-8', low_memory=False)
    
    # Run verifications
    verify_institutional_codes(df)
    analyze_relationships(df)
    analyze_temporal_evolution(df)

if __name__ == "__main__":
    verify_codes_complete()