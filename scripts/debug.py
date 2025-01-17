import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Set paths
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
data_file = project_root / "data/processed/intermediate/clean_organizations.csv"

# Check if file exists
if not data_file.exists():
    print(f"❌ Error: File not found at {data_file}")
    exit()

# Load dataset
df = pd.read_csv(data_file)

# Debugging function
def analyze_original_data(df):
    print("\n🔎 Original Organizations Dataset Overview")
    print(df.info())

    print("\n📌 Missing Values Count:")
    print(df.isnull().sum())

    print("\n🛠️ CODE_SOORT Distribution (Institution Types in Original Data):")
    print(df['CODE_SOORT'].value_counts())

    print("\n🏫 CODE_FUNCTIE Distribution (Function Types in Original Data):")
    print(df['CODE_FUNCTIE'].value_counts())

    # Check if HBO and WO exist in original dataset
    expected_types = {'HBO', 'WO'}
    found_types = set(df['CODE_SOORT'].unique())

    missing_types = expected_types - found_types
    extra_types = found_types - expected_types

    if missing_types:
        print(f"\n⚠️ Warning: The following expected higher education types are missing in the original dataset: {missing_types}")
    else:
        print("\n✅ HBO and WO exist in the original dataset!")

    if extra_types:
        print(f"\n🚨 Other unexpected institution types found: {extra_types}")

    # Visualize CODE_SOORT distribution
    plt.figure(figsize=(8, 4))
    df['CODE_SOORT'].value_counts().plot(kind="bar", color="blue")
    plt.title("Distribution of CODE_SOORT (Institution Types) in Original Organizations Data")
    plt.xlabel("Institution Type")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.show()

# Run analysis
if __name__ == "__main__":
    print(f"📂 Running from: {script_dir}")
    print(f"📄 Loading data from: {data_file}\n")
    analyze_original_data(df)