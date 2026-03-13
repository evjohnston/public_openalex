import pandas as pd
from pathlib import Path

# 🔹 Change this one line to set the base folder
base_folder = "deepseek"

# Define the input folder where CSVs are stored
csv_dir = Path(base_folder) / "CSVs"

# Find all metadata CSV files automatically
csv_files = sorted(csv_dir.glob("DS*_metadata.csv"))

records = []
for file_path in csv_files:
    paper_number = int(file_path.stem.split("_")[0].replace("DS000", ""))

    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df["paper_number"] = paper_number
    records.append(df)

# Combine all into one DataFrame
all_authors = pd.concat(records, ignore_index=True)

all_authors["author_id"] = all_authors["author_id"].fillna("")

# Normalize names: flip "Last, First" to "First Last", then clean whitespace
def normalize_name(name):
    name = str(name).strip()
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        name = f"{parts[1]} {parts[0]}"
    return name.strip()

all_authors["author_name_clean"] = all_authors["author_name"].apply(normalize_name)

# Deduplicate by cleaned name per paper
all_authors = all_authors.drop_duplicates(subset=["author_name_clean", "paper_number"])

# Group by cleaned name, picking the first non-empty author_id
grouped = all_authors.groupby("author_name_clean").agg({
    "author_id": lambda x: next((v for v in x if v != ""), ""),
    "paper_number": lambda x: sorted(set(x))
}).reset_index()

# Derive final fields
grouped["No. Papers"] = grouped["paper_number"].apply(len)
grouped["Papers"] = grouped["paper_number"].apply(lambda x: ", ".join(map(str, x)))
grouped["Author"] = grouped["author_name_clean"]
grouped["OA_Profile"] = grouped["author_id"]
grouped["OA_ID"] = grouped["author_id"].apply(lambda x: x.split("/")[-1] if x else "")
grouped["Notes"] = ""

# Select final column structure
final_df = grouped[["Author", "No. Papers", "Notes", "OA_Profile", "OA_ID", "Papers"]]

# Export to CSV inside the same folder
output_file = csv_dir / "authors_metadata.csv"
final_df.to_csv(output_file, index=False)

print(f"Combined CSV created: {output_file}")