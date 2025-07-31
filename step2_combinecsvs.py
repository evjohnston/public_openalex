import pandas as pd
from pathlib import Path

# 🔹 Change this one line to set the base folder
base_folder = "Anthropic"

# Define the input folder where CSVs are stored
csv_dir = Path(base_folder) / "CSVs"

# Find all metadata CSV files automatically
csv_files = sorted(csv_dir.glob("*_metadata.csv"))

records = []
for file_path in csv_files:
    # Extract paper number from filename (assumes 'paperX_metadata.csv' format)
    paper_number = int(file_path.stem.split("_")[0].replace("paper", ""))

    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df["paper_number"] = paper_number
    records.append(df)

# Combine all into one DataFrame
all_authors = pd.concat(records, ignore_index=True)

# Deduplicate by author_name and author_id per paper
all_authors = all_authors.drop_duplicates(subset=["author_name", "author_id", "paper_number"])

# Group by author_name and author_id to aggregate paper participation
grouped = all_authors.groupby(["author_name", "author_id"]).agg({
    "paper_number": lambda x: sorted(set(x))
}).reset_index()

# Derive final fields
grouped["No. Papers"] = grouped["paper_number"].apply(len)
grouped["Papers"] = grouped["paper_number"].apply(lambda x: ", ".join(map(str, x)))
grouped["Author"] = grouped["author_name"]
grouped["OA_Profile"] = grouped["author_id"]
grouped["OA_ID"] = grouped["author_id"].apply(lambda x: x.split("/")[-1])
grouped["Notes"] = ""

# Select final column structure
final_df = grouped[["Author", "No. Papers", "Notes", "OA_Profile", "OA_ID", "Papers"]]

# Export to CSV inside the same folder
output_file = csv_dir / "authors_metadata.csv"
final_df.to_csv(output_file, index=False)

print(f"Combined CSV created: {output_file}")
