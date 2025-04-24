import pandas as pd
from pathlib import Path

# Define the data directory
# data_dir = Path("CSVs")

# Associate each CSV with its corresponding paper number
paper_files = {
    "paper1_metadata.csv": 1,
    "paper2_metadata.csv": 2,
    "paper3_metadata.csv": 3,
    "paper4_metadata.csv": 4
}

# Load all data with paper number tagging
records = []
for file_path, paper_number in paper_files.items():
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

# Export to CSV
final_df.to_csv("authors_metadata.csv", index=False)
