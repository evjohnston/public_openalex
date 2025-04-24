import json
import csv

# Load JSON data from a file
json_file = "paper4.json"
with open(json_file, "r", encoding="utf-8") as file:
    data = json.load(file)  # Load the JSON data

authors = data.get("authorships", [])  # Get the list of authors

# Prepare the CSV file
csv_file = "paper4_metadata.csv"

# Extract the data and write it to a CSV
with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    # Write the header row
    writer.writerow([
        "Author Position",
        "Author ID",
        "Author Name",
        "ORCID",
        "Is Corresponding",
        "Raw Author Name",
        "Affiliations",
    ])

    # Write author metadata
    for author_data in authors:
        author = author_data.get("author", {})
        writer.writerow([
            author_data.get("author_position", ""),
            author.get("id", ""),
            author.get("display_name", ""),
            author.get("orcid", ""),
            author_data.get("is_corresponding", False),
            author_data.get("raw_author_name", ""),
            ", ".join([aff.get("display_name", "") for aff in author_data.get("affiliations", [])]),
        ])

print(f"CSV file '{csv_file}' has been created successfully.")