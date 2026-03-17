import json
import csv
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import base_folder, input_folder, output_folder

# Iterate over all JSON files in the input folder
for filename in os.listdir(input_folder):
    if filename.endswith(".json"):
        json_path = os.path.join(input_folder, filename)

        # Load JSON data
        with open(json_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        authors = data.get("authorships", [])

        # Create a CSV filename based on the JSON filename
        csv_filename = filename.replace(".json", "_metadata.csv")
        csv_path = os.path.join(output_folder, csv_filename)

        # Write to CSV
        with open(csv_path, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)

            # Write header
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

        print(f"CSV file '{csv_path}' has been created successfully.")
