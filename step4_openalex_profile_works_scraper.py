import requests
import csv
import time
from pathlib import Path

# 🔹 Change this one line to set the base folder
base_folder = "KimiK2"

# Define input and output CSV paths
input_csv = Path(base_folder) / "CSVs" / "enriched_authors_metadata.csv"
output_csv = Path(base_folder) / "CSVs" / "authors_and_works.csv"

# Read author data from the CSV
authors_data = []
with open(input_csv, mode="r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        authors_data.append(row)

def fetch_all_works(author_id):
    """
    Fetch all works from OpenAlex API with pagination
    """
    base_url = f"https://api.openalex.org/works?filter=author.id:{author_id}&per_page=50"
    works = []
    cursor = "*"

    while cursor:
        url = f"{base_url}&cursor={cursor}"
        print(f"🔍 Fetching works for {author_id} (Cursor: {cursor})...")
        
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"❌ Failed to fetch works for {author_id}, status code: {response.status_code}")
            break

        data = response.json()
        works.extend(data.get("results", []))

        # Next page cursor
        cursor = data.get("meta", {}).get("next_cursor")

        time.sleep(0.2)  # Delay to avoid rate limiting

    return works

# Write output CSV
with open(output_csv, mode="w", newline="", encoding="utf-8") as file:
    fieldnames = [
        "Author ID", "Author Name", "Work ID", "Title", "DOI", "Year", "Publication Date",
        "Type", "Language", "Citations", "Topics", "Co-Authors"
    ]
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    for index, author in enumerate(authors_data, start=1):
        author_id = author.get("OA_Profile", "").strip()  # Use OA_Profile (full URL)
        author_name = author.get("Author", "Unknown")

        if not author_id.startswith("https://openalex.org/"):
            print(f"⏭️ Skipping {author_name} (Invalid Author ID)")
            continue

        works = fetch_all_works(author_id)

        for work in works:
            writer.writerow({
                "Author ID": author_id,
                "Author Name": author_name,
                "Work ID": work.get("id", "N/A"),
                "Title": work.get("title", "N/A"),
                "DOI": work.get("doi", "N/A"),
                "Year": work.get("publication_year", "N/A"),
                "Publication Date": work.get("publication_date", "N/A"),
                "Type": work.get("type", "N/A"),
                "Language": work.get("language", "N/A"),
                "Citations": work.get("cited_by_count", 0),
                "Topics": ", ".join([t["display_name"] for t in work.get("topics", [])]) or "N/A",
                "Co-Authors": ", ".join([
                    a["author"]["display_name"]
                    for a in work.get("authorships", [])
                    if a["author"]["id"] != author_id
                ]) or "N/A",
            })

        print(f"✅ Fetched {len(works)} works for {author_name}")

print(f"\n✅ Works data saved in '{output_csv}'!")
