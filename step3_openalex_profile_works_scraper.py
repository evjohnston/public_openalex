import requests
import csv
import time

# Input and output CSV files
input_csv = "authors_enriched.csv"
output_csv = "authors_and_works.csv"

# Read author data from the CSV
authors_data = []
with open(input_csv, mode="r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        authors_data.append(row)

# Function to fetch all works from OpenAlex with pagination
def fetch_all_works(author_id):
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

        # Check if there's another page
        cursor = data.get("meta", {}).get("next_cursor")

        # Delay to avoid rate limiting
        time.sleep(.2)

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
        author_id = author.get("Author ID", "").strip()
        author_name = author.get("Author Name", "Unknown")

        if not author_id.startswith("https://openalex.org/"):
            print(f"⏭️ Skipping {author_name} (Invalid Author ID)")
            continue

        works = fetch_all_works(author_id)

        for work in works:
            work_id = work.get("id", "N/A")
            title = work.get("title", "N/A")
            doi = work.get("doi", "N/A")
            year = work.get("publication_year", "N/A")
            pub_date = work.get("publication_date", "N/A")
            work_type = work.get("type", "N/A")
            language = work.get("language", "N/A")
            citations = work.get("cited_by_count", 0)

            # Extract topics
            topics = [t["display_name"] for t in work.get("topics", [])]
            topics_str = ", ".join(topics) if topics else "N/A"

            # Extract co-authors
            co_authors = [a["author"]["display_name"] for a in work.get("authorships", []) if a["author"]["id"] != author_id]
            co_authors_str = ", ".join(co_authors) if co_authors else "N/A"

            writer.writerow({
                "Author ID": author_id,
                "Author Name": author_name,
                "Work ID": work_id,
                "Title": title,
                "DOI": doi,
                "Year": year,
                "Publication Date": pub_date,
                "Type": work_type,
                "Language": language,
                "Citations": citations,
                "Topics": topics_str,
                "Co-Authors": co_authors_str,
            })

        print(f"✅ Fetched {len(works)} works for {author_name}")

print(f"\n✅ Works data saved in '{output_csv}'!")
