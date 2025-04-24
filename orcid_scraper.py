import requests
from bs4 import BeautifulSoup
import csv
import time

# Input and output CSV files
input_csv = "authors_enriched.csv"
output_csv = "orcid_enriched.csv"

# Read author data from the CSV
authors_data = []
with open(input_csv, mode="r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        authors_data.append(row)

# Function to scrape ORCID profile data
def scrape_orcid_profile(orcid_url):
    print(f"🔍 Scraping ORCID profile: {orcid_url}...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(orcid_url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Failed to retrieve ORCID page for {orcid_url}, status code: {response.status_code}")
        return {}

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract employment history
    employment_section = soup.find("div", {"id": "employment"})
    employments = []
    if employment_section:
        for job in employment_section.find_all("div", class_="organization"):
            institution = job.find("span", class_="ng-star-inserted")
            position = job.find("span", class_="position-title")
            years = job.find("div", class_="date-range")
            
            institution_name = institution.text.strip() if institution else "Unknown Institution"
            position_name = position.text.strip() if position else "Unknown Position"
            years_text = years.text.strip() if years else "N/A"
            
            employments.append(f"{institution_name} ({years_text})")

    employment_str = ", ".join(employments) if employments else "N/A"

    # Extract keywords
    keywords_section = soup.find("div", {"id": "keywords"})
    keywords = [kw.text.strip() for kw in keywords_section.find_all("span")] if keywords_section else []
    keywords_str = ", ".join(keywords) if keywords else "N/A"

    # Extract country
    country_section = soup.find("div", {"id": "country"})
    country = country_section.text.strip() if country_section else "N/A"

    # Extract researcher URLs
    website_section = soup.find("div", {"id": "websites"})
    websites = [w.text.strip() for w in website_section.find_all("a")] if website_section else []
    websites_str = ", ".join(websites) if websites else "N/A"

    print(f"✅ Successfully scraped ORCID data for {orcid_url}")

    return {
        "ORCID - Employment": employment_str,
        "ORCID - Keywords": keywords_str,
        "ORCID - Country": country,
        "ORCID - Websites": websites_str,
    }

# Write output CSV
with open(output_csv, mode="w", newline="", encoding="utf-8") as file:
    fieldnames = list(authors_data[0].keys()) + [
        "ORCID - Employment", "ORCID - Keywords", "ORCID - Country", "ORCID - Websites"
    ]
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    for index, author in enumerate(authors_data, start=1):
        orcid_url = author.get("ORCID", "").strip()

        if orcid_url and orcid_url.startswith("https://orcid.org/"):
            print(f"\n🔍 Processing ORCID {index}/{len(authors_data)}: {author.get('Author Name', 'Unknown')}")
            orcid_data = scrape_orcid_profile(orcid_url)

            if orcid_data:
                author.update(orcid_data)
            else:
                print(f"⚠️ Skipping ORCID {orcid_url} due to missing data.")
        else:
            print(f"⏭️ Skipping author {author.get('Author Name', 'Unknown')} (No ORCID found)")

        writer.writerow(author)
        time.sleep(.25)  # Prevent rate-limiting

print(f"\n✅ ORCID-enriched CSV file '{output_csv}' has been created successfully!")
