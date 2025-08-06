import pandas as pd
import os
import re

# Directories to process
main_dirs = ['Anthropic', 'Google', 'OpenAI']
csv_subdir = 'CSVs'
authors_filename = 'enriched_authors_metadata.csv'
output_filename = 'author_career_trajectory.csv'

# Match columns like affiliation_country_2001
country_year_pattern = re.compile(r"^affiliation_country_(\d{4})$")

def process_author_row(row, year_country_cols):
    # Extract countries in order of year
    country_sequence = []
    year_country_map = {}

    for year, col in year_country_cols.items():
        country = row.get(col)
        if pd.notna(country):
            country = country.strip()
            country_sequence.append((int(year), country))
            year_country_map[int(year)] = country
        else:
            year_country_map[int(year)] = None

    # Sort by year
    country_sequence.sort()

    # Remove consecutive duplicates
    trajectory = []
    prev = None
    for _, country in country_sequence:
        if country != prev:
            trajectory.append(country)
            prev = country

    full_trajectory = " ---> ".join(trajectory) if trajectory else None

    # Years in US
    years_in_us = sum(1 for _, c in country_sequence if c == "United States")

    # Simplified trajectory
    simplified = None
    even_more_simplified = None
    if len(set(trajectory)) == 1 and len(trajectory) > 0:
        country = trajectory[0]
        simplified = f"Affiliated with one country"
        even_more_simplified = f"Started and Ended in {country}"
    elif trajectory:
        first = trajectory[0]
        last = trajectory[-1]
        simplified = f"Multinational Travel ---> {last}"

        if first == last:
            if len(trajectory) > 1:
                even_more_simplified = f"Started and Ended in {first} (with travel in between)"
            else:
                even_more_simplified = f"Started and Ended in {first}"
        else:
            even_more_simplified = f"Started in {first}, Ended up in {last}"
    else:
        simplified = None
        even_more_simplified = None

    # Year-wise columns
    year_country_fields = {str(year): year_country_map.get(year) for year in sorted(year_country_map)}

    return pd.Series({
        "Author": row["Author"],
        "OA_Profile" : row["OA_Profile"],
        "Geographic Trajectory": full_trajectory,
        "Years in the US": years_in_us,
        "Geographic Trajectory (simplified)": simplified,
        "Geographic Trajectory (even more simplified)": even_more_simplified,
        **year_country_fields
    })

def process_file(folder):
    author_path = os.path.join(folder, csv_subdir, authors_filename)
    output_path = os.path.join(folder, csv_subdir, output_filename)

    if not os.path.exists(author_path):
        print(f"Missing: {author_path}")
        return

    df = pd.read_csv(author_path)
    df = df[df['affiliation_data'] == True]  # Only authors with affiliation data

    # Extract year-country columns
    year_country_cols = {
        int(match.group(1)): col
        for col in df.columns
        if (match := country_year_pattern.match(col))
    }

    if not year_country_cols:
        print(f"No year-based country columns found in {author_path}")
        return

    # Process each author
    trajectory_df = df.apply(lambda row: process_author_row(row, year_country_cols), axis=1)

    # Save result
    trajectory_df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")

# Run for each org
for folder in main_dirs:
    process_file(folder)
