import os
import pandas as pd
import re

# Configuration
main_dirs = ['Anthropic', 'Google', 'OpenAI']
csv_subdir = 'CSVs'
authors_filename = 'enriched_authors_metadata.csv'
institutions_filename = 'enriched_institutions.csv'

# Updated regex patterns for already-processed columns
id_pattern = re.compile(r'^affiliation_id_(\d{4})$')
country_pattern = re.compile(r'^affiliation_country_(\d{4})$')
name_pattern = re.compile(r'^affiliation_display_name_(\d{4})$')

# Remove invisible characters and normalize NULL/empty
def clean_text(val):
    if isinstance(val, str):
        val = re.sub(r'[\u200B-\u200D\uFEFF\u00A0]', '', val)  # Remove invisible characters
        val = val.strip()
        if val.upper() == "NULL" or val == "":
            return None
    return val

def clean_dataframe(df):
    return df.map(clean_text)  # Use map instead of deprecated applymap

def process_folder(folder):
    author_path = os.path.join(folder, csv_subdir, authors_filename)
    institution_path = os.path.join(folder, csv_subdir, institutions_filename)

    if not os.path.exists(author_path) or not os.path.exists(institution_path):
        print(f"Skipping {folder} (missing files)")
        return

    print(f"Processing: {author_path}")

    # Load and clean CSVs
    author_df = clean_dataframe(pd.read_csv(author_path))
    inst_df = clean_dataframe(pd.read_csv(institution_path))

    # Build mapping from institution ID -> {display_name, country}
    inst_info = inst_df.set_index('id')[['display_name', 'country']].to_dict('index')

    # Identify year-specific columns (using updated patterns)
    id_cols, country_cols, name_cols = {}, {}, {}
    for col in author_df.columns:
        if id_match := id_pattern.match(col):
            id_cols[int(id_match.group(1))] = col
        elif country_match := country_pattern.match(col):
            country_cols[int(country_match.group(1))] = col
        elif name_match := name_pattern.match(col):
            name_cols[int(name_match.group(1))] = col

    valid_years = sorted(set(id_cols) & set(country_cols) & set(name_cols), reverse=True)
    print(f"Valid years found: {valid_years}")

    # Remove existing analytics columns if they exist
    analytics_columns = [
        "affiliation_data", "unique_affiliation_count", "all_usa", "some_usa", "all_china", "some_china",
        "unique_country_count", "all_countries", "Most Recent Affiliation", 
        "Most Recent Affiliation Country"
    ]
    
    for col in analytics_columns:
        if col in author_df.columns:
            author_df = author_df.drop(columns=[col])

    # Add enriched affiliation info for each year (update existing country columns)
    for year in valid_years:
        country_col = country_cols[year]
        id_col = id_cols[year]

        # Update country information using institution mapping
        author_df[country_col] = author_df[id_col].map(
            lambda x: inst_info.get(x, {}).get('country') if pd.notna(x) and x in inst_info else None
        )

    # Add enrichment analytics
    def insert_analytics(row):
        ids = []
        countries = []
        display_names = []

        for year in valid_years:
            aff_id = row.get(id_cols[year])
            aff_country = row.get(country_cols[year])
            aff_name = row.get(name_cols[year])

            # Better null checking
            def is_valid_value(val):
                if pd.isna(val):
                    return False
                if isinstance(val, str):
                    cleaned = val.strip().upper()
                    if cleaned == "NULL" or cleaned == "":
                        return False
                return True

            if is_valid_value(aff_id):
                ids.append(str(aff_id).strip())
            if is_valid_value(aff_country):
                countries.append(str(aff_country).strip())
            if is_valid_value(aff_name):
                display_names.append(str(aff_name).strip())

        unique_ids = set(ids)
        unique_countries = set(countries)

        return pd.Series({
            "affiliation_data": len(ids) > 0 or len(countries) > 0 or len(display_names) > 0,
            "unique_affiliation_count": len(unique_ids),
            "all_usa": len(countries) > 0 and all(c == "United States" for c in countries),
            "some_usa": any(c == "United States" for c in countries),
            "all_china": len(countries) > 0 and all(c == "China" for c in countries),
            "some_china": any(c == "China" for c in countries),
            "unique_country_count": len(unique_countries),
            "all_countries": ", ".join(sorted(unique_countries)) if unique_countries else None,
            "Most Recent Affiliation": display_names[0] if display_names else None,
            "Most Recent Affiliation Country": countries[0] if countries else None,
        })

    analytics_df = author_df.apply(insert_analytics, axis=1)

    # Insert enrichment columns after i10_index
    insert_index = author_df.columns.get_loc("i10_index") + 1
    for col in reversed(analytics_df.columns):
        author_df.insert(insert_index, col, analytics_df[col])

    # Save cleaned and enriched output
    author_df.to_csv(author_path, index=False)
    print(f"Saved: {author_path}")

# Run for each main directory
for folder in main_dirs:
    process_folder(folder)