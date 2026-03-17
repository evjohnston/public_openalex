import pandas as pd
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import base_folder

# --------------------------------------------------
# FILE PATHS
# --------------------------------------------------

authors_file = os.path.join(base_folder, "CSVs", "enriched_authors_metadata.csv")
aff_file = os.path.join(base_folder, "CSVs", "author_affiliations_enhanced.csv")

years = list(range(1983, 2027))

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

authors = pd.read_csv(authors_file)
aff = pd.read_csv(aff_file)

# --------------------------------------------------
# REMOVE OLD AFFILIATION FORMAT
# --------------------------------------------------

legacy_cols = []

for y in years:
    legacy_cols += [f"ID-{y}", f"affiliation-{y}"]

authors = authors.drop(columns=[c for c in legacy_cols if c in authors.columns], errors="ignore")

# --------------------------------------------------
# MERGE
# --------------------------------------------------

df = authors.merge(
    aff,
    on="OA_ID",
    how="left",
    suffixes=("", "_aff")
)

# --------------------------------------------------
# COPY ENHANCED AFFILIATION COLUMNS
# --------------------------------------------------

for y in years:

    id_col = f"affiliation_id_{y}"
    name_col = f"affiliation_display_name_{y}"
    country_col = f"affiliation_country_{y}"

    if f"{id_col}_aff" in df.columns:
        df[id_col] = df[f"{id_col}_aff"]

    if f"{name_col}_aff" in df.columns:
        df[name_col] = df[f"{name_col}_aff"]

    if f"{country_col}_aff" in df.columns:
        df[country_col] = df[f"{country_col}_aff"]

# Remove temporary merge columns
df = df.drop(columns=[c for c in df.columns if c.endswith("_aff")])

# --------------------------------------------------
# BUILD COLUMN LISTS
# --------------------------------------------------

id_cols = [f"affiliation_id_{y}" for y in years if f"affiliation_id_{y}" in df.columns]
country_cols = [f"affiliation_country_{y}" for y in years if f"affiliation_country_{y}" in df.columns]
name_cols = [f"affiliation_display_name_{y}" for y in years if f"affiliation_display_name_{y}" in df.columns]

# --------------------------------------------------
# NORMALIZE EMPTY STRINGS TO NaN
# (step8 writes '' for missing affiliations; standardize to NaN)
# --------------------------------------------------

df[id_cols] = df[id_cols].replace('', pd.NA)
df[country_cols] = df[country_cols].replace('', pd.NA)
df[name_cols] = df[name_cols].replace('', pd.NA)

# --------------------------------------------------
# FAST ANALYTICS
# --------------------------------------------------

df["affiliation_data"] = df[id_cols].notna().any(axis=1)

# Count unique non-NaN values only
df["unique_affiliation_count"] = df[id_cols].apply(
    lambda row: row.dropna().nunique(), axis=1
)

df["unique_country_count"] = df[country_cols].apply(
    lambda row: row.dropna().nunique(), axis=1
)

df["some_usa"] = (df[country_cols] == "United States").any(axis=1)
df["some_china"] = (df[country_cols] == "China").any(axis=1)

# FIX: check that all NON-EMPTY country entries are China/USA,
# not that every year column (including blank years) is China/USA
country_is_china = df[country_cols] == "China"
country_is_usa = df[country_cols] == "United States"
country_is_empty = df[country_cols].isna()

df["all_china"] = (country_is_china | country_is_empty).all(axis=1) & df["affiliation_data"]
df["all_usa"] = (country_is_usa | country_is_empty).all(axis=1) & df["affiliation_data"]

# --------------------------------------------------
# ALL COUNTRIES
# --------------------------------------------------

def combine_countries(row):
    vals = sorted(set(str(c).strip() for c in row if pd.notna(c) and str(c).strip()))
    return ", ".join(vals) if vals else None

df["all_countries"] = df[country_cols].apply(combine_countries, axis=1)

# --------------------------------------------------
# MOST RECENT AFFILIATION YEAR
# --------------------------------------------------

recent_year = df[id_cols].notna().iloc[:, ::-1].idxmax(axis=1)

year_series = recent_year.str.extract(r"(\d{4})")[0]

df["Most Recent Affiliation Year"] = pd.to_numeric(year_series, errors="coerce").astype("Int64")

# blank out rows with no affiliation
df.loc[~df["affiliation_data"], "Most Recent Affiliation Year"] = pd.NA

# --------------------------------------------------
# MOST RECENT NAME + COUNTRY
# --------------------------------------------------

def get_recent_value(row, cols):
    for c in reversed(cols):
        val = row.get(c)
        if pd.notna(val):
            return val
    return None

df["Most Recent Affiliation"] = df.apply(lambda r: get_recent_value(r, name_cols), axis=1)
df["Most Recent Affiliation Country"] = df.apply(lambda r: get_recent_value(r, country_cols), axis=1)

# --------------------------------------------------
# ORDER AFFILIATION COLUMNS
# --------------------------------------------------

ordered_aff_cols = []

for y in years:

    cols = [
        f"affiliation_id_{y}",
        f"affiliation_country_{y}",
        f"affiliation_display_name_{y}"
    ]

    ordered_aff_cols += [c for c in cols if c in df.columns]

other_cols = [c for c in df.columns if c not in ordered_aff_cols]

df = df[other_cols + ordered_aff_cols]

# --------------------------------------------------
# SAVE
# --------------------------------------------------

df.to_csv(authors_file, index=False)

# --------------------------------------------------
# VERIFICATION
# --------------------------------------------------

print("Affiliations merged, analytics rebuilt, columns sorted, and year formatting fixed.")
print(f"\nVerification:")
print(f"  Total authors: {len(df)}")
print(f"  Authors with affiliation data: {df['affiliation_data'].sum()}")
print(f"  all_china=True: {df['all_china'].sum()}")
print(f"  all_usa=True: {df['all_usa'].sum()}")
print(f"  some_china=True: {df['some_china'].sum()}")
print(f"  some_usa=True: {df['some_usa'].sum()}")