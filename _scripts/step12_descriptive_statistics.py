import pandas as pd
import sys, os
import numpy as np
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import base_folder

authors_file = os.path.join(base_folder, "CSVs", "enriched_authors_metadata.csv")

df = pd.read_csv(authors_file)

target_vars = [
    "works_count",
    "cited_by_count",
    "2yr_mean_citedness",
    "h_index",
    "i10_index",
    "unique_affiliation_count",
    "unique_country_count",
    "all_china",
    "all_usa",
    "some_china",
    "some_usa",
    "affiliation_data",
]

# Keep only columns that actually exist in the data
present = [c for c in target_vars if c in df.columns]
missing = [c for c in target_vars if c not in df.columns]

if missing:
    print(f"Columns not found in CSV (skipped): {missing}")

print(f"Columns found: {present}\n")

numeric_cols = []
boolean_cols = []

for col in present:
    if df[col].dtype in ["bool", "object"]:
        unique_vals = set(df[col].dropna().unique())
        if unique_vals <= {True, False, "True", "False"}:
            df[col] = df[col].map({"True": True, "False": False, True: True, False: False}).astype(float)
            boolean_cols.append(col)
        else:
            continue
    numeric_cols.append(col)

all_numeric = numeric_cols + boolean_cols
all_numeric = list(dict.fromkeys(all_numeric))

# Columns that only make sense when affiliation data was actually present
affiliation_filtered_cols = {"all_china", "some_china", "some_usa", "all_usa", "unique_affiliation_count", "unique_country_count"}

# Build the affiliation mask once
if "affiliation_data" in df.columns:
    affiliation_mask = df["affiliation_data"] == 1.0  # already converted to float above
else:
    affiliation_mask = pd.Series(True, index=df.index)

stats_rows = []

for col in present:
    row = {"variable": col}

    # Use filtered series for the country flag columns
    if col in affiliation_filtered_cols:
        series = df.loc[affiliation_mask, col]
    else:
        series = df[col]

    if col in all_numeric:
        s = pd.to_numeric(series, errors="coerce")
        row["count"] = int(s.count())
        row["missing"] = int(s.isna().sum())
        row["mean"] = s.mean()
        row["std"] = s.std()
        row["min"] = s.min()
        row["25th_percentile"] = s.quantile(0.25)
        row["median_50th"] = s.quantile(0.50)
        row["75th_percentile"] = s.quantile(0.75)
        row["max"] = s.max()
        row["sum"] = s.sum()
        row["skewness"] = s.skew()
        row["kurtosis"] = s.kurtosis()

        if col in boolean_cols:
            row["pct_true"] = s.mean() * 100
            row["pct_false"] = (1 - s.mean()) * 100

    else:
        row["count"] = int(series.count())
        row["missing"] = int(series.isna().sum())
        row["unique_values"] = series.nunique()
        row["most_common"] = series.mode().iloc[0] if not series.mode().empty else np.nan
        row["most_common_freq"] = int(series.value_counts().iloc[0]) if series.count() > 0 else np.nan

    stats_rows.append(row)

stats_df = pd.DataFrame(stats_rows)

tables_dir = "tables"
os.makedirs(tables_dir, exist_ok=True)

folder_name = os.path.basename(os.path.normpath(base_folder))
output_path = os.path.join(tables_dir, f"{folder_name}_descriptive_statistics.csv")
stats_df.to_csv(output_path, index=False)
print(f"Saved to: {output_path}")
print()
print(stats_df.to_string(index=False))