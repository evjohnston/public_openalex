# %% [Setup and Imports]
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
import pycountry
from collections import Counter
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


# Setup
sns.set(style="whitegrid")
plt.rcParams['figure.dpi'] = 150

# Directories and Filenames
ORG_DIRS = ['Anthropic', 'Google', 'OpenAI']
CSV_SUBDIR = 'CSVs'
METADATA_FILE = 'enriched_authors_metadata.csv'
TRAJECTORY_FILE = 'author_career_trajectory.csv'
VIS_DIR = 'vis'
TABLE_DIR = 'tables'

os.makedirs(VIS_DIR, exist_ok=True)
os.makedirs(TABLE_DIR, exist_ok=True)

# Load data
dfs = []
for org in ORG_DIRS:
    trajectory_path = os.path.join(org, CSV_SUBDIR, TRAJECTORY_FILE)
    metadata_path = os.path.join(org, CSV_SUBDIR, METADATA_FILE)

    print(f"Looking for:\n- {trajectory_path}\n- {metadata_path}")
    if not os.path.exists(trajectory_path):
        print(f"\u274c Missing: {trajectory_path}")
        continue
    if not os.path.exists(metadata_path):
        print(f"\u274c Missing: {metadata_path}")
        continue

    traj_df = pd.read_csv(trajectory_path)
    traj_df.columns = traj_df.columns.str.strip()
    meta_df = pd.read_csv(metadata_path)
    meta_df.columns = meta_df.columns.str.strip()

    df = pd.merge(traj_df, meta_df[['Author', 'No. Papers', 'Most Recent Affiliation Country']], on='Author', how='left')
    df['Org'] = org
    dfs.append(df)
    
    # Compute Foreign Experience Years from metadata columns
    foreign_cols = [col for col in meta_df.columns if col.startswith('affiliation_country_')]
    if foreign_cols:
        df['Foreign Experience Years'] = meta_df[foreign_cols].apply(
            lambda row: sum(1 for val in row if isinstance(val, str) and val != 'United States'), axis=1
        )
    else:
        df['Foreign Experience Years'] = 0
        
    dfs.append(df)

if not dfs:
    raise ValueError("\u274c No data loaded. Ensure your file structure and names are correct.")

combined_df = pd.concat(dfs, ignore_index=True)

# Utility

def save_fig(fig, name):
    fig.savefig(os.path.join(VIS_DIR, f"{name}.png"), bbox_inches='tight')
    plt.close(fig)

def save_table(df, name):
    df.to_html(os.path.join(TABLE_DIR, f"{name}.html"), index=False)

def get_prefix(org):
    return org.lower() + "_"

# %% [Visualizations Per Organization]
for org in ORG_DIRS:
    org_df = combined_df[combined_df['Org'] == org].copy()
    org_df.columns = org_df.columns.str.strip()
    prefix = get_prefix(org)

    # Author Distribution by Number of Papers
    fig, ax = plt.subplots()
    sns.histplot(data=org_df, x='No. Papers', bins=range(1, org_df['No. Papers'].max() + 2), ax=ax, hue=None, legend=False)
    ax.set_title(f"{org}: Author Distribution by Number of Papers")
    ax.set_xlabel("No. Papers")
    ax.set_ylabel("Number of Authors")
    save_fig(fig, f"{prefix}author_papers_distribution")

    # Geographic Trajectory (even more simplified)
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.countplot(data=org_df, y='Geographic Trajectory (even more simplified)',
                  order=org_df['Geographic Trajectory (even more simplified)'].value_counts().index[:10], ax=ax, hue=None, legend=False)
    ax.set_title(f"{org}: Geographic Trajectory End States")
    ax.set_xlabel("Author Count")
    save_fig(fig, f"{prefix}geographic_trajectory_end_states")

    # Foreign Experience
    latest_year_cols = [c for c in org_df.columns if c.isnumeric()]
    def foreign_years(row):
        years = [c for c in latest_year_cols if c in row and isinstance(row[c], str)]
        return sum(1 for y in years if row[y] != 'United States')
    org_df['Foreign Experience Years'] = org_df.apply(foreign_years, axis=1)

    fig, ax = plt.subplots()
    sns.histplot(data=org_df, x='Foreign Experience Years', bins=10, ax=ax, hue=None, legend=False)
    ax.set_title(f"{org}: Foreign Experience (Years outside US)")
    ax.set_xlabel("Years Abroad")
    save_fig(fig, f"{prefix}foreign_experience_duration")

    # US Retention
    first_year = min(map(int, latest_year_cols))
    last_year = max(map(int, latest_year_cols))
    us_retention = ((org_df[str(first_year)] == 'United States') & (org_df[str(last_year)] == 'United States')).mean()
    us_df = pd.DataFrame({'Org': [org], 'US Retention Rate': [round(us_retention * 100, 2)]})

    # Top 10 Countries by Most Recent Affiliation
    most_recent_col = 'Most Recent Affiliation Country'
    top_countries = org_df[most_recent_col].value_counts().nlargest(10).reset_index()
    top_countries.columns = ['Country', 'Author Count']

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=top_countries, x='Author Count', y='Country', palette='viridis', ax=ax, hue=None, legend=False)
    ax.set_title(f"{org}: Top 10 Countries by Most Recent Affiliation")
    save_fig(fig, f"{prefix}top_10_countries_bar")

# %% [Combined: Author Distribution Across Papers]
fig, ax = plt.subplots(figsize=(8, 5))
sns.histplot(data=combined_df, x='No. Papers', hue='Org', multiple='stack',
             bins=range(1, combined_df['No. Papers'].max() + 2), ax=ax)
ax.set_title("Author Distribution by Number of Papers")
ax.set_xlabel("No. Papers")
ax.set_ylabel("Number of Authors")
save_fig(fig, "all_author_papers_distribution")

# %% [Combined: Trajectory End States]
fig, ax = plt.subplots(figsize=(8, 5))
sns.countplot(data=combined_df, y='Geographic Trajectory (even more simplified)', hue='Org',
              order=combined_df['Geographic Trajectory (even more simplified)'].value_counts().index[:10], ax=ax)
ax.set_title("Geographic Trajectory End States by Org")
ax.set_xlabel("Author Count")
save_fig(fig, "all_geographic_trajectory_end_states")

# %% [Mega Table: Retention Rate + Author Count + Foreign Years]
def calculate_retention(df):
    return df['Geographic Trajectory (even more simplified)'].fillna('').str.endswith('United States').mean()

mega_retention = []
for org in ORG_DIRS + ['All']:
    df = combined_df if org == 'All' else combined_df[combined_df['Org'] == org]
    retention = round(calculate_retention(df) * 100, 2)
    unique_authors = df['Author'].nunique()
    foreign_mean = round(df['Foreign Experience Years'].mean(), 2)
    foreign_median = round(df['Foreign Experience Years'].median(), 2)

    mega_retention.append({
        'Org': org,
        'US Retention Rate': retention,
        'Unique Authors': unique_authors,
        'Foreign Years (Mean)': foreign_mean,
        'Foreign Years (Median)': foreign_median
    })

retention_df = pd.DataFrame(mega_retention)
save_table(retention_df, "mega_retention_author_table")

# %% [Mega Table: Top 10 Countries]
most_recent_col = 'Most Recent Affiliation Country'

mega_countries = pd.DataFrame()
for org in ORG_DIRS + ['All']:
    df = combined_df if org == 'All' else combined_df[combined_df['Org'] == org]
    top_countries = df[most_recent_col].value_counts().nlargest(10).reset_index()
    top_countries.columns = [f"Top 10 Countries ({org})", f"Count ({org})"]
    if mega_countries.empty:
        mega_countries = top_countries
    else:
        mega_countries = pd.concat([mega_countries, top_countries], axis=1)

save_table(mega_countries, "mega_top10_countries")

# %% [Unified Summary Table — One Table, Averages and Medians Combined]
summary_cols = [
    'works_count', 'cited_by_count', '2yr_mean_citedness',
    'h_index', 'i10_index', 'unique_affiliation_count', 'unique_country_count'
]
flag_cols = ['all_usa', 'some_usa', 'all_china', 'some_china']

def filter_existing_cols(df, cols):
    existing = [col for col in cols if col in df.columns]
    missing = set(cols) - set(existing)
    if missing:
        print(f"⚠️ Missing columns in DataFrame: {missing}")
    return existing

# Store results in a dictionary
org_summaries = {}

# Process each org
for org in ORG_DIRS:
    metadata_path = os.path.join(org, CSV_SUBDIR, METADATA_FILE)
    if not os.path.exists(metadata_path):
        print(f"⚠️ Skipping summary stats for {org}: metadata file missing.")
        continue

    df = pd.read_csv(metadata_path)
    df.columns = df.columns.str.strip()

    existing_summary_cols = filter_existing_cols(df, summary_cols)
    row_data = {}

    for col in existing_summary_cols:
        mean_val = round(df[col].mean(), 2)
        median_val = round(df[col].median(), 2)
        row_data[col] = f"{mean_val} ({median_val})"

    existing_flag_cols = filter_existing_cols(df, flag_cols)
    for flag in existing_flag_cols:
        total = int(df[flag].sum())
        row_data[flag] = str(total)

    org_summaries[org] = row_data

# Combine metadata for "All"
all_metadata = []
for org in ORG_DIRS:
    metadata_path = os.path.join(org, CSV_SUBDIR, METADATA_FILE)
    if os.path.exists(metadata_path):
        df = pd.read_csv(metadata_path)
        df.columns = df.columns.str.strip()
        all_metadata.append(df)

if all_metadata:
    combined_df = pd.concat(all_metadata, ignore_index=True)
    row_data = {}

    existing_summary_cols = filter_existing_cols(combined_df, summary_cols)
    for col in existing_summary_cols:
        mean_val = round(combined_df[col].mean(), 2)
        median_val = round(combined_df[col].median(), 2)
        row_data[col] = f"{mean_val} ({median_val})"

    existing_flag_cols = filter_existing_cols(combined_df, flag_cols)
    for flag in existing_flag_cols:
        total = int(combined_df[flag].sum())
        row_data[flag] = str(total)

    org_summaries['All'] = row_data
else:
    print("❌ No metadata files found for combined summary.")

# Convert to a single DataFrame
summary_table = pd.DataFrame.from_dict(org_summaries, orient='index')
summary_table.index.name = 'Org'
summary_table.reset_index(inplace=True)

# Save
save_table(summary_table, "unified_summary_table")
