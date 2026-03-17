# %% [Setup and Imports]
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
import sys, os
import pycountry
import warnings

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from collections import Counter
from config import base_folder

warnings.simplefilter(action='ignore', category=FutureWarning)

# Setup
sns.set(style="whitegrid")
plt.rcParams['figure.dpi'] = 150

# Directories and Filenames
CSV_SUBDIR = 'CSVs'
METADATA_FILE = 'enriched_authors_metadata.csv'
TRAJECTORY_FILE = 'author_career_trajectory.csv'
VIS_DIR = 'vis'

os.makedirs(VIS_DIR, exist_ok=True)

# Load data
trajectory_path = os.path.join(base_folder, CSV_SUBDIR, TRAJECTORY_FILE)
metadata_path = os.path.join(base_folder, CSV_SUBDIR, METADATA_FILE)

print(f"Looking for:\n- {trajectory_path}\n- {metadata_path}")
if not os.path.exists(trajectory_path):
    raise ValueError(f"❌ Missing: {trajectory_path}")
if not os.path.exists(metadata_path):
    raise ValueError(f"❌ Missing: {metadata_path}")

traj_df = pd.read_csv(trajectory_path)
traj_df.columns = traj_df.columns.str.strip()
meta_df = pd.read_csv(metadata_path)
meta_df.columns = meta_df.columns.str.strip()

combined_df = pd.merge(traj_df, meta_df[['Author', 'No. Papers', 'Most Recent Affiliation Country', 'OA_Profile']], on='Author', how='left')
combined_df['Org'] = base_folder

# Compute Foreign Experience Years from metadata columns
foreign_cols = [col for col in meta_df.columns if col.startswith('affiliation_country_')]
if foreign_cols:
    combined_df['Foreign Experience Years'] = meta_df[foreign_cols].apply(
        lambda row: sum(1 for val in row if isinstance(val, str) and val != 'United States'), axis=1
    )
else:
    combined_df['Foreign Experience Years'] = 0

# Utility
def save_fig(fig, name):
    fig.savefig(os.path.join(VIS_DIR, f"{name}.png"), bbox_inches='tight')
    plt.close(fig)

def get_prefix(org):
    return org.lower() + "_"

# %% [Visualizations]
org_df = combined_df.copy()
org_df.columns = org_df.columns.str.strip()
prefix = get_prefix(base_folder)

# Author Distribution by Number of Papers
fig, ax = plt.subplots()
sns.histplot(data=org_df, x='No. Papers', bins=range(1, org_df['No. Papers'].max() + 2), ax=ax, hue=None, legend=False)
ax.set_title(f"{base_folder}: Author Distribution by Number of Papers")
ax.set_xlabel("No. Papers")
ax.set_ylabel("Number of Authors")
save_fig(fig, f"{prefix}author_papers_distribution")

# Geographic Trajectory (even more simplified)
fig, ax = plt.subplots(figsize=(8, 5))
sns.countplot(data=org_df, y='Geographic Trajectory (even more simplified)',
              order=org_df['Geographic Trajectory (even more simplified)'].value_counts().index[:10], ax=ax, hue=None, legend=False)
ax.set_title(f"{base_folder}: Geographic Trajectory End States")
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
ax.set_title(f"{base_folder}: Foreign Experience (Years outside US)")
ax.set_xlabel("Years Abroad")
save_fig(fig, f"{prefix}foreign_experience_duration")

# US Retention
first_year = min(map(int, latest_year_cols))
last_year = max(map(int, latest_year_cols))
us_retention = ((org_df[str(first_year)] == 'United States') & (org_df[str(last_year)] == 'United States')).mean()
us_df = pd.DataFrame({'Org': [base_folder], 'US Retention Rate': [round(us_retention * 100, 2)]})

# Top 10 Countries by Most Recent Affiliation
most_recent_col = 'Most Recent Affiliation Country'
top_countries = org_df[most_recent_col].value_counts().nlargest(10).reset_index()
top_countries.columns = ['Country', 'Author Count']

fig, ax = plt.subplots(figsize=(8, 5))
sns.barplot(data=top_countries, x='Author Count', y='Country', palette='viridis', ax=ax, hue=None, legend=False)
ax.set_title(f"{base_folder}: Top 10 Countries by Most Recent Affiliation")
save_fig(fig, f"{prefix}top_10_countries_bar")