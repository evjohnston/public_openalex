import pandas as pd
import numpy as np
from pathlib import Path

# 🔹 Change this one line to set the base folder
base_folder = "KimiK2"

# Define file paths based on base folder
input_file = Path(base_folder) / "CSVs" / "enriched_authors_metadata.csv"
output_file = Path(base_folder) / "CSVs" / "authors_affiliations.csv"
institutions_file = Path(base_folder) / "CSVs" / "all_institutions.csv"

def count_unique_affiliations(row):
    """
    Count unique affiliations for an author based on unique institution IDs
    """
    id_cols = [col for col in row.index if col.startswith('ID-')]
    unique_ids = set(id for id in row[id_cols] if pd.notna(id) and id != '')
    return len(unique_ids)

def create_all_institutions_summary(df):
    """
    Create a summary of all unique institutions and their author counts
    """
    print("\nCreating institutions summary...")
    
    id_cols = [col for col in df.columns if col.startswith('ID-')]
    affiliation_cols = [col for col in df.columns if col.startswith('affiliation-')]
    
    institution_pairs = []
    for id_col, aff_col in zip(id_cols, affiliation_cols):
        pairs = df[[id_col, aff_col]].dropna()
        pairs = pairs[pairs[id_col] != '']
        pairs.columns = ['id', 'display_name']
        institution_pairs.append(pairs)
    
    all_institutions = pd.concat(institution_pairs, ignore_index=True)
    
    author_counts = {}
    for _, row in df.iterrows():
        unique_ids = set(id for id in row[id_cols] if pd.notna(id) and id != '')
        for id in unique_ids:
            author_counts[id] = author_counts.get(id, 0) + 1
    
    institutions_summary = (
        all_institutions
        .drop_duplicates(subset='id')
        .assign(author_count=lambda x: x['id'].map(author_counts))
        .sort_values('author_count', ascending=False)
    )
    
    return institutions_summary

def create_affiliations_spreadsheet(input_path: Path, output_path: Path, institutions_path: Path):
    """
    Create new spreadsheets focusing on affiliations data and institution summary
    """
    print(f"Reading input file: {input_path}")
    df = pd.read_csv(input_path)
    
    affiliation_cols = [col for col in df.columns if col.startswith('affiliation-')]
    id_cols = [col for col in df.columns if col.startswith('ID-')]
    
    years = sorted(list(set([col.split('-')[1] for col in affiliation_cols])), reverse=True)
    
    base_cols = ['Author', 'OA_ID']
    year_cols = []
    for year in years:
        year_cols.extend([f'affiliation-{year}', f'ID-{year}'])
    
    print("\nReorganizing columns chronologically...")
    affiliations_df = df[base_cols + year_cols].copy()
    
    print("Counting unique affiliations per author...")
    affiliations_df.insert(2, 'unique_affiliation_count',
                           affiliations_df.apply(count_unique_affiliations, axis=1))
    
    affiliation_counts = affiliations_df[
        [col for col in affiliations_df.columns if col.startswith('affiliation-')]
    ].notna().sum(axis=1)
    
    print("\nGenerating statistics...")
    print(f"Total number of authors: {len(affiliations_df)}")
    print(f"Authors with at least one affiliation: {(affiliation_counts > 0).sum()}")
    print(f"Authors with no affiliations: {(affiliation_counts == 0).sum()}")
    print(f"Year range: {years[-1]} to {years[0]}")
    
    institutions_df = create_all_institutions_summary(affiliations_df)
    
    print(f"\nSaving authors affiliations to {output_path}...")
    affiliations_df.to_csv(output_path, index=False)
    
    print(f"Saving institutions summary to {institutions_path}...")
    institutions_df.to_csv(institutions_path, index=False)
    
    print("\nSummary of institutions:")
    print(f"Total unique institutions: {len(institutions_df)}")
    print(f"Maximum authors per institution: {institutions_df['author_count'].max()}")
    print(f"Average authors per institution: {institutions_df['author_count'].mean():.2f}")
    
    print("\nDone!")
    return affiliations_df, institutions_df

def main():
    print("Starting OpenAlex Institutions Scraper")
    print("=" * 50)
    
    create_affiliations_spreadsheet(input_file, output_file, institutions_file)

if __name__ == "__main__":
    main()
