import pandas as pd
import numpy as np

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
    
    # Get all ID columns
    id_cols = [col for col in df.columns if col.startswith('ID-')]
    affiliation_cols = [col for col in df.columns if col.startswith('affiliation-')]
    
    # Create pairs of institution IDs and names
    institution_pairs = []
    for id_col, aff_col in zip(id_cols, affiliation_cols):
        pairs = df[[id_col, aff_col]].dropna()
        pairs = pairs[pairs[id_col] != '']  # Remove empty strings
        pairs.columns = ['id', 'display_name']
        institution_pairs.append(pairs)
    
    # Combine all pairs
    all_institutions = pd.concat(institution_pairs, ignore_index=True)
    
    # Count unique authors per institution
    author_counts = {}
    for _, row in df.iterrows():
        unique_ids = set(id for id in row[id_cols] if pd.notna(id) and id != '')
        for id in unique_ids:
            author_counts[id] = author_counts.get(id, 0) + 1
    
    # Create final summary
    institutions_summary = (all_institutions
        .drop_duplicates(subset='id')
        .assign(author_count=lambda x: x['id'].map(author_counts))
        .sort_values('author_count', ascending=False)
    )
    
    return institutions_summary

def create_affiliations_spreadsheet(input_file: str, output_file: str, institutions_file: str):
    """
    Create new spreadsheets focusing on affiliations data and institution summary
    """
    print(f"Reading input file: {input_file}")
    df = pd.read_csv(input_file)
    
    # Get all year-related columns
    affiliation_cols = [col for col in df.columns if col.startswith('affiliation-')]
    id_cols = [col for col in df.columns if col.startswith('ID-')]
    
    # Extract years and sort in descending order
    years = sorted(list(set([col.split('-')[1] for col in affiliation_cols])), reverse=True)
    
    # Create list of columns in desired order
    base_cols = ['Author', 'OA_ID']
    year_cols = []
    for year in years:
        year_cols.extend([f'affiliation-{year}', f'ID-{year}'])
    
    # Create new dataframe with selected columns
    print("\nReorganizing columns chronologically...")
    affiliations_df = df[base_cols + year_cols].copy()
    
    # Add affiliation count column
    print("Counting unique affiliations per author...")
    affiliations_df.insert(2, 'unique_affiliation_count', 
                         affiliations_df.apply(count_unique_affiliations, axis=1))
    
    # Count non-null affiliations per author
    affiliation_counts = affiliations_df[[col for col in affiliations_df.columns 
                                        if col.startswith('affiliation-')]].notna().sum(axis=1)
    
    print("\nGenerating statistics...")
    print(f"Total number of authors: {len(affiliations_df)}")
    print(f"Authors with at least one affiliation: {(affiliation_counts > 0).sum()}")
    print(f"Authors with no affiliations: {(affiliation_counts == 0).sum()}")
    print(f"Year range: {years[-1]} to {years[0]}")
    
    # Create institutions summary
    institutions_df = create_all_institutions_summary(affiliations_df)
    
    # Save to CSV files
    print(f"\nSaving authors affiliations to {output_file}...")
    affiliations_df.to_csv(output_file, index=False)
    
    print(f"Saving institutions summary to {institutions_file}...")
    institutions_df.to_csv(institutions_file, index=False)
    
    print("\nSummary of institutions:")
    print(f"Total unique institutions: {len(institutions_df)}")
    print(f"Maximum authors per institution: {institutions_df['author_count'].max()}")
    print(f"Average authors per institution: {institutions_df['author_count'].mean():.2f}")
    
    print("\nDone!")
    
    return affiliations_df, institutions_df

def main():
    input_file = 'enriched_authors_metadata.csv'
    output_file = 'authors_affiliations.csv'
    institutions_file = 'all_institutions.csv'
    
    print("Starting OpenAlex Institutions Scraper")
    print("=" * 50)
    
    affiliations_df, institutions_df = create_affiliations_spreadsheet(
        input_file, output_file, institutions_file)

if __name__ == "__main__":
    main()