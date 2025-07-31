import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Set

# 🔹 Change this one line to set the base folder
base_folder = "Google"

# Define all file paths dynamically
author_file = Path(base_folder) / "CSVs" / "authors_affiliations.csv"
institutions_file = Path(base_folder) / "CSVs" / "enriched_institutions.csv"
lineage_file = Path(base_folder) / "CSVs" / "institutions_lineage.csv"
output_file = Path(base_folder) / "CSVs" / "author_affiliations_enhanced.csv"

class AuthorAffiliationsAnalyzer:
    def __init__(self):
        """Initialize the analyzer"""
        self.author_df = None
        self.institutions_df = None
        self.lineage_df = None
        self.institution_country_map = {}

    def load_data(self, author_path: Path, institutions_path: Path, lineage_path: Path):
        """Load all required data files"""
        print("Loading data files...")
        self.author_df = pd.read_csv(author_path)
        self.institutions_df = pd.read_csv(institutions_path)
        self.lineage_df = pd.read_csv(lineage_path)

        # Create institution to country mapping
        self.institution_country_map = dict(zip(
            self.institutions_df['id'],
            self.institutions_df['country']
        ))

    def get_institution_country(self, institution_id: str) -> str:
        """Get country for an institution ID"""
        return self.institution_country_map.get(institution_id, '')

    def get_unique_countries(self, row: pd.Series) -> Set[str]:
        """Get unique countries from all affiliations"""
        countries = set()
        id_cols = [col for col in row.index if col.startswith('ID-') and not pd.isna(row[col])]

        for col in id_cols:
            if row[col]:
                country = self.get_institution_country(row[col])
                if country:
                    countries.add(country)
        return countries

    def analyze_affiliations(self) -> pd.DataFrame:
        """Analyze author affiliations and add country-based columns"""
        print("\nAnalyzing author affiliations...")

        years = sorted(list(set([
            col.split('-')[1] for col in self.author_df.columns
            if col.startswith('affiliation-') and col.split('-')[1].isdigit()
        ])), reverse=True)

        result_data = []
        total_authors = len(self.author_df)

        for idx, row in self.author_df.iterrows():
            print(f"Processing author {idx + 1}/{total_authors}")
            countries = self.get_unique_countries(row)

            author_data = {
                'Author': row['Author'],
                'OA_ID': row['OA_ID'],
                'unique_affiliation_count': row['unique_affiliation_count'],
                'all_china': len(countries) > 0 and all(c == 'China' for c in countries),
                'some_china': 'China' in countries,
                'some_usa': 'United States' in countries,
                'all_countries': '; '.join(sorted(countries)) if countries else ''
            }

            for year in years:
                aff_col = f'affiliation-{year}'
                id_col = f'ID-{year}'

                if aff_col in row.index and id_col in row.index:
                    author_data[f'affiliation_display_name_{year}'] = row[aff_col]
                    author_data[f'affiliation_id_{year}'] = row[id_col]
                    author_data[f'affiliation_country_{year}'] = (
                        self.get_institution_country(row[id_col]) if pd.notna(row[id_col]) else ''
                    )

            result_data.append(author_data)

        return pd.DataFrame(result_data)

    def generate_summary_statistics(self, df: pd.DataFrame):
        """Generate and print summary statistics"""
        print("\nSummary Statistics:")
        print(f"Total authors analyzed: {len(df)}")
        print(f"Authors with all Chinese affiliations: {df['all_china'].sum()}")
        print(f"Authors with some Chinese affiliations: {df['some_china'].sum()}")
        print(f"Authors with some US affiliations: {df['some_usa'].sum()}")

        countries = [c.strip() for countries in df['all_countries'].dropna()
                     for c in countries.split(';') if c.strip()]
        country_counts = pd.Series(countries).value_counts()

        print("\nTop 10 countries by author affiliations:")
        print(country_counts.head(10))

def main():
    print("Starting OpenAlex Author Affiliations Analysis")
    print("=" * 50)

    analyzer = AuthorAffiliationsAnalyzer()
    analyzer.load_data(author_file, institutions_file, lineage_file)

    result_df = analyzer.analyze_affiliations()

    print(f"\nSaving enhanced affiliations to {output_file}...")
    result_df.to_csv(output_file, index=False)

    analyzer.generate_summary_statistics(result_df)

    print("\nDone!")

if __name__ == "__main__":
    main()
