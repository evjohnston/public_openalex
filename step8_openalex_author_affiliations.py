import pandas as pd
import numpy as np
from typing import Dict, List, Set

class AuthorAffiliationsAnalyzer:
    def __init__(self):
        """Initialize the analyzer"""
        self.institutions_data = None
        self.lineage_data = None
        self.institution_country_map = {}
        
    def load_data(self, author_file: str, institutions_file: str, lineage_file: str):
        """Load all required data files"""
        print("Loading data files...")
        self.author_df = pd.read_csv(author_file)
        self.institutions_df = pd.read_csv(institutions_file)
        self.lineage_df = pd.read_csv(lineage_file)
        
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
        
        # Get all ID columns
        id_cols = [col for col in row.index if col.startswith('ID-') and not pd.isna(row[col])]
        
        for col in id_cols:
            if row[col]:  # Check if the ID exists
                country = self.get_institution_country(row[col])
                if country:  # Only add non-empty country values
                    countries.add(country)
        
        return countries
    
    def analyze_affiliations(self) -> pd.DataFrame:
        """Analyze author affiliations and add country-based columns"""
        print("\nAnalyzing author affiliations...")
        
        # Get all year-based columns
        year_cols = []
        all_cols = self.author_df.columns
        years = sorted(list(set([
            col.split('-')[1] for col in all_cols 
            if col.startswith('affiliation-') and col.split('-')[1].isdigit()
        ])), reverse=True)
        
        # Initialize new DataFrame with base columns
        result_data = []
        total_authors = len(self.author_df)
        
        for idx, row in self.author_df.iterrows():
            print(f"Processing author {idx + 1}/{total_authors}")
            
            # Get unique countries for this author
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
            
            # Add year-based affiliation information
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
        
        # Create DataFrame from processed data
        result_df = pd.DataFrame(result_data)
        
        return result_df
    
    def generate_summary_statistics(self, df: pd.DataFrame):
        """Generate and print summary statistics"""
        print("\nSummary Statistics:")
        print(f"Total authors analyzed: {len(df)}")
        print(f"Authors with all Chinese affiliations: {df['all_china'].sum()}")
        print(f"Authors with some Chinese affiliations: {df['some_china'].sum()}")
        print(f"Authors with some US affiliations: {df['some_usa'].sum()}")
        
        # Calculate country distribution
        countries = [c.strip() for countries in df['all_countries'].dropna() 
                    for c in countries.split(';') if c.strip()]
        country_counts = pd.Series(countries).value_counts()
        
        print("\nTop 10 countries by author affiliations:")
        print(country_counts.head(10))

def main():
    input_files = {
        'author': 'authors_affiliations.csv',
        'institutions': 'enriched_institutions.csv',
        'lineage': 'institutions_lineage.csv'
    }
    output_file = 'author_affiliations_enhanced.csv'
    
    print("Starting OpenAlex Author Affiliations Analysis")
    print("=" * 50)
    
    # Initialize and run analysis
    analyzer = AuthorAffiliationsAnalyzer()
    analyzer.load_data(
        input_files['author'],
        input_files['institutions'],
        input_files['lineage']
    )
    
    # Process the data
    result_df = analyzer.analyze_affiliations()
    
    # Save results
    print(f"\nSaving enhanced affiliations to {output_file}...")
    result_df.to_csv(output_file, index=False)
    
    # Generate summary statistics
    analyzer.generate_summary_statistics(result_df)
    
    print("\nDone!")

if __name__ == "__main__":
    main()