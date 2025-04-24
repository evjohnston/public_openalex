import pandas as pd
import requests
import time
from typing import Dict, List, Any, Optional
from collections import defaultdict

def get_author_data(oa_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch author data from OpenAlex API
    """
    print("Fetching data from OpenAlex API...")
    try:
        # Remove any 'https://openalex.org/authors/' prefix if present
        oa_id = oa_id.replace('https://openalex.org/authors/', '')
        url = f'https://api.openalex.org/people/{oa_id}'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data for {oa_id}: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error for {oa_id}: {str(e)}")
        return None

def process_affiliations(affiliations: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """
    Process affiliations data and organize by year
    """
    year_affiliations = {}
    no_year_affiliations = []

    for affiliation in affiliations:
        institution = affiliation.get('institution', {})
        years = affiliation.get('years', [])
        
        if not years:
            no_year_affiliations.append({
                'display_name': institution.get('display_name', ''),
                'id': institution.get('id', '')
            })
        else:
            for year in years:
                year_affiliations[str(year)] = {
                    'display_name': institution.get('display_name', ''),
                    'id': institution.get('id', '')
                }
    
    return {
        'by_year': year_affiliations,
        'no_year': no_year_affiliations
    }

def enrich_author_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich the input DataFrame with additional OpenAlex data
    """
    # Initialize lists to store new data
    new_data = []
    all_years = set()
    
    total_authors = len(df)
    print(f"\nProcessing {total_authors} authors...")
    print("-" * 50)

    # Process each author
    for index, row in df.iterrows():
        print(f"\nProcessing author {index + 1}/{total_authors}")
        print(f"Author: {row['Author']}")
        print(f"OpenAlex ID: {row['OA_ID']}")
        oa_id = row['OA_ID']
        author_data = get_author_data(oa_id)
        
        if not author_data:
            continue

        # Extract basic author information
        author_info = {
            'Author': row['Author'],
            'No. Papers': row['No. Papers'],
            'Notes': row['Notes'],
            'OA_Profile': row['OA_Profile'],
            'OA_ID': row['OA_ID'],
            'ORCID': author_data.get('orcid', ''),
            'Display_name': author_data.get('display_name', ''),
            'Display_name_alternatives': ', '.join(author_data.get('display_name_alternatives', [])),
            'works_count': author_data.get('works_count', 0),
            'cited_by_count': author_data.get('cited_by_count', 0)
        }

        # Extract summary statistics
        summary_stats = author_data.get('summary_stats', {})
        author_info.update({
            '2yr_mean_citedness': summary_stats.get('2yr_mean_citedness', 0),
            'h_index': summary_stats.get('h_index', 0),
            'i10_index': summary_stats.get('i10_index', 0)
        })

        # Process affiliations
        affiliations_data = process_affiliations(author_data.get('affiliations', []))
        
        # Add affiliations by year
        for year, affiliation in affiliations_data['by_year'].items():
            all_years.add(year)
            author_info[f'affiliation-{year}'] = affiliation['display_name']
            author_info[f'ID-{year}'] = affiliation['id']

        # Add affiliations with no year
        if affiliations_data['no_year']:
            author_info['affiliation-noyear'] = '; '.join(
                [aff['display_name'] for aff in affiliations_data['no_year']]
            )
            author_info['ID-noyear'] = '; '.join(
                [aff['id'] for aff in affiliations_data['no_year']]
            )

        new_data.append(author_info)
        
        # Add delay to respect API rate limits
        print("Waiting 1 second before next request...")
        time.sleep(.2)

    # Create new DataFrame with all columns
    result_df = pd.DataFrame(new_data)
    
    # Ensure all year columns exist (fill with empty strings if missing)
    for year in all_years:
        if f'affiliation-{year}' not in result_df.columns:
            result_df[f'affiliation-{year}'] = ''
        if f'ID-{year}' not in result_df.columns:
            result_df[f'ID-{year}'] = ''

    # Sort columns
    static_columns = ['Author', 'No. Papers', 'Notes', 'OA_Profile', 'OA_ID', 
                     'ORCID', 'Display_name', 'Display_name_alternatives',
                     'works_count', 'cited_by_count', '2yr_mean_citedness',
                     'h_index', 'i10_index']
    
    year_columns = sorted([col for col in result_df.columns 
                          if col.startswith(('affiliation-', 'ID-'))],
                         key=lambda x: ('noyear' in x, x))
    
    result_df = result_df[static_columns + year_columns]
    
    return result_df

def main():
    print("Starting OpenAlex Profile Scraper")
    print("=" * 50)
    
    print("\nReading input CSV file...")
    # Read input CSV
    input_df = pd.read_csv('authors_metadata.csv')
    print(f"Found {len(input_df)} authors in the input file")
    
    # Process the data
    enriched_df = enrich_author_data(input_df)
    
    # Save to new CSV
    output_filename = 'enriched_authors_metadata.csv'
    print(f"\nSaving results to {output_filename}...")
    enriched_df.to_csv(output_filename, index=False)
    print("\nData processing completed successfully!")
    print(f"- Input records processed: {len(input_df)}")
    print(f"- Output records created: {len(enriched_df)}")
    print(f"- Results saved to: {output_filename}")
    print("\nDone!")

if __name__ == "__main__":
    main()