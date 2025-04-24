import pandas as pd
import requests
import time
from typing import Dict, List, Any, Optional

def get_institution_data(institution_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch institution data from OpenAlex API
    """
    try:
        # Transform OpenAlex ID to API URL
        api_id = institution_id.replace('https://openalex.org/', '')
        url = f'https://api.openalex.org/institutions/{api_id}'
        
        print(f"Fetching data for {institution_id}")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data for {institution_id}: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error for {institution_id}: {str(e)}")
        return None

def process_associated_institutions(associated_institutions: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    Process associated institutions and separate them by relationship type
    """
    children = []
    children_ids = []
    parents = []
    parents_ids = []
    
    if associated_institutions:
        for inst in associated_institutions:
            if inst.get('relationship') == 'child':
                children.append(inst.get('display_name', ''))
                children_ids.append(inst.get('id', ''))
            elif inst.get('relationship') == 'parent':
                parents.append(inst.get('display_name', ''))
                parents_ids.append(inst.get('id', ''))
    
    return {
        'children': '; '.join(children) if children else '',
        'children_ids': '; '.join(children_ids) if children_ids else '',
        'parents': '; '.join(parents) if parents else '',
        'parents_ids': '; '.join(parents_ids) if parents_ids else ''
    }

def enrich_institutions_data(input_file: str, output_file: str):
    """
    Enrich institutions data with additional metadata from OpenAlex
    """
    print(f"Reading input file: {input_file}")
    df = pd.read_csv(input_file)
    
    # Initialize lists to store new data
    enriched_data = []
    
    total_institutions = len(df)
    print(f"\nProcessing {total_institutions} institutions...")
    
    for idx, row in df.iterrows():
        print(f"\nProcessing institution {idx + 1}/{total_institutions}")
        institution_data = get_institution_data(row['id'])
        
        if not institution_data:
            continue
            
        # Extract base institution data
        institution_info = {
            'id': row['id'],
            'author_count': row['author_count'],
            'display_name': institution_data.get('display_name', ''),
            'type': institution_data.get('type', ''),
            'country': institution_data.get('geo', {}).get('country', ''),
            'city': institution_data.get('geo', {}).get('city', ''),
            'display_name_acronyms': '; '.join(institution_data.get('display_name_acronyms', [])),
            'display_name_alternatives': '; '.join(institution_data.get('display_name_alternatives', [])),
            'works_count': institution_data.get('works_count', 0),
            'cited_by_count': institution_data.get('cited_by_count', 0)
        }
        
        # Extract summary statistics
        summary_stats = institution_data.get('summary_stats', {})
        institution_info.update({
            '2yr_mean_citedness': summary_stats.get('2yr_mean_citedness', 0),
            'h_index': summary_stats.get('h_index', 0),
            'i10_index': summary_stats.get('i10_index', 0)
        })
        
        # Extract Wikipedia URL
        ids = institution_data.get('ids', {})
        institution_info['wikipedia'] = ids.get('wikipedia', '')
        
        # Process associated institutions
        associated = process_associated_institutions(
            institution_data.get('associated_institutions', [])
        )
        institution_info.update({
            'associated_institutions_children': associated['children'],
            'children_ids': associated['children_ids'],
            'associated_institutions_parent': associated['parents'],
            'parent_ids': associated['parents_ids']
        })
        
        enriched_data.append(institution_info)
        
        # Add delay to respect API rate limits
        print("Waiting 1 second before next request...")
        time.sleep(.005)
    
    # Create new DataFrame with enriched data
    enriched_df = pd.DataFrame(enriched_data)
    
    # Save to CSV
    print(f"\nSaving enriched data to {output_file}...")
    enriched_df.to_csv(output_file, index=False)
    print("Done!")
    
    return enriched_df

def main():
    input_file = 'all_institutions.csv'
    output_file = 'enriched_institutions.csv'
    
    print("Starting OpenAlex Institutions Enrichment")
    print("=" * 50)
    
    enriched_df = enrich_institutions_data(input_file, output_file)
    
    print("\nSummary statistics:")
    print(f"Total institutions processed: {len(enriched_df)}")
    print(f"Total works: {enriched_df['works_count'].sum():,}")
    print(f"Total citations: {enriched_df['cited_by_count'].sum():,}")
    print(f"Average h-index: {enriched_df['h_index'].mean():.2f}")

if __name__ == "__main__":
    main()