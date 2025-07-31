import pandas as pd
import networkx as nx
from pathlib import Path
from typing import Dict, List, Any

# 🔹 Change this one line to set the base folder
base_folder = "Anthropic"

# Define file paths
input_file = Path(base_folder) / "CSVs" / "enriched_institutions.csv"
output_file = Path(base_folder) / "CSVs" / "institutions_lineage.csv"

class InstitutionLineageAnalyzer:
    def __init__(self, input_path: Path):
        """Initialize the analyzer with input file"""
        self.df = pd.read_csv(input_path)
        self.graph = nx.DiGraph()
        self._build_graph()

    def _build_graph(self):
        """Build a directed graph of institution relationships"""
        print("Building institution relationship graph...")
        
        for _, row in self.df.iterrows():
            self.graph.add_node(row['id'],
                                display_name=row['display_name'],
                                author_count=row['author_count'],
                                works_count=row['works_count'],
                                cited_by_count=row['cited_by_count'])

        for _, row in self.df.iterrows():
            if pd.notna(row['children_ids']) and row['children_ids']:
                for child in row['children_ids'].split('; '):
                    self.graph.add_edge(row['id'], child)

            if pd.notna(row['parent_ids']) and row['parent_ids']:
                for parent in row['parent_ids'].split('; '):
                    self.graph.add_edge(parent, row['id'])

    def get_full_lineage(self, institution_id: str) -> Dict[str, List[str]]:
        """Get all ancestors and descendants of an institution"""
        try:
            return {
                'ancestors': list(nx.ancestors(self.graph, institution_id)),
                'descendants': list(nx.descendants(self.graph, institution_id))
            }
        except nx.NetworkXError:
            return {'ancestors': [], 'descendants': []}

    def get_hierarchy_metrics(self, institution_id: str) -> Dict[str, Any]:
        """Calculate metrics for an institution and its hierarchy"""
        lineage = self.get_full_lineage(institution_id)
        inst_data = self.df[self.df['id'] == institution_id].iloc[0]

        children_ids = lineage['descendants']
        children_data = self.df[self.df['id'].isin(children_ids)]

        return {
            'institution_name': inst_data['display_name'],
            'direct_author_count': inst_data['author_count'],
            'total_author_count': inst_data['author_count'] + children_data['author_count'].sum(),
            'direct_works_count': inst_data['works_count'],
            'total_works_count': inst_data['works_count'] + children_data['works_count'].sum(),
            'direct_citations': inst_data['cited_by_count'],
            'total_citations': inst_data['cited_by_count'] + children_data['cited_by_count'].sum(),
            'num_children': len(children_ids),
            'num_ancestors': len(lineage['ancestors'])
        }

    def create_lineage_summary(self, output_path: Path):
        """Create a summary of all institutions and their lineage information"""
        print("\nAnalyzing institutional lineages...")
        lineage_data = []

        for idx, row in self.df.iterrows():
            print(f"Processing institution {idx + 1}/{len(self.df)}: {row['display_name']}")
            metrics = self.get_hierarchy_metrics(row['id'])
            lineage_data.append({
                'id': row['id'],
                'institution_name': metrics['institution_name'],
                'direct_author_count': metrics['direct_author_count'],
                'total_author_count': metrics['total_author_count'],
                'direct_works_count': metrics['direct_works_count'],
                'total_works_count': metrics['total_works_count'],
                'direct_citations': metrics['direct_citations'],
                'total_citations': metrics['total_citations'],
                'num_children': metrics['num_children'],
                'num_ancestors': metrics['num_ancestors'],
                'has_parent': metrics['num_ancestors'] > 0,
                'is_parent': metrics['num_children'] > 0
            })

        lineage_df = pd.DataFrame(lineage_data).sort_values('total_author_count', ascending=False)

        print(f"\nSaving lineage analysis to {output_path}...")
        lineage_df.to_csv(output_path, index=False)

        print("\nLineage Analysis Summary:")
        print(f"Total institutions analyzed: {len(lineage_df)}")
        print(f"Institutions with children: {lineage_df['is_parent'].sum()}")
        print(f"Institutions with parents: {lineage_df['has_parent'].sum()}")
        print(f"Largest total author count: {lineage_df['total_author_count'].max():,}")
        print(f"Average children per institution: {lineage_df['num_children'].mean():.2f}")

        return lineage_df

def main():
    print("Starting OpenAlex Institutions Lineage Analysis")
    print("=" * 50)

    analyzer = InstitutionLineageAnalyzer(input_file)
    analyzer.create_lineage_summary(output_file)

    print("\nDone!")

if __name__ == "__main__":
    main()
