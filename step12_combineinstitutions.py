import pandas as pd
from pathlib import Path

# List of companies whose enriched institution data should be combined
companies = ["DeepSeek"]

# Output path for the combined mega dataset
combined_output_path = Path("combined") / "mega_institutions.csv"
combined_output_path.parent.mkdir(parents=True, exist_ok=True)

def load_and_tag_dataset(company: str) -> pd.DataFrame:
    """Load a company's enriched institution dataset and tag author source."""
    file_path = Path(company) / "CSVs" / "enriched_institutions.csv"

    if not file_path.exists():
        print(f"⚠️ File not found for {company}: {file_path}")
        return pd.DataFrame()

    df = pd.read_csv(file_path)

    # Initialize all source-specific author columns
    df['openai_authors'] = 0
    df['anthropic_authors'] = 0
    df['google_authors'] = 0

    company_lower = company.lower()
    if company_lower == "openai":
        df['openai_authors'] = df['author_count']
    elif company_lower == "anthropic":
        df['anthropic_authors'] = df['author_count']
    elif company_lower == "google":
        df['google_authors'] = df['author_count']
    else:
        print(f"⚠️ Unrecognized company: {company}")

    return df

def combine_datasets(companies: list) -> pd.DataFrame:
    all_dfs = []

    for company in companies:
        print(f"🔹 Loading data for {company}")
        df = load_and_tag_dataset(company)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        print("❌ No data loaded. Exiting.")
        return pd.DataFrame()

    print("🔄 Combining all datasets...")
    combined_df = pd.concat(all_dfs, ignore_index=True)

    print("🧹 Deduplicating by 'id' and summing author columns...")
    combined_df = (
        combined_df.groupby("id", as_index=False)
        .agg(lambda x: x.iloc[0] if x.name not in ['openai_authors', 'anthropic_authors', 'google_authors'] else x.sum())
    )

    print("➕ Updating 'author_count' to reflect total authors from all sources")
    combined_df['author_count'] = (
        combined_df['openai_authors'] +
        combined_df['anthropic_authors'] +
        combined_df['google_authors']
    )

    print("🔽 Sorting institutions by total author count descending")
    combined_df = combined_df.sort_values(by='author_count', ascending=False).reset_index(drop=True)

    return combined_df


def main():
    print("🚀 Building combined mega institution dataset...\n")
    combined_df = combine_datasets(companies)

    if combined_df.empty:
        print("❌ No combined data to save.")
        return

    print(f"\n💾 Saving combined dataset to {combined_output_path}")
    combined_df.to_csv(combined_output_path, index=False)
    print("✅ Mega dataset saved successfully!")

    print("\n📊 Summary:")
    print(f"Total institutions: {len(combined_df)}")
    print("Author counts by source:")
    print(combined_df[['openai_authors', 'anthropic_authors', 'google_authors']].sum())

if __name__ == "__main__":
    main()
