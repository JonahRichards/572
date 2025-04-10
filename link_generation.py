"""
Build links between unis based on our transition heuristic.

Author: Jonah Richards
Date: 2025-03-02
"""

import pandas as pd
from tqdm import tqdm

def temporal_check(source_end, dest_start):
    try:
        return int(source_end) <= int(dest_start)
    except (ValueError, TypeError):
        return False

def build_links(input_csv="education_data_cleaned.csv", output_csv="education_links.csv"):
    print(f"Reading cleaned data from {input_csv}...")
    df = pd.read_csv(input_csv)
    print(f"Initial data shape: {df.shape}")

    required_columns = ["id", "name", "university", "degree", "start_year", "end_year", "city", "region", "country"]
    for col in required_columns:
        if col not in df.columns:
            print(f"Error: Required column '{col}' not found in the CSV.")
            return

    grouped = df.groupby("id")
    total_ids = len(grouped)
    edges = []
    ignored_ids = 0

    for id_val, group in tqdm(grouped, total=total_ids, desc="Processing IDs"):
        if len(group) != group["degree"].nunique():
            ignored_ids += 1
            continue

        degree_to_record = {row["degree"]: row for idx, row in group.iterrows()}

        def add_edge(source_deg, dest_deg):
            source = degree_to_record[source_deg]
            dest = degree_to_record[dest_deg]
            # Sanity check: source end_year should be <= destination start_year.
            if temporal_check(source["end_year"], dest["start_year"]):
                edges.append({
                    "name": source["name"],
                    "source_degree": source["degree"],
                    "source_university": source["university"],
                    "source_start_year": source["start_year"],
                    "source_end_year": source["end_year"],
                    "source_city": source["city"],
                    "source_region": source["region"],
                    "source_country": source["country"],
                    "destination_degree": dest["degree"],
                    "destination_university": dest["university"],
                    "destination_start_year": dest["start_year"],
                    "destination_end_year": dest["end_year"],
                    "destination_city": dest["city"],
                    "destination_region": dest["region"],
                    "destination_country": dest["country"]
                })

        if "bachelors" in degree_to_record and "masters" in degree_to_record:
            add_edge("bachelors", "masters")
        if "masters" in degree_to_record and "phd" in degree_to_record:
            add_edge("masters", "phd")
        if "bachelors" in degree_to_record and "phd" in degree_to_record and "masters" not in degree_to_record:
            add_edge("bachelors", "phd")

    print(f"Processed {total_ids} ids; ignored {ignored_ids} ids due to duplicate/ambiguous degree records.")
    print(f"Total edges generated: {len(edges)}")

    edges_df = pd.DataFrame(edges)
    edges_df.to_csv(output_csv, index=False)
    print(f"Directed links saved to {output_csv}.")

if __name__ == "__main__":
    data_root = "C:/Projects/572/data/"

    build_links(data_root + "education_data_matched.csv", output_csv=data_root + "education_links.csv")
