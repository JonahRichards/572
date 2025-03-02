import pandas as pd
from tqdm import tqdm

def temporal_check(source_end, dest_start):
    """
    Check if the source's end year is less than or equal to the destination's start year.
    Converts the values to integers; if conversion fails, returns False.
    """
    try:
        return int(source_end) <= int(dest_start)
    except (ValueError, TypeError):
        return False

def build_links(input_csv="education_data_cleaned.csv", output_csv="education_links.csv"):
    """
    Reads the cleaned education data CSV, groups records by id, and for each id determines
    the university transitions based on degree progression. Valid transitions are:
      - bachelor's -> master's
      - master's -> phd
      - bachelor's -> phd (if no master's exists)
    Additionally, a temporal sanity check is performed: the source's end year must be
    less than or equal to the destination's start year.
    If any id has duplicate degree levels, that id is ignored.
    The resulting directed edges (with full source and destination fields, and a single name field)
    are saved to a CSV.
    """
    print(f"Reading cleaned data from {input_csv}...")
    df = pd.read_csv(input_csv)
    print(f"Initial data shape: {df.shape}")
    
    # Expected columns in the cleaned CSV:
    # "id", "name", "university", "degree", "start_year", "end_year", "city", "region", "country"
    required_columns = ["id", "name", "university", "degree", "start_year", "end_year", "city", "region", "country"]
    for col in required_columns:
        if col not in df.columns:
            print(f"Error: Required column '{col}' not found in the CSV.")
            return

    # Group the data by id
    grouped = df.groupby("id")
    total_ids = len(grouped)
    edges = []
    ignored_ids = 0
    
    print("Processing each id for degree transitions...")
    # Iterate through each group with a progress bar
    for id_val, group in tqdm(grouped, total=total_ids, desc="Processing IDs"):
        # If there are duplicate degree levels (i.e. count != number of unique 'degree'), ignore this id.
        if len(group) != group["degree"].nunique():
            ignored_ids += 1
            continue
        
        # Create a dictionary mapping degree to the corresponding record.
        # We assume each group has one record per degree.
        degree_to_record = {row["degree"]: row for idx, row in group.iterrows()}
        
        # Helper function to add an edge if the temporal check passes.
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
        
        # Build valid transitions.
        # 1. Bachelor's -> Master's
        if "bachelors" in degree_to_record and "masters" in degree_to_record:
            add_edge("bachelors", "masters")
        # 2. Master's -> PhD
        if "masters" in degree_to_record and "phd" in degree_to_record:
            add_edge("masters", "phd")
        # 3. Bachelor's -> PhD (if no master's exists)
        if "bachelors" in degree_to_record and "phd" in degree_to_record and "masters" not in degree_to_record:
            add_edge("bachelors", "phd")
    
    print(f"Processed {total_ids} ids; ignored {ignored_ids} ids due to duplicate/ambiguous degree records.")
    print(f"Total edges generated: {len(edges)}")
    
    # Create a DataFrame from the edges and save to CSV.
    edges_df = pd.DataFrame(edges)
    edges_df.to_csv(output_csv, index=False)
    print(f"Directed links saved to {output_csv}.")

if __name__ == "__main__":
    build_links()
