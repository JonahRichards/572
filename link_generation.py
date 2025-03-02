import pandas as pd
from tqdm import tqdm

def build_links(input_csv="education_data_cleaned.csv", output_csv="education_links.csv"):
    """
    Reads the cleaned education data CSV, groups records by id, and for each id determines
    the university transitions based on degree progression. Valid transitions are:
      - bachelor's -> master's
      - master's -> phd
      - bachelor's -> phd (if no master's exists)
    If any id has duplicate degree levels, that id is ignored.
    The resulting directed edges (source to destination university) are saved to a CSV.
    """
    print(f"Reading cleaned data from {input_csv}...")
    df = pd.read_csv(input_csv)
    print(f"Initial data shape: {df.shape}")
    
    # Group the data by id
    grouped = df.groupby("id")
    total_ids = len(grouped)
    edges = []
    ignored_ids = 0
    
    print("Processing each id for degree transitions...")
    # Iterate through each group using a progress bar
    for id_val, group in tqdm(grouped, total=total_ids, desc="Processing IDs"):
        # If there are duplicate degree levels (i.e. number of rows != number of unique degrees), ignore this id.
        if len(group) != group["degree"].nunique():
            ignored_ids += 1
            continue
        
        # Create a mapping from degree to university for this id.
        degree_to_uni = group.set_index("degree")["university"].to_dict()
        
        # Build valid transitions.
        # 1. Bachelor's -> Master's
        if "bachelors" in degree_to_uni and "masters" in degree_to_uni:
            edges.append({
                "source": degree_to_uni["bachelors"],
                "destination": degree_to_uni["masters"]
            })
        # 2. Master's -> PhD
        if "masters" in degree_to_uni and "phd" in degree_to_uni:
            edges.append({
                "source": degree_to_uni["masters"],
                "destination": degree_to_uni["phd"]
            })
        # 3. Bachelor's -> PhD (direct, if no master's exists)
        if "bachelors" in degree_to_uni and "phd" in degree_to_uni and "masters" not in degree_to_uni:
            edges.append({
                "source": degree_to_uni["bachelors"],
                "destination": degree_to_uni["phd"]
            })
    
    print(f"Processed {total_ids} ids; ignored {ignored_ids} ids due to duplicate/ambiguous degree records.")
    print(f"Total edges generated: {len(edges)}")
    
    # Create a DataFrame from the edges and save to CSV.
    edges_df = pd.DataFrame(edges)
    edges_df.to_csv(output_csv, index=False)
    print(f"Directed edges saved to {output_csv}.")

if __name__ == "__main__":
    build_links()
