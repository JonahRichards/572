"""
Builds a gephi compatible network compatible graph from the generated links (amalgamating duplicated links into weighted links)
Also adds lat lng data sourced from https://www.kaggle.com/datasets/juanmah/world-cities

Author: Jonah Richards
Date: 2025-04-08
"""
import pandas as pd
import networkx as nx
from collections import Counter
import os


def get_mode_city(city_list):
    if not city_list:
        return None
    counter = Counter(city_list)
    return counter.most_common(1)[0][0]


def load_world_cities(worldcities_csv):
    try:
        world_df = pd.read_csv(worldcities_csv)
    except Exception as e:
        print(f"Error reading world cities file '{worldcities_csv}': {e}")
        return {}

    world_df['city_normalized'] = world_df['city_ascii'].str.strip().str.lower()
    world_df_unique = world_df.drop_duplicates(subset=['city_normalized'])
    city_to_coords = world_df_unique.set_index('city_normalized')[['lat', 'lng']].to_dict(orient='index')
    return {city: (float(info['lat']), float(info['lng'])) for city, info in city_to_coords.items()}


def build_graph(links_csv, worldcities_csv, output_gexf):
    print(f"Loading links from '{links_csv}'...")
    try:
        links_df = pd.read_csv(links_csv, low_memory=False)
    except Exception as e:
        print(f"Error reading links CSV: {e}")
        return

    print(f"Loaded {links_df.shape[0]} rows from links CSV.")

    required_columns = ["source_university", "source_city", "destination_university", "destination_city"]
    for col in required_columns:
        if col not in links_df.columns:
            print(f"Error: Required column '{col}' not found in links CSV.")
            return

    uni_to_cities = {}
    for idx, row in links_df.iterrows():
        src_uni = row["source_university"]
        src_city = row["source_city"]
        if pd.notnull(src_uni) and pd.notnull(src_city):
            uni_to_cities.setdefault(src_uni, []).append(src_city.strip())
        dst_uni = row["destination_university"]
        dst_city = row["destination_city"]
        if pd.notnull(dst_uni) and pd.notnull(dst_city):
            uni_to_cities.setdefault(dst_uni, []).append(dst_city.strip())

    print(f"Identified {len(uni_to_cities)} unique universities from links.")

    uni_to_mode_city = {uni: get_mode_city(cities) for uni, cities in uni_to_cities.items()}

    city_coords = load_world_cities(worldcities_csv)
    if not city_coords:
        print("No world city coordinates loaded. Exiting.")
        return

    nodes = {}
    for uni, city in uni_to_mode_city.items():
        norm_city = city.strip().lower() if city else ""
        if norm_city in city_coords:
            nodes[uni] = city_coords[norm_city]
        else:
            print(f"Excluding university '{uni}' because city '{city}' not found in world cities.")

    print(f"{len(nodes)} universities will be included as nodes (after filtering by valid city coordinates).")

    G = nx.DiGraph()
    for uni, (lat, lng) in nodes.items():
        G.add_node(uni, lat=lat, long=lng)

    for idx, row in links_df.iterrows():
        src_uni = row["source_university"]
        dst_uni = row["destination_university"]
        if src_uni in nodes and dst_uni in nodes:
            if G.has_edge(src_uni, dst_uni):
                G[src_uni][dst_uni]['weight'] += 1
            else:
                G.add_edge(src_uni, dst_uni, weight=1)

    print(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")

    try:
        nx.write_gexf(G, output_gexf)
        print(f"Graph saved to '{output_gexf}'.")
    except Exception as e:
        print(f"Error saving graph to '{output_gexf}': {e}")

    print("\nGraph Statistics:")
    print(f" - Nodes: {G.number_of_nodes()}")
    print(f" - Edges: {G.number_of_edges()}")
    if G.number_of_nodes() > 0:
        degrees = [d for n, d in G.degree()]
        print(f" - Average degree: {sum(degrees) / len(degrees):.2f}")


def main():
    data_root = "C:/Projects/572/data/"
    links_csv = os.path.join(data_root, "education_data_links.csv")
    worldcities_csv = os.path.join(data_root, "worldcities.csv")
    output_gexf = os.path.join(data_root, "education_network.gexf")

    build_graph(links_csv, worldcities_csv, output_gexf)


if __name__ == "__main__":
    main()
