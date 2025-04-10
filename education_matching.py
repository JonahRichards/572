"""
Attemps to clean education data my matching common mispellings or qualified (e.g. Department of XXXX) uni names to more commonly occuring canonical names.

Author: Jonah Richards
Date: 2025-04-5
"""

import pandas as pd
from rapidfuzz import fuzz

FUZZY_THRESHOLD = 90
TOP_N = 500


def build_university_mapping(freq_dict, top_n=TOP_N, fuzzy_threshold=FUZZY_THRESHOLD):
    sorted_names = sorted(freq_dict.items(), key=lambda x: x[1], reverse=True)

    mapping = {name: name for name, count in sorted_names}
    total_substitutions = 0

    top_candidates = [name for name, count in sorted_names[:top_n]]

    for candidate in top_candidates:
        for name, count in sorted_names:
            if name == candidate:
                continue
            if mapping[name] == name:
                similarity = fuzz.token_sort_ratio(candidate, name)
                if similarity >= fuzzy_threshold:
                    mapping[name] = candidate
                    total_substitutions += 1
                    print(f"Fuzzy mapping: '{name}' -> '{candidate}' (similarity: {similarity})")
                elif candidate.lower() in name.lower():
                    mapping[name] = candidate
                    total_substitutions += 1
                    print(f"Substring mapping: '{name}' -> '{candidate}' (candidate is substring)")

    return mapping, total_substitutions


def main(input_csv, output_csv):
    print(f"Reading data from '{input_csv}'...")
    try:
        df = pd.read_csv(input_csv, low_memory=False)
    except Exception as e:
        print(f"Error reading file '{input_csv}': {e}")
        return

    print(f"Loaded {df.shape[0]} rows from '{input_csv}'.")

    if "university" not in df.columns:
        print("Error: 'university' column not found in the data.")
        return

    freq_series = df["university"].value_counts()
    freq_dict = freq_series.to_dict()
    unique_count = len(freq_dict)
    print(f"Found {unique_count} unique university names.")

    mapping, total_substitutions = build_university_mapping(freq_dict, top_n=TOP_N, fuzzy_threshold=FUZZY_THRESHOLD)
    print(f"Total substitutions made: {total_substitutions}")

    df["university"] = df["university"].apply(lambda x: mapping.get(x, x))

    df.to_csv(output_csv, index=False)
    print(f"Final matched data saved to '{output_csv}'. Total rows: {df.shape[0]}.")


if __name__ == "__main__":
    data_root = "C:/Projects/572/data/"
    input_csv = data_root + "education_data_cleaned.csv"
    output_csv = data_root + "education_data_matched.csv"
    main(input_csv, output_csv)
