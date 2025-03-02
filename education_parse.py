import os
import glob
import xml.etree.ElementTree as ET
from collections import defaultdict
import concurrent.futures
import time
from tqdm import tqdm
import pandas as pd

def strip_namespace(tag):
    """Remove namespace from an XML tag."""
    if '}' in tag:
        return tag.split('}', 1)[1]
    return tag

def flatten_xml(element, parent_key=''):
    """
    Recursively flattens an XML element into a dictionary of key-value pairs.
    - Uses dot-notation for nested elements.
    - Includes attributes (prefixed with '@') and text (if available).
    """
    items = {}
    # Determine the new key based on the current element's tag (without namespace)
    tag = strip_namespace(element.tag)
    new_key = f"{parent_key}.{tag}" if parent_key else tag

    # Add attributes
    for attr, value in element.attrib.items():
        attr_key = f"{new_key}.@{attr}"
        items[attr_key] = value

    # Add element text if non-empty
    text = element.text.strip() if element.text else ""
    if text:
        items[new_key] = text

    # Recursively process child elements
    for child in element:
        items.update(flatten_xml(child, new_key))
    return items

def process_education_xml_file(file_path):
    """
    Process one education XML file: parse, flatten, and return its key-value dictionary.
    Returns None on error.
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        flattened = flatten_xml(root)
        return flattened
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None

def gather_education_xml_files(root_directory):
    """
    Walks the directory tree from the root directory.
    For every directory named "educations", gathers all XML file paths within it.
    """
    xml_files = []
    for dirpath, dirnames, filenames in os.walk(root_directory):
        if os.path.basename(dirpath) == "educations":
            for file in filenames:
                if file.lower().endswith('.xml'):
                    xml_files.append(os.path.join(dirpath, file))
    return xml_files

def main(root_directory, output_csv="education_data.csv", num_workers=8):
    print(f"Gathering XML files from root: {root_directory}")
    xml_files = gather_education_xml_files(root_directory)
    total_files = len(xml_files)
    print(f"Found {total_files} education XML file(s).")

    flattened_data = []  # List to hold each file's flattened dict

    start_time = time.time()
    # Use a ProcessPoolExecutor for parallel processing
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = executor.map(process_education_xml_file, xml_files)
        for i, result in enumerate(tqdm(futures, total=total_files, desc="Processing XML files"), start=1):
            if result is not None:
                flattened_data.append(result)
            if i % 1000 == 0:
                print(f"Debug: Processed {i} files so far.")
    end_time = time.time()
    print(f"Processed {total_files} XML files in {end_time - start_time:.2f} seconds.")

    # Combine the flattened dictionaries into a DataFrame.
    # Since not every XML file has the same keys, pandas will fill missing keys with NaN.
    df = pd.DataFrame(flattened_data)
    # Optionally, sort columns alphabetically
    # df = df.reindex(sorted(df.columns), axis=1)
    
    # Save DataFrame to CSV and print the DataFrame
    df.to_csv(output_csv, index=False)
    print(f"\nCombined education data saved to {output_csv}.")
    print("\nData preview:")
    print(df.head().to_string(index=False))

if __name__ == "__main__":
    # Replace this with your root directory that contains subdirectories with id directories.
    root_dir = "C:/Temp/ORCID_2019_activites_5"
    
    # Run the main processing function.
    main(root_directory=root_dir, output_csv="education_data_raw.csv", num_workers=16)
