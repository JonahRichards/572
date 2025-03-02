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

def extract_keys_from_xml(element, prefix=''):
    """
    Recursively extract tag names from an XML element using dot-notation.
    Strips namespaces for clarity.
    """
    keys = set()
    tag = strip_namespace(element.tag)
    full_key = f"{prefix}.{tag}" if prefix else tag
    keys.add(full_key)
    for child in element:
        keys |= extract_keys_from_xml(child, full_key)
    return keys

def process_xml_file(file_path):
    """
    Process a single XML file to extract its field keys.
    Returns a set of keys or None if an error occurs.
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        file_keys = extract_keys_from_xml(root)
        return file_keys
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def process_files(directory, num_workers=None, limit=None):
    """
    Processes XML files in the given directory using parallel processing.
    If 'limit' is provided, only processes that many files.
    Returns the total number of files processed and a dictionary mapping each key
    to its occurrence count (across files).
    """
    xml_files = glob.glob(os.path.join(directory, "*.xml"))
    if limit:
        xml_files = xml_files[:limit]
    total_files = len(xml_files)
    
    print(f"Found {total_files} XML files in {directory}. Starting processing...")
    start_time = time.time()
    
    field_counts = defaultdict(int)
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = executor.map(process_xml_file, xml_files)
        for i, file_keys in enumerate(tqdm(futures, total=total_files, desc="Processing XML files"), start=1):
            if file_keys is None:
                continue
            for key in file_keys:
                field_counts[key] += 1
            if i % 10000 == 0:
                print(f"Debug: Processed {i} files so far.")
                
    end_time = time.time()
    print(f"Processed {total_files} XML files in {end_time - start_time:.2f} seconds.")
    return total_files, field_counts

def save_field_counts(field_counts, output_path="field_occurrences.csv"):
    """
    Save the field occurrence counts to a CSV file using pandas, and also print them.
    """
    # Create a DataFrame from the dictionary and sort by count descending
    df = pd.DataFrame(list(field_counts.items()), columns=["Field", "Count"])
    df.sort_values(by="Count", ascending=False, inplace=True)
    df.to_csv(output_path, index=False)
    
    print(f"\nField occurrences across files (saved to {output_path}):")
    print(df.to_string(index=False))
    
if __name__ == "__main__":
    # Replace with your actual directory containing the XML files.
    directory = "C:/Temp/2013_public_profiles~/xml"
    
    # Process only the first 200 XML files as a test.
    total_files, field_counts = process_files(directory, num_workers=8, limit=None)
    
    # Save the field occurrences using pandas and also print the results.
    save_field_counts(field_counts)
    
    # Estimated Runtime Discussion:
    # Based on a preliminary test of one file (~0.005 sec per file sequentially), processing 300,000 files
    # would take approximately 300000 * 0.005 = 1500 seconds (or ~25 minutes) sequentially.
    # Using 8 parallel workers could reduce the time roughly to 1500 / 8 ≈ 188 seconds (around 3 minutes),
    # assuming ideal scaling and minimal I/O bottlenecks.
