"""
Extracts data from education listing xml files. Batch processes a directory of ORCID databas archive files.

Author: Jonah Richards
Date: 2025-04-5
"""

import gc
import os
import glob
import tarfile
import gzip
from lxml import etree
import pandas as pd
import concurrent.futures
import time
from tqdm import tqdm
from functools import partial

data_root = "C:/Projects/572/data/"

def flatten_xml(element, parent_key=''):
    items = {}
    tag = element.tag.split('}', 1)[-1]
    new_key = f"{parent_key}.{tag}" if parent_key else tag
    for attr, value in element.attrib.items():
        items[f"{new_key}.@{attr}"] = value
    if element.text and element.text.strip():
        items[new_key] = element.text.strip()
    for child in element:
        items.update(flatten_xml(child, new_key))
    return items

def process_xml_stream(xml_stream):
    try:
        tree = etree.parse(xml_stream)
        root = tree.getroot()
        return flatten_xml(root)
    except Exception:
        return None

def process_archive(archive_path, batch_size=100000, output_prefix="education_raw"):
    batch = []
    file_index = 0
    processed_files = 0
    education_files = 0
    start_time = time.time()

    with gzip.open(archive_path, 'rb') as gz_stream:
        with tarfile.open(fileobj=gz_stream, mode='r|*') as tar:
            for member in tar:
                if member.isfile() and member.name.endswith('.xml'):
                    parent_dir = os.path.basename(os.path.dirname(member.name))
                    if parent_dir.lower() != "educations":
                        continue
                    education_files += 1
                    xml_file = tar.extractfile(member)
                    if xml_file is not None:
                        record = process_xml_stream(xml_file)
                        if record:
                            batch.append(record)
                    processed_files += 1
                    if processed_files % 1000 == 0:
                        print(f"{os.path.basename(archive_path)}: {processed_files} XML files processed, {education_files} education files", flush=True)
                    if len(batch) >= batch_size:
                        output_file = os.path.join(data_root, f"processed/{output_prefix}_{os.path.basename(archive_path)}_{file_index:03d}.csv")
                        pd.DataFrame(batch).to_csv(output_file, index=False)
                        print(f"Wrote {output_file} with {len(batch)} records", flush=True)
                        file_index += 1
                        batch = []
                        gc.collect()
    if batch:
        output_file = os.path.join(data_root, f"processed/{output_prefix}_{os.path.basename(archive_path)}_{file_index:03d}.csv")
        pd.DataFrame(batch).to_csv(output_file, index=False)
        print(f"Wrote final {output_file} with {len(batch)} records", flush=True)

    elapsed = time.time() - start_time
    print(f"Finished {os.path.basename(archive_path)}: {education_files} education files processed in {elapsed:.2f} seconds", flush=True)
    return archive_path

def process_all_archives(directory, num_workers=4, batch_size=100000, output_prefix="education_raw"):
    archive_files = glob.glob(os.path.join(directory, '*.tar.gz'))
    print(f"Found {len(archive_files)} archives in {directory}", flush=True)

    output_dir = os.path.join(data_root, "processed")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    process_archive_partial = partial(process_archive, batch_size=batch_size, output_prefix=output_prefix)
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        for _ in tqdm(executor.map(process_archive_partial, archive_files),
                      total=len(archive_files), desc="Archives processed"):
            pass

if __name__ == "__main__":
    archive_directory = os.path.join(data_root, "archives")
    process_all_archives(archive_directory, num_workers=2, batch_size=50000, output_prefix="education_raw")
