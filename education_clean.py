"""
Cleans raw education listing data by dropping incomplete rows, cleaning uni name formatting and categorizing degree types.

Author: Jonah Richards
Date: 2025-03-02
"""

import pandas as pd
import re
import unicodedata


def remove_accents(text):
    """
    Replace accented letters with their unaccented versions.
    """
    try:
        text = unicodedata.normalize('NFD', text)
        text = text.encode('ascii', 'ignore').decode('utf-8')
        return str(text)
    except Exception as e:
        return text


def clean_university_name(name):
    if pd.isna(name):
        return name

    if not isinstance(name, str):
        name = str(name)

    name = remove_accents(name)

    abbreviations = {
        r'\bUniv\b': 'University',
        r'\bUniv\.\b': 'University',
        r'\bInst\b': 'Institute',
        r'\bInst\.\b': 'Institute',
        r'\bTech\b': 'Technology',
        r'\bTech\.\b': 'Technology',
        r'\bColl\b': 'College',
        r'\bColl\.\b': 'College',
        r'\bDept\b': 'Department',
        r'\bDept\.\b': 'Department',
        r'\bSch\b': 'School',
        r'\bSch\.\b': 'School',
        r'\bCtr\b': 'Center',
        r'\bCtr\.\b': 'Center',
        r'\bIntl\b': 'International',
        r'\bIntl\.\b': 'International',
        r'\bSci\b': 'Science',
        r'\bSci\.\b': 'Science',
        r'\bMgmt\b': 'Management',
        r'\bMgmt\.\b': 'Management'
    }
    for pattern, full in abbreviations.items():
        name = re.sub(pattern, full, name, flags=re.IGNORECASE)

    name = re.sub(r'\bthe\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[\'"]', '', name)
    name = re.sub(r'[-,\.]', ' ', name)
    name = re.sub(r'[\'"]', '', name)
    name = re.sub(r'[\(\)]', '', name)
    name = re.sub(r'[\\/]', '', name)
    name = " ".join(name.split())
    name = name.title()

    return name


def classify_role(role_title):
    role_clean = role_title.lower()
    role_clean = re.sub(r'[^\w\s]', '', role_clean)

    bachelor_keywords = [
        "bachelor", "bachelors", "undergrad", "bsc", "ba",
        "baccalaureate", "baccalaureat",
        "grado", "licenciatura", "licence",
        "laurea triennale", "bacharelado"
    ]

    masters_keywords = [
        "master", "masters", "msc", "ma", "mba",
        "máster", "masterado",
        "maestria", "maestría",
        "mastere", "mastère",
        "magister", "magistère",
        "laurea magistrale",
        "mestrado"
    ]

    phd_keywords = [
        "phd", "doctoral", "dphil", "doctorate",
        "doctor",
        "doctorado",
        "doctorat", "docteur",
        "doktor",
        "promotion",
        "dottorato",
        "doutorado"
    ]

    for keyword in phd_keywords:
        if keyword in role_clean:
            return "phd"
    for keyword in masters_keywords:
        if keyword in role_clean:
            return "masters"
    for keyword in bachelor_keywords:
        if keyword in role_clean:
            return "bachelors"

    return None


def main(input_dir="raw_csvs", output_csv="education_data_cleaned.csv"):
    import os
    import pandas as pd

    csv_files = [os.path.join(input_dir, file) for file in os.listdir(input_dir) if file.endswith(".csv")]
    print(f"Found {len(csv_files)} CSV file(s) in directory '{input_dir}'.")

    required_columns = {
        "id": "education.source.source-orcid.path",
        "name": "education.source.source-name",
        "university": "education.organization.name",
        "role_title": "education.role-title",
        "start_year": "education.start-date.year",
        "end_year": "education.end-date.year",
        "city": "education.organization.address.city",
        "region": "education.organization.address.region",
        "country": "education.organization.address.country"
    }

    processed_files = 0
    skipped_files = 0
    processed_dfs = []

    total_input_rows = 0
    total_missing_drops = 0
    total_classification_drops = 0

    for csv_file in csv_files:
        print(f"\nProcessing file: {csv_file}")
        try:
            df = pd.read_csv(csv_file, low_memory=False)
        except Exception as e:
            print(f"Error reading file '{csv_file}': {e}. Skipping.")
            skipped_files += 1
            continue

        total_input_rows += df.shape[0]

        missing_cols = [col for col in required_columns.values() if col not in df.columns]
        if missing_cols:
            print(f"Skipping file '{csv_file}' due to missing required columns: {missing_cols}")
            skipped_files += 1
            continue

        df_extracted = df[list(required_columns.values())].rename(columns={
            required_columns["id"]: "id",
            required_columns["name"]: "name",
            required_columns["university"]: "university",
            required_columns["role_title"]: "role_title",
            required_columns["start_year"]: "start_year",
            required_columns["end_year"]: "end_year",
            required_columns["city"]: "city",
            required_columns["region"]: "region",
            required_columns["country"]: "country"
        })

        df_extracted["university"] = df_extracted["university"].apply(clean_university_name)

        before_drop = df_extracted.shape[0]
        df_extracted.dropna(subset=["id", "name", "university", "role_title",
                                    "start_year", "end_year", "city", "region", "country"], inplace=True)
        after_drop = df_extracted.shape[0]
        missing_drop = before_drop - after_drop
        total_missing_drops += missing_drop
        print(f"Dropped {missing_drop} row(s) due to missing required fields.")

        before_class_drop = df_extracted.shape[0]
        df_extracted["degree"] = df_extracted["role_title"].apply(classify_role)
        df_extracted.dropna(subset=["degree"], inplace=True)
        after_class_drop = df_extracted.shape[0]
        class_drop = before_class_drop - after_class_drop
        total_classification_drops += class_drop
        print(f"Dropped {class_drop} row(s) due to unclassifiable role titles.")

        processed_dfs.append(df_extracted[["id", "name", "university", "degree",
                                           "start_year", "end_year", "city", "region", "country"]])
        processed_files += 1
        print(f"Finished processing '{csv_file}'.")

    if processed_dfs:
        df_final = pd.concat(processed_dfs, ignore_index=True)
        df_final.to_csv(output_csv, index=False)
        print(f"\nFinal cleaned data saved to '{output_csv}'. Combined shape: {df_final.shape}")
    else:
        print("No valid CSV data was processed. Exiting without creating output CSV.")

    total_dropped_rows = total_missing_drops + total_classification_drops
    output_rows = df_final.shape[0] if processed_dfs else 0
    print("\nSummary:")
    print(f"  Total input rows: {total_input_rows}")
    print(
        f"  Total rows dropped: {total_dropped_rows} (Missing fields: {total_missing_drops}, Unclassifiable: {total_classification_drops})")
    print(f"  Total output rows: {output_rows}")
    print(f"  Files processed: {processed_files}")
    print(f"  Files skipped: {skipped_files}")


if __name__ == "__main__":
    data_root = "C:/Projects/572/data/"

    main(input_dir=data_root + "processed", output_csv=data_root + "education_data_cleaned.csv")
