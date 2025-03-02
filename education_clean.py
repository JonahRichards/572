import pandas as pd
import re

def classify_role(role_title):
    """
    Classifies a role title as 'bachelors', 'masters', or 'phd' by performing
    a keyword search on the lowercased, punctuation-stripped role_title.
    Returns the classification as a string, or None if no match is found.
    """
    # Normalize the text
    role_clean = role_title.lower()
    role_clean = re.sub(r'[^\w\s]', '', role_clean)
    
    # Exhaustive keyword lists for each degree category.
    # Bachelor's degree keywords (English and common foreign terms)
    bachelor_keywords = [
        "bachelor", "bachelors", "undergrad", "bsc", "ba",
        "baccalaureate", "baccalaureat",
        "grado", "licenciatura", "licence",
        "laurea triennale", "bacharelado"
    ]
    
    # Master's degree keywords (English and common foreign terms)
    masters_keywords = [
        "master", "masters", "msc", "ma", "mba",
        "máster", "masterado",
        "maestria", "maestría",
        "mastere", "mastère",
        "magister", "magistère",
        "laurea magistrale",
        "mestrado"
    ]
    
    # PhD/Doctorate keywords (English and common foreign terms)
    phd_keywords = [
        "phd", "doctoral", "dphil", "doctorate",
        "doctor",  # common shorthand; usually implies doctorate in context
        "doctorado",
        "doctorat", "docteur",
        "doktor",
        "promotion",  # German "Promotion" for a PhD
        "dottorato",
        "doutorado"
    ]
    
    # Check for PhD keywords first.
    for keyword in phd_keywords:
        if keyword in role_clean:
            return "phd"
    # Then check for Master's keywords.
    for keyword in masters_keywords:
        if keyword in role_clean:
            return "masters"
    # Then check for Bachelor's keywords.
    for keyword in bachelor_keywords:
        if keyword in role_clean:
            return "bachelors"
    
    # If no keyword matches, return None.
    return None

def main(input_csv="education_data_raw.csv", output_csv="education_data_cleaned.csv"):
    print(f"Reading raw data from {input_csv}...")
    df = pd.read_csv(input_csv)
    print(f"Initial data shape: {df.shape}")

    # Define the expected flattened column names from the previous extraction.
    # Adjust these keys if your flattening script produced different names.
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
    
    # Check for missing required columns in the raw CSV.
    missing_cols = [col for col in required_columns.values() if col not in df.columns]
    if missing_cols:
        print(f"Error: The following required columns are missing in the raw CSV: {missing_cols}")
        return
    
    # Extract and rename the columns.
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
    
    print("\nExtracted columns preview:")
    print(df_extracted.head().to_string(index=False))
    
    # Drop rows with any nulls in the required columns.
    before_drop = df_extracted.shape[0]
    df_extracted.dropna(subset=["id", "name", "university", "role_title", 
                                  "start_year", "end_year", "city", "region", "country"], inplace=True)
    after_drop = df_extracted.shape[0]
    print(f"\nDropped {before_drop - after_drop} rows due to missing required fields.")
    
    # Classify each role_title into a degree category.
    print("\nClassifying role titles into degree categories...")
    df_extracted["degree"] = df_extracted["role_title"].apply(classify_role)
    
    # Drop rows where classification failed.
    before_class_drop = df_extracted.shape[0]
    df_extracted.dropna(subset=["degree"], inplace=True)
    after_class_drop = df_extracted.shape[0]
    print(f"Dropped {before_class_drop - after_class_drop} rows due to unclassifiable role titles.")
    
    print("\nDegree classification distribution:")
    print(df_extracted["degree"].value_counts())
    
    # Create the final DataFrame with the desired columns.
    df_final = df_extracted[["id", "name", "university", "degree", 
                             "start_year", "end_year", "city", "region", "country"]]
    
    # Save the final cleaned data to CSV.
    df_final.to_csv(output_csv, index=False)
    print(f"\nFinal cleaned data saved to {output_csv}. Final shape: {df_final.shape}")

if __name__ == "__main__":
    main()
