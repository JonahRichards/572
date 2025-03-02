import pandas as pd

# Load the final links CSV
df = pd.read_csv("education_links.csv")
print("Original data shape:", df.shape)

# Filter rows where both source and destination countries are 'Canada'
# Here we convert the country values to lowercase for a case-insensitive match.
df_filtered = df[
    (df["source_country"] == "CA") &
    (df["destination_country"] == "CA")
]

print("Filtered data shape (only Canadian universities):", df_filtered.shape)

# Save the filtered data to a new CSV file
df_filtered.to_csv("education_links_canada.csv", index=False)
print("Filtered data saved to education_links_canada.csv")
