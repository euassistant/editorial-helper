import csv
import pandas as pd

# Load the first CSV file
file1 = pd.read_csv('reviewer_metrics copy.csv')
file1 = file1.drop(columns=['Date Invited', 'Date Completed'])
# Load the second CSV file
file2 = pd.read_csv('cleaned_reviewer_metrics.csv')  # Replace with your second file's name


# Strip leading/trailing spaces from key columns
key_columns = ['MS Number', 'Version', 'Year', 'Editor', 'Journal']
for col in key_columns:
    file1[col] = file1[col].astype(str).str.strip()
    file2[col] = file2[col].astype(str).str.strip()

# Ensure the 'Version' and 'Year' columns are of the same type
file1['Version'] = file1['Version'].astype(str)
file2['Version'] = file2['Version'].astype(str)

file1['Year'] = file1['Year'].astype(str)
file2['Year'] = file2['Year'].astype(str)

# Print a few rows to debug
print("File1 sample:")
print(file1.head())
print("\nFile2 sample:")
print(file2.head())

# Merge the two files on the specified columns
merged_data = pd.merge(file1, file2, on=key_columns, how='left')

# Save the merged data to a new CSV file
merged_data.to_csv('merged_file.csv', index=False)