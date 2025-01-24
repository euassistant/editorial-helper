import re
import os
import pandas as pd
from datetime import datetime


def get_local_data():
    folder_path = '../Dealt With/'
    pattern = r'([A-Za-z]+)-D-(\d{2}-\d{5})(R\d+)\s\((.*?)\)\s(\d{4})-(\d{2})-(\d{2})\.pdf'    
    files = os.listdir(folder_path)
    file_data = []
    existing_df = pd.read_csv('raw_data.csv')
    existing_ms_numbers = existing_df['MS Number'].tolist()  # Extract existing MS Numbers to avoid duplicates
    # Loop through the files and check for new rows
    for file_name in files:
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):  # Check if it's a file, not a directory
            match = re.search(pattern, file_name)
            if match:
                journal = match.group(1)  # EUONCO
                ms_number = f"{journal}-D-{match.group(2)}{match.group(3)}"  # EUONCO-D-24-00622R2
                version = match.group(3)  # R2
                editor = match.group(4)   # Assel
                year = int(match.group(5))     # 2024
                file_info = {
                    'Name': file_name,
                    'MS Number': ms_number,
                    'Version': version,
                    'Year': year,
                    'Editor': editor,
                    'Journal': journal
                }
                if ms_number not in existing_ms_numbers:
                    file_data.append(file_info)
    # If there is any new data, create a DataFrame and append it to the CSV
    if file_data:
        new_df = pd.DataFrame(file_data)
        new_df['Date Invited'] = None
        new_df['Date Completed'] = None
        new_df = new_df.sort_values(by='Year', ascending=False)
        combined_df = pd.concat([new_df, existing_df], ignore_index=True)
        # Write the combined DataFrame back to the CSV (overwrite entire file)
        combined_df.to_csv('reviewer_metrics.csv', index=False)

        # Append the new rows to the CSV, ensuring columns are consistent
        #new_df.to_csv('merged_2025-01-22.csv', mode='a', header=not os.path.exists('merged_2025-01-22.csv'), index=False)
        print(f"Appended {len(new_df)} new rows")
    else:
        print("No new rows to append.")

def main():
    get_local_data()

main()
