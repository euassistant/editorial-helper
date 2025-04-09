import re
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables
load_dotenv()

def import_data():
    folder_path = '../Dealt With/'
    pattern = r'([A-Za-z]+)-D-(\d{2}-\d{5})(R\d+)\s\((.*?)\)\s(\d{4})-(\d{2})-(\d{2})\.pdf'
    files = os.listdir(folder_path)
    file_data = []

    for file_name in files:
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):  # Ensure it's a file, not a directory
            match = re.search(pattern, file_name)
            if match:
                file_data.append({
                    'Name': file_name,
                    'MS_Number': f"{match.group(1)}-D-{match.group(2)}{match.group(3)}",
                    'Version': match.group(3),
                    'Year': int(match.group(5)),
                    'Editor': match.group(4),
                    'Journal': match.group(1)
                })

    if file_data:
        df = pd.DataFrame(file_data)

        try:
            # Initialize Supabase client
            supabase = create_client(
                os.getenv("SUPABASE_URL"),
                os.getenv("SUPABASE_KEY")
            )

            # Get existing records to check for duplicates
            existing = supabase.table('reviewer_metrics').select('MS_Number').execute()
            existing_ms_numbers = [record['MS_Number'] for record in existing.data]

            # Filter out records that already exist
            new_records = df[~df['MS_Number'].isin(existing_ms_numbers)]

            if not new_records.empty:
                # Insert only new records
                response = supabase.table('reviewer_metrics').insert(new_records.to_dict('records')).execute()
                print(f"Successfully imported {len(new_records)} new records.")
            else:
                print("No new records to import.")

            # Verify total records in database
            result = supabase.table('reviewer_metrics').select("*").execute()
            print(f"Total records in database: {len(result.data)}")

        except Exception as e:
            print(f"Error connecting to Supabase: {str(e)}")
    else:
        print("No matching files found.")

def main():
    import_data()

if __name__ == "__main__":
    main()
