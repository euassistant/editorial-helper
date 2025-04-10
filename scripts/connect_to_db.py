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

        # Remove duplicates by keeping the latest version for each MS_Number
        print(f"Found {len(df)} total records before deduplication")

        # Extract version number for sorting using raw string
        df['Version_Num'] = df['Version'].str.extract(r'R(\d+)').astype(int)

        # Sort by MS_Number and Version_Num in descending order
        df = df.sort_values(['MS_Number', 'Version_Num'], ascending=[True, False])

        # Keep only the first occurrence of each MS_Number (which will be the latest version)
        df = df.drop_duplicates(subset=['MS_Number'], keep='first')

        # Drop the temporary Version_Num column
        df = df.drop('Version_Num', axis=1)

        print(f"After deduplication: {len(df)} unique records")

        try:
            # Initialize Supabase client
            supabase = create_client(
                os.getenv("SUPABASE_URL"),
                os.getenv("SUPABASE_KEY")
            )

            # Get all existing MS_Numbers from the database
            print("Fetching existing MS_Numbers from database...")
            existing = supabase.table('reviewer_metrics').select('MS_Number').execute()
            existing_ms_numbers = [record['MS_Number'] for record in existing.data]
            print(f"Found {len(existing_ms_numbers)} existing MS_Numbers in database")

            # Find MS_Numbers that are missing from the database
            missing_records = df[~df['MS_Number'].isin(existing_ms_numbers)]
            print(f"Found {len(missing_records)} MS_Numbers that need to be added")

            # Insert only the missing records
            if not missing_records.empty:
                print("Adding missing records...")
                for _, record in missing_records.iterrows():
                    try:
                        # Double-check if the record exists before inserting
                        check = supabase.table('reviewer_metrics').select('MS_Number').eq('MS_Number', record['MS_Number']).execute()
                        if not check.data:
                            response = supabase.table('reviewer_metrics').insert(
                                record.to_dict()
                            ).execute()
                            print(f"Added: {record['MS_Number']}")
                    except Exception as e:
                        if "duplicate key" not in str(e):
                            print(f"Error adding record {record['MS_Number']}: {str(e)}")
            else:
                print("No missing records found - all MS_Numbers are already in the database")

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
