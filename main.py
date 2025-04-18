import re
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables
load_dotenv()

def get_local_data():
    # Initialize Supabase client
    supabase = create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY")
    )

    print("\n=== Starting Data Import Process ===")

    # Query the data from Supabase reviewer_metrics table
    print("\nQuerying reviewer_metrics table...")
    result = supabase.table('reviewer_metrics').select(
        'Name',
        'MS_Number',
        'Version',
        'Year',
        'Editor',
        'Journal'
    ).execute()

    print(f"Found {len(result.data)} records in reviewer_metrics")

    # Get existing data from reviewer_metrics_prod table
    print("\nQuerying reviewer_metrics_prod table...")
    existing_result = supabase.table('reviewer_metrics_prod').select(
        'MS_Number'
    ).execute()

    print(f"Found {len(existing_result.data)} records in reviewer_metrics_prod")

    # Convert to DataFrames
    df = pd.DataFrame(result.data)
    print(f"df columns: {df.columns.tolist()}")
    try:
        existing_df = pd.DataFrame(existing_result.data)
        print(f"existing_df columns: {existing_df.columns.tolist()}")
        if 'MS_Number' not in existing_df.columns:
            print("Error: 'MS_Number' column not found in existing_df. Available columns:", existing_df.columns.tolist())
            existing_ms_numbers = []
        else:
            existing_ms_numbers = existing_df['MS_Number'].values.tolist()
    except Exception as e:
        print(f"Error converting existing_result.data to DataFrame: {e}")
        existing_ms_numbers = []


    # Get list of existing MS_Numbers
    print(f"\nNumber of existing MS_Numbers: {len(existing_ms_numbers)}")

    # Filter for new rows
    new_rows = df[~df['MS_Number'].isin(existing_ms_numbers)]
    print(f"\nFound {len(new_rows)} new records to import")

    if not new_rows.empty:
        print("\nSample of new records:")
        print(new_rows[['MS_Number', 'Name', 'Year']].head())

        # Clean up the new rows
        new_rows = new_rows.copy()

        # Remove duplicate columns
        new_rows = new_rows.loc[:, ~new_rows.columns.duplicated()]

        # Remove rows with null MS_Number
        new_rows = new_rows.dropna(subset=['MS_Number'])
        print(f"After removing null MS_Number: {len(new_rows)} records")

        # Remove duplicates based on MS_Number
        new_rows = new_rows.drop_duplicates(subset=['MS_Number'], keep='last')
        print(f"After removing duplicates: {len(new_rows)} records")

        # Clean Year column
        def clean_year(year):
            try:
                if pd.isna(year):
                    return None
                if isinstance(year, str):
                    # Look for 4-digit year
                    import re
                    match = re.search(r'\d{4}', year)
                    if match:
                        return int(match.group(0))
                return int(year) if pd.notnull(year) else None
            except:
                return None

        new_rows['Year'] = new_rows['Year'].apply(clean_year)

        # Convert to records and ensure no NaN values
        new_rows = new_rows.replace({pd.NA: None})
        new_rows = new_rows.where(pd.notnull(new_rows), None)

        # Convert to records
        records = new_rows.to_dict('records')
        print(f"\nPrepared {len(records)} records for import")

        # Insert new records into reviewer_metrics_prod
        try:
            print("\nAttempting to import records...")
            response = supabase.table('reviewer_metrics_prod').upsert(records).execute()
            print(f"Successfully imported {len(records)} new records to reviewer_metrics_prod")

            # Verify the import
            verify_result = supabase.table('reviewer_metrics_prod').select("*").execute()
            print(f"\nVerification - Total records in reviewer_metrics_prod: {len(verify_result.data)}")

            # Check if our new records are present
            imported_ms_numbers = [record['MS_Number'] for record in records]
            verify_df = pd.DataFrame(verify_result.data)
            found_count = verify_df[verify_df['MS_Number'].isin(imported_ms_numbers)].shape[0]
            print(f"Found {found_count} of {len(imported_ms_numbers)} imported records in the database")

        except Exception as e:
            print(f"Error importing records: {str(e)}")
            print("Full error details:")
            import traceback
            print(traceback.format_exc())
    else:
        print("No new records to import")

    # Get the updated data from reviewer_metrics_prod
    updated_result = supabase.table('reviewer_metrics_prod').select("*").execute()
    combined_df = pd.DataFrame(updated_result.data)

    return combined_df

def save_to_db(df):
    try:
        # Initialize Supabase client
        supabase = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_KEY")
        )

      

        try:
            # Clean up the DataFrame before saving
            # Remove duplicate columns
            records_df = df.loc[:, ~df.columns.duplicated()]

            # Remove rows with null MS_Number
            records_df = records_df.dropna(subset=['MS_Number'])

            # Convert to records and ensure no NaN values
            records_df = records_df.replace({pd.NA: None})
            records_df = records_df.where(pd.notnull(records_df), None)

            # Get unique MS_Number values
            unique_ms_numbers = records_df['MS_Number'].unique()
            print(f"Found {len(unique_ms_numbers)} unique MS_Number values")

            # Process records one by one to avoid duplicate constraint errors
            total_processed = 0
            total_errors = 0

            for ms_number in unique_ms_numbers:
                # Get the most recent record for this MS_Number
                record = records_df[records_df['MS_Number'] == ms_number].iloc[-1].to_dict()

                try:
                    response = supabase.table('reviewer_metrics_prod').upsert([record]).execute()
                    total_processed += 1
                    if total_processed % 100 == 0:
                        print(f"Processed {total_processed} records")
                except Exception as e:
                    print(f"Error processing MS_Number {ms_number}: {str(e)}")
                    total_errors += 1

            # Verify the total number of records
            result = supabase.table('reviewer_metrics_prod').select("*").execute()
            print(f"Total records in Supabase: {len(result.data)}")

        except Exception as e:
            print(f"Error saving to Supabase: {str(e)}")
            print("Full error details:")
            import traceback
            print(traceback.format_exc())

    except Exception as e:
        print(f"Error connecting to Supabase: {str(e)}")
        print("Full error details:")
        import traceback
        print(traceback.format_exc())

def main():
    df = get_local_data()
    save_to_db(df)

if __name__ == "__main__":
    main()
