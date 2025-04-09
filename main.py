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

    # Query the data from Supabase
    result = supabase.table('reviewer_metrics').select(
        'Name',
        'MS_Number',
        'Version',
        'Year',
        'Editor',
        'Journal'
    ).execute()

    df = pd.DataFrame(result.data)

    # Read existing data from CSV
    existing_df = pd.read_csv('reviewer_metrics.csv')
    # Rename MS Number to MS_Number for consistency
    existing_df = existing_df.rename(columns={'MS Number': 'MS_Number'})
    existing_ms_numbers = existing_df['MS_Number'].tolist()

    # Filter for new rows
    new_rows = df[~df['MS_Number'].isin(existing_ms_numbers)]

    if not new_rows.empty:
        # Prepare new rows for CSV
        new_rows = new_rows.rename(columns={
            'MS_Number': 'MS Number',
            'Date_Invited': 'Date Invited',
            'Date_Completed': 'Date Completed'
        })
        new_rows = new_rows.sort_values(by='Year', ascending=False)

        # Combine with existing data
        combined_df = pd.concat([new_rows, existing_df], ignore_index=True)
        combined_df.to_csv('reviewer_metrics.csv', index=False)
        print(f"Appended {len(new_rows)} new rows")
    else:
        print("No new rows to append.")
        combined_df = existing_df

    # Ensure consistent column names before returning
    combined_df = combined_df.rename(columns={'MS Number': 'MS_Number'})

    # Print DataFrame structure for debugging
    print("\nDataFrame Structure:")
    print(f"Columns: {combined_df.columns.tolist()}")
    print(f"Shape: {combined_df.shape}")

    return combined_df

def save_to_db(df):
    try:
        # Initialize Supabase client
        supabase = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_KEY")
        )

        print("\nDataFrame Structure:")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Shape: {df.shape}")

        try:
            # Rename columns to match Supabase if needed
            column_mapping = {
                'Date Invited': 'Date_Invited',
                'Date Completed': 'Date_Completed'
            }
            records_df = df.copy()
            records_df = records_df.rename(columns=column_mapping)

            # Remove duplicates, keeping the last occurrence
            print(f"Checking for duplicates in MS_Number...")
            duplicates = records_df[records_df.duplicated('MS_Number', keep=False)]
            if not duplicates.empty:
                print(f"Found {len(duplicates)} duplicate entries")
                print("Sample of duplicates:")
                print(duplicates[['MS_Number', 'Name']].head())

            records_df = records_df.drop_duplicates('MS_Number', keep='last')
            print(f"Shape after removing duplicates: {records_df.shape}")

            # Replace NaN values with None
            records_df = records_df.replace({pd.NA: None})
            records_df = records_df.where(pd.notnull(records_df), None)

            # Convert to records and ensure no NaN values
            records = records_df.to_dict('records')

            # Use upsert instead of insert to handle duplicates
            response = supabase.table('reviewer_metrics_prod').upsert(records).execute()
            print(f"Successfully processed {len(records)} records in Supabase.")

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
