import os
from dotenv import load_dotenv
from supabase import create_client
import pandas as pd

# Load environment variables
load_dotenv()

def test_connection():
    try:
        # Initialize Supabase client
        supabase = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_KEY")
        )

        print("Successfully connected to Supabase!")

        # Test connection to reviewer_metrics_prod table
        try:
            # First try to read from the table
            print("\nTesting read access...")
            result = supabase.table('reviewer_metrics_prod').select('*').limit(1).execute()
            print("Read test successful!")
            if result.data:
                print(f"Found {len(result.data)} records")
                print("\nSample record:")
                print(result.data[0])
            else:
                print("Connected to table but no records found")

            # Then try to insert a test record
            print("\nTesting insert access...")
            test_record = {
                'Name': 'Test Name',
                'MS_Number': 'TEST-123',
                'Version': 'R1',
                'Year': 2024,
                'Editor': 'Test Editor',
                'Journal': 'Test Journal'
            }

            # First check if the record already exists
            existing = supabase.table('reviewer_metrics_prod').select('MS_Number').eq('MS_Number', 'TEST-123').execute()
            if existing.data:
                print("Test record already exists, trying to update instead...")
                insert_result = supabase.table('reviewer_metrics_prod').update(test_record).eq('MS_Number', 'TEST-123').execute()
            else:
                print("Test record doesn't exist, trying to insert...")
                insert_result = supabase.table('reviewer_metrics_prod').insert(test_record).execute()

            print("Insert/Update test successful!")

            # Verify the record was inserted/updated
            print("\nVerifying the record...")
            verify = supabase.table('reviewer_metrics_prod').select('*').eq('MS_Number', 'TEST-123').execute()
            if verify.data:
                print("Record verified successfully!")
                print(verify.data[0])
            else:
                print("Could not verify the record was inserted/updated")

        except Exception as e:
            print(f"\nError accessing table: {str(e)}")
            if "row-level security policy" in str(e):
                print("\nThis error indicates a Row Level Security (RLS) policy is preventing the operation.")
                print("Please check:")
                print("1. The policy was created correctly in Supabase")
                print("2. The policy is enabled (not disabled)")
                print("3. The policy applies to the correct role (service_role)")
                print("4. The policy allows the operation you're trying to perform")
            else:
                print("\nThis might be a different type of error. Please check:")
                print("1. The table structure matches the data you're trying to insert")
                print("2. All required fields are present")
                print("3. The data types match the column types in the table")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    test_connection()
