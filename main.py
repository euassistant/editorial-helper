import re
import os
import pandas as pd
import duckdb
from datetime import datetime

def get_local_data():
    con = duckdb.connect('reviews.duckdb')
    # Query the reviews table
    try:
        df = con.execute("SELECT * FROM reviews").fetchdf()
        print("Data fetched successfully from DuckDB.")
    except Exception as e:
        print(f"Error fetching data: {e}")
        df = pd.DataFrame()  # Return an empty DataFrame in case of error

    # Close the connection
    con.close()

    return df


def check_for_new_data():
    # Get the existing data
    df = get_local_data()
    
    # Initialize new_man as an empty DataFrame with the same columns as df
    new_man = pd.DataFrame(columns=df.columns)
    
    # Extract existing MS Numbers to avoid duplicates
    existing_df = pd.read_csv('reviewer_metrics.csv')

    existing_ms_numbers = existing_df['MS Number'].tolist()
    
    # Iterate over the DataFrame rows
    for index, row in df.iterrows():
        ms_number = row['MS_Number']
        
        # Check if the ms_number is not in the existing MS numbers
        if ms_number not in existing_ms_numbers:
            # Append the row to new_man DataFrame
            new_man = new_man.append(row, ignore_index=True)
            print(f"MS Number {ms_number} is new and added to new_man.")
    
    return new_man


def format_data():
    df = check_for_new_data()


    if not df.empty:
        new_df = pd.DataFrame(df)
        new_df['Date Invited'] = None
        new_df['Date Completed'] = None
        new_df = new_df.sort_values(by='Year', ascending=False)
        existing_df = pd.read_csv('reviewer_metrics.csv')
        combined_df = pd.concat([new_df, existing_df], ignore_index=True)
        # Write the combined DataFrame back to the CSV (overwrite entire file)
        combined_df.to_csv('reviewer_metrics.csv', index=False)

     # Append the new rows to the CSV, ensuring columns are consistent
        print(f"Appended {len(new_df)} new rows")
        return combined_df
    else:
        print("No new rows to append.")
        return None
    

def main():
    combined_df = format_data()
    if combined_df is not None:
        print("Data successfully combined and saved.")
    else:
        print("No changes made to the data.")

main()
