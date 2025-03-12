import re
import os
import pandas as pd
import duckdb
from datetime import datetime


def initialize_metrics_db():
    # Load the CSV file and ensure date columns are parsed correctly
    df = pd.read_csv('merged_file.csv', parse_dates=['Date Invited', 'Date Completed'])

    # Debugging: Print the first few rows to verify the data
    print("CSV Data Sample:")
    print(df.head())

    # Connect to DuckDB
    con = duckdb.connect('metrics.duckdb')

    # Drop the existing table if it exists
    con.execute("DROP TABLE IF EXISTS reviewer_metrics")

    # Create the table with the correct schema
    con.execute("""
    CREATE TABLE reviewer_metrics (
        Name STRING,
        "MS Number" STRING,
        Version STRING,
        Year INTEGER,
        Editor STRING,
        Journal STRING,
        "Date Invited" DATE,
        "Date Completed" STRING
    )
    """)

    # Insert data into the table
    con.execute("INSERT INTO reviewer_metrics SELECT * FROM df")

    con.close()
    print("Data imported successfully into DuckDB.")

def fetch_data():
    # Connect to DuckDB and fetch data
    con = duckdb.connect('metrics.duckdb')
    df = con.execute("SELECT * FROM reviewer_metrics").fetchdf()
    con.close()

    # Debugging: Print the first few rows to verify the data
    print("Fetched Data Sample:")
    print(df.head())

    return df



def get_local_data():
    """Get data from reviews database"""
    try:
        with duckdb.connect('reviews.duckdb', read_only=True) as con:
            df = con.execute("SELECT * FROM reviews").fetchdf()
            print("Data fetched successfully from reviews database.")
            return df
    except Exception as e:
        print(f"Error fetching from reviews database: {e}")
        return pd.DataFrame()

def check_for_new_data():
    # Get both sets of data
    reviews_df = get_local_data()
    existing_df = fetch_data()
    
    # Rename the column from 'MS_Number' to 'MS Number' in reviews_df
    reviews_df = reviews_df.rename(columns={'MS_Number': 'MS Number'})
    
    # Initialize new_man as an empty DataFrame with the same columns as reviews_df
    new_man = pd.DataFrame(columns=reviews_df.columns)
    
    # Get list of existing MS Numbers
    existing_ms_numbers = existing_df['MS Number'].tolist() if not existing_df.empty else []
    
    # Check for new entries
    for index, row in reviews_df.iterrows():
        ms_number = row['MS Number']
        if ms_number not in existing_ms_numbers:
            new_man = pd.concat([new_man, pd.DataFrame([row])], ignore_index=True)
           # print(f"MS Number {ms_number} is new and added to new_man.")
    
    return new_man

def format_data():
    df = check_for_new_data()

    if not df.empty:
        new_df = pd.DataFrame(df)
        # Convert None to NULL for DuckDB
        new_df['Date Invited'] = pd.to_datetime(new_df['Date Invited'])
        new_df['Date Completed'] = pd.to_datetime(new_df['Date Completed'])
        new_df = new_df.sort_values(by='Year', ascending=False)
        
        try:
            # Save to metrics database with conflict handling
            with duckdb.connect('metrics.duckdb') as con:
                # Create temporary table for new data
                con.execute("CREATE TEMP TABLE IF NOT EXISTS temp_metrics AS SELECT * FROM new_df")
                
                # Insert data with conflict handling
                con.execute("""
                    INSERT INTO reviewer_metrics 
                    SELECT * FROM temp_metrics
                    WHERE "MS Number" NOT IN (SELECT "MS Number" FROM reviewer_metrics)
                """)
                
                # Clean up temporary table
                con.execute("DROP TABLE IF EXISTS temp_metrics")
            
            print(f"Appended {len(new_df)} new rows")
            return new_df
        except Exception as e:
            print(f"Error inserting data: {e}")
            return None
    else:
        print("No new rows to append.")
        return fetch_data()

def main():
    
    combined_df = format_data()
    
    if combined_df is not None:
        combined_df.to_csv('combined_df2.csv', index=False)
        print("Data successfully combined and saved.")
    else:
        print("No changes made to the data.")

if __name__ == "__main__":
    main()
