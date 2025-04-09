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
        "Date Completed" DATE
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
    #print("Fetched Data Sample:")
    #print(df.head())
    # Convert None to NULL for DuckDB
    #df['Date Invited'] = df['Date Invited'].dt.date
    #df['Date Completed'] = df['Date Completed'].dt.date

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

                # Fetch all data
                existing_df = con.execute("SELECT * FROM reviewer_metrics").fetchdf()

            # Combine new and existing data
            complete_df = pd.concat([new_df, existing_df], ignore_index=True)
            # Remove duplicates keeping the first occurrence (which will be from new_df)
            complete_df = complete_df.drop_duplicates(subset='MS Number', keep='first')

            # Sort the complete dataset by Year and Date Invited (most recent first)
            complete_df['Date Invited'] = pd.to_datetime(complete_df['Date Invited'])
            complete_df = complete_df.sort_values(by=['Year', 'Date Invited'],
                                                ascending=[False, False])

            print(f"Appended {len(new_df)} new rows")
            return complete_df  # Return the sorted dataset with new entries at the top
        except Exception as e:
            print(f"Error inserting data: {e}")
            return None
    else:
        print("No new rows to append.")
        df = fetch_data()
        # Sort existing data as well
        df['Date Invited'] = pd.to_datetime(df['Date Invited'])
        df = df.sort_values(by=['Year', 'Date Invited'], ascending=[False, False])
        return df

def main():

    combined_df = format_data()

    if combined_df is not None:
        combined_df.to_csv('combined_df2.csv', index=False)
        print("Data successfully combined and saved.")
    else:
        print("No changes made to the data.")

if __name__ == "__main__":
    main()
