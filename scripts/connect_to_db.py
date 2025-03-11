import re
import os
import duckdb
import pandas as pd
from datetime import datetime

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
                    'MS Number': f"{match.group(1)}-D-{match.group(2)}{match.group(3)}", 
                    'Version': match.group(3),
                    'Year': int(match.group(5)),
                    'Editor': match.group(4),
                    'Journal': match.group(1)
                })

    if file_data:
        df = pd.DataFrame(file_data)
        
        # Connect to DuckDB and insert data
        con = duckdb.connect(database='reviews.duckdb')
        con.execute("CREATE TABLE IF NOT EXISTS reviews AS SELECT * FROM df")
        
        # Insert new data into the table
        con.execute("INSERT INTO reviews SELECT * FROM df")
        
        # Close connection
        con.close()
        print("Data imported successfully into DuckDB.")
    else:
        print("No matching files found.")

def main():
    import_data()

if __name__ == "__main__":
    main()
