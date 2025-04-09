import duckdb
import pandas as pd

def check_metrics_db():
    # Connect to the metrics database
    conn = duckdb.connect('metrics.duckdb')

    # Get all tables in the database
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").df()
    print("\nTables in metrics.duckdb:")
    print(tables['name'].tolist())

    # For each table, show its structure and contents
    for table in tables['name']:
        print(f"\nStructure of table '{table}':")
        columns = conn.execute(f"PRAGMA table_info({table})").df()
        print(columns[['name', 'type']])

        print(f"\nContents of table '{table}':")
        data = conn.execute(f"SELECT * FROM {table}").df()
        print(data)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    check_metrics_db()
