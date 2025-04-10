import streamlit as st
import pandas as pd
import duckdb
from dotenv import load_dotenv
from supabase import create_client
import os
import subprocess

# Load environment variables
load_dotenv()

# Page setup
st.set_page_config(page_title="Editorial Helper", page_icon="📝", layout="wide")
st.title("Editorial Helper")
tab1, tab2, tab3 = st.tabs(["Search", "Edit Data", "Analytics"])

# Use a text_input to get the keywords to filter the dataframe
with tab1:
    st.header("Search")

    # Add a refresh button
    if st.button('🔄 Refresh Data'):
        try:
            # Execute main.py
            result = subprocess.run(['python', 'main.py'], capture_output=True, text=True)
            if result.returncode == 0:
                st.success("Data refreshed successfully!")
            else:
                st.error(f"Error refreshing data: {result.stderr}")
        except Exception as e:
            st.error(f"Error executing main.py: {str(e)}")

        st.cache_data.clear()
        st.rerun()

    # Create a container with custom width for the search bar
    search_container = st.container()
    with search_container:
        col1, col2 = st.columns([1, 3])  # This creates two columns with the first being 1/4 of the width
        with col1:
            text_search = st.text_input("Search by Manuscript Name, MS Number, or Editor", value="")

    @st.cache_data(ttl=60)  # Cache for 60 seconds only
    def load_df():
        try:
            # Initialize Supabase client
            supabase = create_client(
                os.getenv("SUPABASE_URL"),
                os.getenv("SUPABASE_KEY")
            )

            # Query data from Supabase
            response = supabase.table('reviewer_metrics_prod').select("*").execute()

            # Convert to DataFrame
            df = pd.DataFrame(response.data)

            # Convert Year to integer
            df['Year'] = df['Year'].astype(int)

            # Convert date strings to datetime objects
            df['Date_Invited'] = pd.to_datetime(df['Date_Invited']).dt.date
            df['Date_Completed'] = pd.to_datetime(df['Date_Completed']).dt.date

            # Sort by Year descending
            df = df.sort_values('Year', ascending=False)

            return df

        except Exception as e:
            st.error(f"Error loading data from Supabase: {str(e)}")
            return pd.DataFrame()

    df = load_df()

    if text_search:
        m1 = df["MS_Number"].str.contains(text_search, case=False, na=False)
        m2 = df["Name"].str.contains(text_search, case=False, na=False)
        m3 = df["Editor"].str.contains(text_search, case=False, na=False)
        df_search = df[m1 | m2 | m3]
        st.text("Search Results")
        st.write(df_search)
    else:
        st.text("All Records")
        column_config = {
            "Year": st.column_config.NumberColumn(
                "Year",
                min_value=2000,
                max_value=2100,
                step=1,
                format="%d"
            ),
            "Date_Invited": st.column_config.DateColumn(
                "Date Invited",
                format="YYYY-MM-DD",
                step=1
            ),
            "Date_Completed": st.column_config.DateColumn(
                "Date Completed",
                format="YYYY-MM-DD",
                step=1
            )
        }
        st.dataframe(df, column_config=column_config)

with tab2:
    st.header("Edit Data")
    # Specify the column config to treat Year as integer and format dates
    column_config = {
        "Year": st.column_config.NumberColumn(
            "Year",
            min_value=2000,
            max_value=2100,
            step=1,
            format="%d"
        ),
        "Date_Invited": st.column_config.DateColumn(
            "Date Invited",
            format="YYYY-MM-DD",
            step=1
        ),
        "Date_Completed": st.column_config.DateColumn(
            "Date Completed",
            format="YYYY-MM-DD",
            step=1
        )
    }
    edf = st.data_editor(df, column_config=column_config)
    if st.button('Save'):
        try:
            # Initialize Supabase client
            supabase = create_client(
                os.getenv("SUPABASE_URL"),
                os.getenv("SUPABASE_KEY")
            )

            # Convert dates to string format for Supabase
            save_df = edf.copy()
            save_df['Date_Invited'] = save_df['Date_Invited'].astype(str)
            save_df['Date_Completed'] = save_df['Date_Completed'].astype(str)

            # Convert DataFrame to records
            records = save_df.to_dict('records')

            # Update Supabase using upsert
            response = supabase.table('reviewer_metrics_prod').upsert(records).execute()
            st.success(f"Successfully saved {len(records)} records to Supabase!")

            # Clear cache and refresh
            st.cache_data.clear()
            st.rerun()

        except Exception as e:
            st.error(f"Error saving to Supabase: {str(e)}")
            print(f"Full error: {str(e)}")  # For debugging

with tab3:
    st.header("Analytics Coming Soon!")
