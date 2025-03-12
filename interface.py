import streamlit as st
import pandas as pd
from main import format_data


# Page setup

st.set_page_config(page_title="Editorial Helper", page_icon="📝", layout="wide")
st.title("Editorial Helper")
tab1, tab2, tab3 = st.tabs(["Search", "Edit Data", "Analytics"])


# Use a text_input to get the keywords to filter the dataframe
with tab1:
    st.header("Search")
    
    # Add a refresh button
    if st.button('🔄 Refresh Data'):
        st.cache_data.clear()
        st.rerun()
        
    text_search = st.text_input("Search by Manuscript Name, MS Number, or Editor", value="")

    @st.cache_data(ttl=60)  # Cache for 60 seconds only
    def load_df():
        try:
            # Read the CSV and convert Year column to integer
            df = format_data()
            df['Year'] = df['Year'].astype(int)  # Convert Year to integer
            return df
        except FileNotFoundError:
            st.error("reviewer_metrics.csv not found. Creating new data...")
            combined_df = format_data()
            if combined_df is not None:
                combined_df['Year'] = combined_df['Year'].astype(int)  # Convert Year to integer
                return combined_df
            return pd.DataFrame()  # Return empty DataFrame if no data

    df = load_df()
    
    if text_search:
        m1 = df["MS Number"].str.contains(text_search, case=False, na=False)
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
            format="%d"  # This ensures no decimal places
            )
        }
        st.dataframe(df, column_config=column_config)



with tab2:
    st.header("Edit Data")
    # Specify the column config to treat Year as integer
    column_config = {
        "Year": st.column_config.NumberColumn(
            "Year",
            min_value=2000,
            max_value=2100,
            step=1,
            format="%d"  # This ensures no decimal places
        )
    }
    edf = st.data_editor(df, column_config=column_config)
    if st.button('Save'):
        edf['Year'] = edf['Year'].astype(int)  # Ensure Year is integer before saving
        edf.to_csv('reviewer_metrics.csv', index=False)
        st.cache_data.clear()
        st.rerun()


with tab3:
    st.header("Analytics Coming Soon!")