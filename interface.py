import streamlit as st
import pandas as pd
from main import get_local_data


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

    csvfn = get_local_data()

    def update(edf):
        edf.to_csv(csvfn, index=False)
        st.cache_data.clear()  # Clear the cache when data is updated
        st.rerun()  # Rerun the app to show new data
    

    @st.cache_data(ttl=60)  # Cache for 60 seconds only
    def load_df():
        return pd.read_csv(csvfn)


    df = load_df()
    m1 = df["MS Number"].str.contains(text_search)
    m2 = df["Name"].str.contains(text_search)
    m3 = df["Editor"].str.contains(text_search)
    df_search = df[m1 | m2 | m3]
    # Show the results, if you have a text_search
    st.text("Search Results")
    if text_search:
        st.write(df_search)



with tab2:
    st.header("Edit Data")
    edf = st.data_editor(df)
    st.button('Save', on_click=update, args=(edf, ))


with tab3:
    st.header("Analytics Coming Soon!")