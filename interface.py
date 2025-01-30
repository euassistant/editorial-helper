import streamlit as st
import pandas as pd

# Page setup
st.set_page_config(page_title="Editorial Helper", page_icon="📝", layout="wide")
st.title("Editorial Helper")

# Use a text_input to get the keywords to filter the dataframe
text_search = st.text_input("Search by MS Number only", value="")
csvfn = 'reviewer_metrics.csv'

def update(edf):
    edf.to_csv(csvfn, index=False)
    load_df.clear()
    

@st.cache_data(ttl='1d')
def load_df():
    return pd.read_csv(csvfn)


df = load_df()
m1 = df["MS Number"].str.contains(text_search)
df_search = df[m1]
# Show the results, if you have a text_search
st.text("Search Results")
if text_search:
    st.write(df_search)

st.title("Data")
edf = st.data_editor(df)
st.button('Save', on_click=update, args=(edf, ))
