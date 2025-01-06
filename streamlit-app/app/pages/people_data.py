import streamlit as st
import pandas as pd
from app.utils.data_loader import load_people_data


def show():
    """Display the people data page."""
    st.title("People Data")
    df = load_people_data()
    st.dataframe(df)

    # Download button
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(label="Download CSV", data=csv, file_name='people_data.csv', mime='text/csv')