import streamlit as st
import pandas as pd

def show():
    """Demonstrate file upload functionality."""
    st.title("File Upload Example")

    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])
    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        st.write("Uploaded File Preview:")
        st.dataframe(df)

        # Additional analysis
        st.write("File Statistics:")
        st.write(df.describe())