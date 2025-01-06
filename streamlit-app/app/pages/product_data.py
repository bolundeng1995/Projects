import streamlit as st
from app.utils.data_loader import load_product_data


def show():
    """Display the product data page."""
    st.title("Product Data")
    df = load_product_data()
    st.dataframe(df)

    # Visualization
    st.bar_chart(df.set_index("Product")["Price"])