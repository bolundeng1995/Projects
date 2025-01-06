import streamlit as st

def show():
    """Display the home page."""
    st.title("Welcome to the Streamlit App")
    st.markdown("""
    This is a basic Streamlit app with multiple pages.
    Use the sidebar to navigate between different sections.
    """)
    st.image("app/static/images/welcome.png", caption="Welcome Image", use_column_width=True)