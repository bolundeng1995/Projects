import streamlit as st

def load_css():
    """
    Load and apply custom CSS from a static file.
    """
    css_file = "app/static/css/style.css"  # Path to your CSS file
    try:
        with open(css_file, "r") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.error(f"CSS file not found: {css_file}")