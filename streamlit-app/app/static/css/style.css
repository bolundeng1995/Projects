import streamlit as st
from app.pages import home, people_data, product_data

def load_css(css_file):
    """Load and apply a CSS file."""
    with open(css_file, "r") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Load custom CSS
load_css("app/static/css/style.css")

# Sidebar navigation
st.sidebar.title("Navigation")
tab = st.sidebar.radio("Go to:", ["Home", "People Data", "Product Data"])

# Page routing
if tab == "Home":
    home.show()
elif tab == "People Data":
    people_data.show()
elif tab == "Product Data":
    product_data.show()