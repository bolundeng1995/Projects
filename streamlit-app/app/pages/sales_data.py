import streamlit as st
from streamlit_option_menu import option_menu
from app.pages import home, people_data, product_data, sales_data
from app.utils.load_css import load_css  # CSS loader function

# Load custom CSS
load_css()

# First Layer (Main Menu)
selected_main = option_menu(
    menu_title=None,
    options=["Main Menu", "Reports", "Settings"],
    icons=["house", "file-earmark-bar-graph", "gear"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
    styles={
        "container": {"padding": "0", "background-color": "#ffffff"},
        "nav-link": {"font-size": "16px", "text-align": "center", "margin": "0px"},
        "nav-link-selected": {"background-color": "#4CAF50", "color": "white"},
    },
)

# Second Layer (Submenus)
if selected_main == "Main Menu":
    selected_sub = option_menu(
        menu_title="Main Menu",
        options=["Home", "People Data", "Product Data", "Sales Data"],
        icons=["house", "people", "cart", "graph-up"],
        menu_icon="list",
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {"padding": "0", "background-color": "#f8f9fa"},
            "nav-link": {"font-size": "14px", "text-align": "center", "margin": "0px"},
            "nav-link-selected": {"background-color": "#4CAF50", "color": "white"},
        },
    )

    # Route to sub-pages
    if selected_sub == "Home":
        home.show()
    elif selected_sub == "People Data":
        people_data.show()
    elif selected_sub == "Product Data":
        product_data.show()
    elif selected_sub == "Sales Data":
        sales_data.show()

elif selected_main == "Reports":
    st.title("Reports Section")
elif selected_main == "Settings":
    st.title("Settings")