import streamlit as st

def show():
    """Demonstrate Streamlit forms."""
    st.title("Streamlit Forms Example")

    with st.form("example_form"):
        st.write("Inside the form")
        name = st.text_input("Enter your name")
        age = st.slider("Select your age", min_value=0, max_value=100)
        submitted = st.form_submit_button("Submit")

        if submitted:
            st.write(f"Hello, {name}! You are {age} years old.")