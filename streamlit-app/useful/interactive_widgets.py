import streamlit as st

def show():
    """Demonstrate interactive widgets."""
    st.title("Interactive Widgets Example")

    # Text input
    name = st.text_input("Enter your name:")
    if name:
        st.write(f"Hello, {name}!")

    # Slider
    age = st.slider("Select your age:", min_value=0, max_value=100, value=25)
    st.write(f"Your age is: {age}")

    # Checkbox
    if st.checkbox("Show a secret message"):
        st.write("🎉 Streamlit is amazing!")