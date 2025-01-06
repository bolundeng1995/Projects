import streamlit as st
import time

def show():
    """Demonstrate progress bar and status updates."""
    st.title("Progress Bar Example")

    st.write("Starting a long computation...")

    # Initialize a progress bar
    progress = st.progress(0)

    for i in range(1, 101):
        time.sleep(0.05)  # Simulate computation
        progress.progress(i)

    st.success("Computation complete!")