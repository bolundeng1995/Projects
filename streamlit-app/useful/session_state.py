import streamlit as st

def show():
    """Demonstrate session state management."""
    st.title("Session State Example")

    if "counter" not in st.session_state:
        st.session_state.counter = 0

    st.write(f"Counter: {st.session_state.counter}")

    if st.button("Increment"):
        st.session_state.counter += 1
        st.write(f"Counter incremented to: {st.session_state.counter}")

    if st.button("Reset"):
        st.session_state.counter = 0
        st.write("Counter reset to 0.")