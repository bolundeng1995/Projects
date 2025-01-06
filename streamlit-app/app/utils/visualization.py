import streamlit as st
import matplotlib.pyplot as plt

def plot_budget_chart(df):
    fig, ax = plt.subplots()
    df.plot(kind='bar', x='Department', y='Budget', ax=ax)
    st.pyplot(fig)