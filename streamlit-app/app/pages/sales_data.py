import streamlit as st
import pandas as pd
import plotly.express as px
from app.utils.data_loader import load_sales_data


def show():
    """Display the sales data page with an elegant blue-green color scheme and two graphs in a row."""
    st.title("Sales Data Visualization")

    # Load the sales data
    df = load_sales_data()

    # Display the dataset
    st.subheader("Complete Sales Data")
    st.dataframe(df)

    # Sidebar Filters
    st.sidebar.header("Filters")
    regions = st.sidebar.multiselect("Select Region(s):", options=df["Region"].unique(), default=df["Region"].unique())
    products = st.sidebar.multiselect("Select Product(s):", options=df["Product"].unique(),
                                      default=df["Product"].unique())
    date_range = st.sidebar.date_input(
        "Select Date Range:",
        value=(df["Date"].min(), df["Date"].max()),
        min_value=df["Date"].min(),
        max_value=df["Date"].max()
    )

    # Apply filters
    filtered_df = df[
        (df["Region"].isin(regions)) &
        (df["Product"].isin(products)) &
        (df["Date"] >= pd.to_datetime(date_range[0])) &
        (df["Date"] <= pd.to_datetime(date_range[1]))
        ]

    # Display filtered data
    st.subheader("Filtered Data")
    st.write(f"Showing {len(filtered_df)} rows")
    st.dataframe(filtered_df)

    # Summary Statistics
    st.subheader("Summary Statistics")
    st.write(f"**Total Sales:** ${filtered_df['Sales'].sum():,.2f}")
    st.write(f"**Average Sales per Transaction:** ${filtered_df['Sales'].mean():,.2f}")

    # Graphs with Blue-Green Color Scheme
    st.subheader("Visualizations with Blue-Green Theme")

    # Columns for Bar Chart and Line Chart
    col1, col2 = st.columns(2)

    # Bar Chart: Sales by Region
    with col1:
        st.markdown("### Sales by Region")
        region_sales = filtered_df.groupby("Region")["Sales"].sum().reset_index()
        fig = px.bar(
            region_sales,
            x="Region",
            y="Sales",
            text="Sales",
            color="Region",
            color_discrete_sequence=px.colors.qualitative.Set2,  # Blue-Green color palette
            title="Sales by Region"
        )
        fig.update_traces(texttemplate="%{text:.2s}", textposition="outside")
        fig.update_layout(
            plot_bgcolor="#F9F9F9",  # Light gray background for the graph
            paper_bgcolor="#FFFFFF",  # White outer background
            font_color="#333333",  # Dark gray text
            yaxis=dict(title="Sales ($)", showgrid=False, color="#333333"),
            xaxis=dict(color="#333333"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Line Chart: Sales Over Time
    with col2:
        st.markdown("### Sales Over Time")
        time_sales = filtered_df.groupby("Date")["Sales"].sum().reset_index()
        fig = px.line(
            time_sales,
            x="Date",
            y="Sales",
            markers=True,
            line_shape="spline",
            color_discrete_sequence=["#2E8B57"],  # Green line
            title="Sales Over Time"
        )
        fig.update_layout(
            plot_bgcolor="#F9F9F9",  # Light gray background for the graph
            paper_bgcolor="#FFFFFF",  # White outer background
            font_color="#333333",  # Dark gray text
            yaxis=dict(title="Sales ($)", showgrid=False, color="#333333"),
            xaxis=dict(color="#333333"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Pie Chart: Sales Distribution by Product
    st.markdown("### Sales Distribution by Product")
    product_sales = filtered_df.groupby("Product")["Sales"].sum().reset_index()
    fig = px.pie(
        product_sales,
        values="Sales",
        names="Product",
        title="Sales by Product",
        color_discrete_sequence=px.colors.sequential.Blues  # Blue gradient for the pie chart
    )
    fig.update_layout(
        plot_bgcolor="#F9F9F9",  # Light gray background for the graph
        paper_bgcolor="#FFFFFF",  # White outer background
        font_color="#333333",  # Dark gray text
    )
    st.plotly_chart(fig, use_container_width=True)