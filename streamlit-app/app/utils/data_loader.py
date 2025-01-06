import pandas as pd

def load_people_data():
    """Load sample people data."""
    data = {
        "Name": ["Alice", "Bob", "Charlie"],
        "Age": [25, 30, 35],
        "City": ["New York", "Los Angeles", "Chicago"]
    }
    return pd.DataFrame(data)

def load_product_data():
    """Load sample product data."""
    data = {
        "Product": ["Laptop", "Smartphone", "Tablet"],
        "Price": [1200, 800, 600],
        "Stock": [10, 25, 15]
    }
    return pd.DataFrame(data)

def load_sales_data():
    """Load sample sales data."""
    data = {
        "Date": pd.date_range(start="2023-01-01", periods=10),
        "Region": ["North", "South", "East", "West", "North", "South", "East", "West", "North", "South"],
        "Product": ["Laptop", "Smartphone", "Tablet", "Smartwatch", "Laptop", "Smartphone", "Tablet", "Smartwatch", "Laptop", "Smartphone"],
        "Sales": [1500, 2000, 1200, 800, 1800, 2200, 1400, 900, 2000, 2500],
    }
    return pd.DataFrame(data)