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