
# Streamlit App Documentation

## Project Structure

```
streamlit_app/
├── app/
│   ├── __init__.py
│   ├── main.py               # Main Streamlit app
│   ├── pages/
│   │   ├── __init__.py
│   │   ├── home.py           # Home page
│   │   ├── people_data.py    # People data page
│   │   ├── product_data.py   # Product data page
│   │   └── sales_data.py     # Sales data visualization page
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── data_loader.py    # Functions to load data
│   │   └── load_css.py       # CSS loader function
│   └── static/
│       ├── css/
│       │   └── style.css     # CSS file
│       └── images/           # Image assets
├── requirements.txt          # Dependencies
├── README.md                 # Documentation
└── .streamlit/
    └── config.toml           # Streamlit configuration
```

---

## File Explanations

### **1. Main App Folder: `app/`**

#### a. **`main.py`**
- **Purpose**: The entry point of the Streamlit app.
- **Content**:
  - Implements the **navigation bar** (e.g., `Main Menu`, `Reports`, `Settings`).
  - Dynamically routes to pages like `Home`, `People Data`, `Product Data`, and `Sales Data` using `option_menu`.
  - Loads custom CSS via the `load_css` utility for styling.

#### b. **`__init__.py`**
- **Purpose**: Marks the `app/` folder as a Python package.
- **Content**: Can be empty or contain app-level initialization code (e.g., imports).

---

### **2. Pages Folder: `pages/`**

#### a. **`home.py`**
- **Purpose**: Displays the **Home** page.
- **Content**:
  - Greets the user and provides a general overview of the app.
  - May display a welcome message, a brief tutorial, or navigation tips.
  - Optionally includes a logo or an introductory image from the `static/images/` folder.

#### b. **`people_data.py`**
- **Purpose**: Displays data about people in a **table format**.
- **Content**:
  - Uses the `data_loader.load_people_data()` utility to fetch data.
  - Shows the data in a table using `st.dataframe`.
  - Allows downloading the data as a CSV.

#### c. **`product_data.py`**
- **Purpose**: Visualizes product-related data.
- **Content**:
  - Fetches data using `data_loader.load_product_data()`.
  - Displays the data in a **bar chart** (e.g., product prices or stock levels).

#### d. **`sales_data.py`**
- **Purpose**: Provides advanced **data visualization** for sales data.
- **Content**:
  - Fetches data using `data_loader.load_sales_data()`.
  - Displays:
    - Bar charts for sales by region.
    - Line charts for sales over time.
    - Pie charts for sales distribution by product.
    - Additional visualizations using Matplotlib or Plotly.

---

### **3. Utilities Folder: `utils/`**

#### a. **`data_loader.py`**
- **Purpose**: Provides reusable functions to load data for different pages.
- **Content**:
  - Example functions:
    - `load_people_data()`: Simulates loading data about people.
    - `load_product_data()`: Simulates loading product data.
    - `load_sales_data()`: Simulates loading sales data.

#### b. **`load_css.py`**
- **Purpose**: Loads and applies custom CSS to the app for consistent styling.
- **Content**:
  - A single function, `load_css()`, that:
    - Reads the `style.css` file in the `static/css/` folder.
    - Applies the CSS styles globally using `st.markdown` with `unsafe_allow_html=True`.

---

### **4. Static Folder: `static/`**

#### a. **`css/`**
- **Purpose**: Stores custom CSS files for styling the app.
- **Content**:
  - **`style.css`**:
    - Defines the app's global styles, such as:
      - Background colors.
      - Fonts.
      - Button styles.
      - Navbar styles.

#### b. **`images/`**
- **Purpose**: Stores static image assets for the app.
- **Content**:
  - Example:
    - A logo or banner displayed on the Home page.

---

### **5. Other Project Files**

#### a. **`requirements.txt`**
- **Purpose**: Lists all Python dependencies for the project.
- **Content**:
  - Example dependencies:
    ```txt
    streamlit
    pandas
    matplotlib
    plotly
    streamlit-option-menu
    ```

#### b. **`README.md`**
- **Purpose**: Provides documentation for the project.
- **Content**:
  - A brief overview of the app and its features.
  - Instructions for setup and running the app.
  - Example:
    ```markdown
    # Streamlit App
    A multi-page Streamlit app for data visualization.

    ## Setup
    1. Clone the repository.
    2. Install dependencies: `pip install -r requirements.txt`
    3. Run the app: `streamlit run app/main.py`

    ## Features
    - View people data, product data, and sales data with interactive visualizations.
    ```

#### c. **`.streamlit/config.toml`**
- **Purpose**: Configures Streamlit-specific settings, such as themes and server options.
- **Content**:
  - Example:
    ```toml
    [theme]
    primaryColor = "#4CAF50"
    backgroundColor = "#FFFFFF"
    secondaryBackgroundColor = "#F0F0F0"
    textColor = "#333333"

    [server]
    port = 8501
    enableCORS = false
    headless = true
    ```

---

## Why This Structure Works

1. **Modularity**:
   - Pages (`pages/`) and utilities (`utils/`) are independent and reusable.
   - Each page focuses on one aspect of the app.

2. **Scalability**:
   - Adding new pages or datasets is as simple as creating new modules.
   - Centralized utilities (`data_loader.py`) simplify data management.

3. **Maintainability**:
   - Separation of logic (data loading, visualization, styling) makes the code easier to read and debug.

4. **Reusability**:
   - Utility functions like `load_css()` and `load_sales_data()` can be reused across multiple apps.

