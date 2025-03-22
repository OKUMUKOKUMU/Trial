import pandas as pd
import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import os
from datetime import datetime
import plotly.express as px

# Load environment variables
load_dotenv()

def connect_to_gsheet(spreadsheet_name, sheet_name):
    """
    Authenticate and connect to Google Sheets.
    """
    scope = ["https://spreadsheets.google.com/feeds", 
             "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", 
             "https://www.googleapis.com/auth/drive"]
    
    try:
        credentials = {
            "type": "service_account",
            "project_id": os.getenv("GOOGLE_PROJECT_ID"),
            "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
            "private_key": os.getenv("GOOGLE_PRIVATE_KEY").replace("\\n", "\n"),
            "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
            "client_id": os.getenv("GOOGLE_CLIENT_ID"),
            "auth_uri": os.getenv("GOOGLE_AUTH_URI"),
            "token_uri": os.getenv("GOOGLE_TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("GOOGLE_AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_X509_CERT_URL")
        }

        client_credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials, scope)
        client = gspread.authorize(client_credentials)
        spreadsheet = client.open(spreadsheet_name)  
        return spreadsheet.worksheet(sheet_name)  # Access specific sheet by name
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets: {e}")
        return None

def load_data_from_google_sheet():
    """
    Load data from Google Sheets.
    """
    with st.spinner("Loading data from Google Sheets..."):
        try:
            worksheet = connect_to_gsheet(SPREADSHEET_NAME, SHEET_NAME)
            if worksheet is None:
                return None
            
            # Get all records from the Google Sheet
            data = worksheet.get_all_records()
            
            if not data:
                st.error("No data found in the Google Sheet.")
                return None

            # Convert data to DataFrame
            df = pd.DataFrame(data)

            # Ensure columns match the updated Google Sheets structure
            df.columns = ["DATE", "ITEM_SERIAL", "ITEM NAME", "DEPARTMENT", "ISSUED_TO", "QUANTITY", 
                        "UNIT_OF_MEASURE", "ITEM_CATEGORY", "WEEK", "REFERENCE", 
                        "DEPARTMENT_CAT", "BATCH NO.", "STORE", "RECEIVED BY"]

            # Convert date and numeric columns
            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
            df["QUANTITY"] = pd.to_numeric(df["QUANTITY"], errors="coerce")
            df.dropna(subset=["QUANTITY"], inplace=True)
            
            # Extract quarter information
            df["QUARTER"] = df["DATE"].dt.to_period("Q")

            # Filter data for 2024 onwards
            current_year = datetime.now().year
            df = df[df["DATE"].dt.year >= current_year - 1]  # Data from last year onwards

            return df
        except Exception as e:
            st.error(f"Error loading data: {e}")
            return None

@st.cache_data(ttl=3600)  # Cache data for 1 hour
def get_cached_data():
    return load_data_from_google_sheet()

def calculate_proportion(df, identifier, department=None, min_proportion=1.0):
    """
    Calculate department-wise usage proportion without subdepartment details.
    Ensures all departments sum to 100%.
    Filters out departments with proportions less than min_proportion.
    """
    if df is None:
        return None
    
    try:
        if identifier.isnumeric():
            filtered_df = df[df["ITEM_SERIAL"].astype(str).str.lower() == identifier.lower()]
        else:
            filtered_df = df[df["ITEM NAME"].str.lower() == identifier.lower()]

        if filtered_df.empty:
            return None

        # If department is specified, filter by department
        if department and department != "All Departments":
            filtered_df = filtered_df[filtered_df["DEPARTMENT"] == department]
            if filtered_df.empty:
                return None

        # Calculate department-level proportions only
        dept_usage = filtered_df.groupby("DEPARTMENT")["QUANTITY"].sum().reset_index()
        
        # Calculate total across all departments
        total_usage = dept_usage["QUANTITY"].sum()
        
        if total_usage == 0:
            return None
            
        # Calculate each department's proportion of the total
        dept_usage["PROPORTION"] = (dept_usage["QUANTITY"] / total_usage) * 100
        
        # Filter out departments with proportions less than min_proportion
        significant_depts = dept_usage[dept_usage["PROPORTION"] >= min_proportion].copy()
        
        # If no departments meet the threshold, return the one with the highest proportion
        if significant_depts.empty and not dept_usage.empty:
            significant_depts = pd.DataFrame([dept_usage.iloc[dept_usage["PROPORTION"].idxmax()]])
        
        # Recalculate proportions to ensure they sum to 100%
        total_proportion = significant_depts["PROPORTION"].sum()
        significant_depts["PROPORTION"] = (significant_depts["PROPORTION"] / total_proportion) * 100
        
        # Calculate relative weights for sorting
        significant_depts["QUANTITY_ABS"] = significant_depts["QUANTITY"].abs()
        significant_depts["INTERNAL_WEIGHT"] = significant_depts["QUANTITY_ABS"] / significant_depts["QUANTITY_ABS"].sum()
        
        # Sort by proportion (descending)
        significant_depts.sort_values(by=["PROPORTION"], ascending=[False], inplace=True)
        
        return significant_depts
    except Exception as e:
        st.error(f"Error calculating proportions: {e}")
        return None

def allocate_quantity(df, identifier, available_quantity, department=None):
    """
    Allocate quantity based on historical proportions at department level only.
    Filters out departments with less than 1% proportion.
    """
    proportions = calculate_proportion(df, identifier, department, min_proportion=1.0)
    if proportions is None:
        return None
    
    # Calculate allocated quantity for each department based on their proportion
    proportions["ALLOCATED_QUANTITY"] = (proportions["PROPORTION"] / 100) * available_quantity
    
    # Round allocated quantities
    proportions["ALLOCATED_QUANTITY"] = proportions["ALLOCATED_QUANTITY"].round(0)
    
    # Ensure the sum matches the available quantity exactly
    allocated_sum = proportions["ALLOCATED_QUANTITY"].sum()
    if abs(allocated_sum - available_quantity) > 0.01 and len(proportions) > 0:  # Allow small rounding error
        difference = int(available_quantity - allocated_sum)
        if difference != 0:
            # Add/subtract the difference from the largest allocation
            index_max = proportions["ALLOCATED_QUANTITY"].idxmax()
            proportions.at[index_max, "ALLOCATED_QUANTITY"] += difference
    
    return proportions

def generate_allocation_chart(result_df, item_name):
    """
    Generate a bar chart for allocation results.
    """
    # Create a summarized version for charting (by DEPARTMENT only)
    chart_df = result_df.copy()
    
    # Create a bar chart
    fig = px.bar(
        chart_df, 
        x="DEPARTMENT", 
        y="ALLOCATED_QUANTITY",
        text="ALLOCATED_QUANTITY",
        title=f"Allocation for {item_name} by Department",
        labels={
            "DEPARTMENT": "Department",
            "ALLOCATED_QUANTITY": "Allocated Quantity"
        },
        height=400,
        color_discrete_sequence=px.colors.qualitative.Vivid
    )
    
    # Customize the layout
    fig.update_layout(
        xaxis_title="Department",
        yaxis_title="Allocated Quantity"
    )
    
    return fig

# Streamlit UI
st.set_page_config(
    page_title="SPP Ingredients Allocation App", 
    layout="centered",
    initial_sidebar_state="expanded"
)

# Custom CSS for improved appearance
st.markdown("""
    <style>
    .title {
        text-align: center;
        font-size: 36px;
        font-weight: bold;
        color: #2E86C1;
        font-family: 'Arial', sans-serif;
        margin-bottom: 10px;
    }
    .subtitle {
        text-align: center;
        font-size: 18px;
        color: #6c757d;
        margin-bottom: 30px;
    }
    .footer {
        text-align: center;
        font-size: 12px;
        color: #888888;
        margin-top: 30px;
    }
    .stButton button {
        background-color: #2E86C1;
        color: white;
        font-weight: bold;
        border-radius: 5px;
        padding: 10px 20px;
    }
    .stButton button:hover {
        background-color: #1C6EA4;
    }
    .card {
        background-color: #ffffff;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
    }
    .filter-header {
        font-size: 20px;
        font-weight: bold;
        color: #2E86C1;
        margin-bottom: 10px;
    }
    .stDataFrame {
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .stSelectbox, .stNumberInput, .stMultiselect {
        margin-bottom: 15px;
    }
    </style>
""", unsafe_allow_html=True)

# Main title and subtitle
st.markdown("<h1 class='title'>SPP Ingredients Allocation App</h1>", unsafe_allow_html=True)
st.markdown("<p class='subtitle'>Efficiently allocate ingredients based on historical usage</p>", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("<h2 class='title'>SPP Ingredients Allocation</h2>", unsafe_allow_html=True)
    st.markdown("<p class='subtitle'>Allocation Settings</p>", unsafe_allow_html=True)
    
    # Google Sheet credentials and details
    SPREADSHEET_NAME = 'BROWNS STOCK MANAGEMENT'
    SHEET_NAME = 'CHECK_OUT'
    
    # Load the data
    if "data" not in st.session_state:
        st.session_state.data = get_cached_data()
    
    data = st.session_state.data
    
    if data is None:
        st.error("Failed to load data from Google Sheets. Please check your connection and credentials.")
        st.stop()
    
    # Extract unique item names, categories, and departments for filtering
    unique_item_names = sorted(data["ITEM NAME"].unique().tolist())
    unique_categories = sorted(data["ITEM_CATEGORY"].unique().tolist())
    unique_departments = sorted(["All Departments"] + data["DEPARTMENT"].unique().tolist())
    
    st.markdown("### Quick Stats")
    st.metric("Total Items", f"{len(unique_item_names)}")
    st.metric("Total Departments", f"{len(unique_departments) - 1}")  # Exclude "All Departments"
    
    # Refresh data button
    if st.button("Refresh Data"):
        st.session_state.data = load_data_from_google_sheet()
        st.success("Data refreshed successfully!")
    
    st.markdown("---")
    st.markdown("### View Options")
    view_mode = st.radio("Select View", ["Allocation Calculator", "Data Overview"])
    
    st.markdown("---")
    st.markdown("<p class='footer'>Developed by Brown's Data Team, ©2025</p>", unsafe_allow_html=True)

# Main content
if view_mode == "Allocation Calculator":
    # Form Layout for Better UX
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown("### Enter Items and Quantities")
    
    with st.form("allocation_form"):
        num_items = st.number_input("Number of items to allocate", min_value=1, max_value=10, step=1, value=1)
        
        # Department selection
        selected_department = st.selectbox("Filter by Department (optional)", unique_departments)

        entries = []
        for i in range(num_items):
            st.markdown(f"**Item {i+1}**")
            col1, col2 = st.columns([2, 1])
            with col1:
                identifier = st.selectbox(f"Select item {i+1}", unique_item_names, key=f"item_{i}")
            with col2:
                available_quantity = st.number_input(f"Quantity:", min_value=0.1, step=0.1, key=f"qty_{i}")

            if identifier and available_quantity > 0:
                entries.append((identifier, available_quantity))

        submitted = st.form_submit_button("Calculate Allocation")
    st.markdown("</div>", unsafe_allow_html=True)

    # Processing Allocation
    if submitted:
        if not entries:
            st.warning("Please enter at least one valid item and quantity!")
        else:
            for identifier, available_quantity in entries:
                result = allocate_quantity(data, identifier, available_quantity, selected_department)
                if result is not None:
                    st.markdown("<div class='card'>", unsafe_allow_html=True)
                    st.markdown(f"<div class='result-header'><h3 style='color: #2E86C1;'>Allocation for {identifier}</h3></div>", unsafe_allow_html=True)
                    
                    # Format the output for better readability
                    formatted_result = result[["DEPARTMENT", "PROPORTION", "ALLOCATED_QUANTITY"]].copy()
                    formatted_result = formatted_result.rename(columns={
                        "DEPARTMENT": "Department",
                        "PROPORTION": "Proportion (%)",
                        "ALLOCATED_QUANTITY": "Allocated Quantity"
                    })
                    
                    # Format numeric columns
                    formatted_result["Proportion (%)"] = formatted_result["Proportion (%)"].round(2)
                    formatted_result["Allocated Quantity"] = formatted_result["Allocated Quantity"].astype(int)
                    
                    # Display the result
                    st.dataframe(formatted_result, use_container_width=True)
                    
                    # Summary statistics
                    st.markdown("#### Allocation Summary")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total Allocated", f"{formatted_result['Allocated Quantity'].sum():,.0f}")
                    with col2:
                        st.metric("Departments", f"{formatted_result['Department'].nunique()}")
                    
                    # Add a download button for the result
                    csv = formatted_result.to_csv(index=False)
                    st.download_button(
                        label="Download Allocation as CSV",
                        data=csv,
                        file_name=f"{identifier}_allocation.csv",
                        mime="text/csv",
                    )
                    
                    # Show visualization
                    chart = generate_allocation_chart(result, identifier)
                    st.plotly_chart(chart, use_container_width=True)
                    
                    st.markdown("</div>", unsafe_allow_html=True)
                else:
                    st.error(f"Item {identifier} not found in historical data or has no usage data for the selected department!")

elif view_mode == "Data Overview":
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown("### Data Overview")
    
    # Advanced Filters
    with st.expander("🔍 Advanced Filters", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            # Date range filter
            min_date = data["DATE"].min().date()
            max_date = data["DATE"].max().date()
            date_range = st.date_input("Select Date Range", [min_date, max_date])
        with col2:
            # Multi-select for item categories
            selected_categories = st.multiselect("Filter by Item Categories", unique_categories, default=[])
        
        col3, col4 = st.columns(2)
        with col3:
            # Multi-select for items
            selected_items = st.multiselect("Filter by Items", unique_item_names, default=[])
        with col4:
            # Multi-select for departments
            selected_overview_dept = st.multiselect("Filter by Departments", unique_departments[1:], default=[])  # Exclude "All Departments"
    
    # Apply filters
    filtered_data = data.copy()
    if date_range:
        filtered_data = filtered_data[(filtered_data["DATE"].dt.date >= date_range[0]) & 
                                     (filtered_data["DATE"].dt.date <= date_range[1])]
    if selected_categories:
        filtered_data = filtered_data[filtered_data["ITEM_CATEGORY"].isin(selected_categories)]
    if selected_items:
        filtered_data = filtered_data[filtered_data["ITEM NAME"].isin(selected_items)]
    if selected_overview_dept:
        filtered_data = filtered_data[filtered_data["DEPARTMENT"].isin(selected_overview_dept)]
    
    # Show data overview
    st.markdown("#### Filtered Data Preview")
    display_columns = ["DATE", "ITEM NAME", "DEPARTMENT", "QUANTITY", "UNIT_OF_MEASURE", "ITEM_CATEGORY"]
    st.dataframe(filtered_data[display_columns].head(100), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Simple statistics
    # Simple statistics
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown("#### Usage Statistics")
    total_usage = filtered_data["QUANTITY"].sum()
    unique_items_count = filtered_data["ITEM NAME"].nunique()
    
    stat_col1, stat_col2, stat_col3 = st.columns(3)
    with stat_col1:
        st.metric("Total Quantity Used", f"{total_usage:,.2f}")
    with stat_col2:
        st.metric("Unique Items", f"{unique_items_count}")
    with stat_col3:
        st.metric("Total Transactions", f"{len(filtered_data):,}")
    
    # Usage by department visualization
    if not filtered_data.empty:
        st.markdown("#### Department Usage")
        dept_usage = filtered_data.groupby("DEPARTMENT")["QUANTITY"].sum().reset_index()
        dept_usage.sort_values(by="QUANTITY", ascending=False, inplace=True)
        
        fig = px.pie(
            dept_usage, 
            values="QUANTITY", 
            names="DEPARTMENT", 
            title="Usage Distribution by Department",
            hole=0.4
        )
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)
