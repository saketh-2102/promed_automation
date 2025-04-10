import streamlit as st
import pandas as pd
from functions import (
    load_and_prepare_data,
    load_pharmacy_sales_data,
    process_revenue_data
)

def check_login(username, password):
    """Simple login verification"""
    return username == "promed" and password == "promed11"



def main_app():
    st.set_page_config(page_title="Hospital Revenue Analysis", layout="wide")
    st.title("🏥 Hospital Revenue Analysis Dashboard")
    
    with st.sidebar:
        st.header("Upload Data")
        uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx"])
        
        if uploaded_file:
            st.success("File uploaded successfully!")
            sheet_revenue = st.text_input("Revenue Sheet Name", value="REVENUE REPORT for IP&OP")
            sheet_pharmacy = st.text_input("Pharmacy Sheet Name", value="PHARMACY SALES REPORT ")
            process_button = st.button("Process Data")
        else:
            st.warning("Please upload an Excel file")
            process_button = False

    if uploaded_file and process_button:
        try:
            with st.spinner("Processing data..."):
                revenue_df = load_and_prepare_data(uploaded_file, sheet_revenue)
                pharmacy_df = load_pharmacy_sales_data(uploaded_file, sheet_pharmacy)

                process_revenue_data(revenue_df, pharmacy_df)

            st.success("Analysis completed successfully!")
            st.subheader("Download Reports")

            col1, col2 = st.columns(2)

            with col1:
                with open("revenue_summary.xlsx", "rb") as f:
                    st.download_button(
                        label="Download Full Report",
                        data=f,
                        file_name="revenue_summary.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

            with col2:
                xls = pd.ExcelFile("revenue_summary.xlsx")
                tab1, tab2, tab3 = st.tabs(["IP Revenue", "OP Revenue", "Pharmacy Revenue"])

                with tab1:
                    st.dataframe(pd.read_excel(xls, sheet_name="IP Revenue"))
                with tab2:
                    st.dataframe(pd.read_excel(xls, sheet_name="OP Revenue"))
                with tab3:
                    st.dataframe(pd.read_excel(xls, sheet_name="Pharmacy Revenue"))

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")


    # Add some documentation
    with st.expander("ℹ️ How to use this application"):
        st.markdown("""
        ### Hospital Revenue Analysis Tool
        
        This application processes hospital revenue data from Excel files and categorizes it into:
        
        - **IP Revenue** (Inpatient)
        - **OP Revenue** (Outpatient)
        
        **Steps to use:**
        1. Upload your Excel file containing revenue data
        2. Enter the sheet name (default is 'REVENUE REPORT for IP&OP')
        3. Click 'Process Data' button
        4. View and download the results
        
        **Output includes:**
        - IP Revenue by department category
        - OP Revenue categorized by service type
        - Summary statistics for both IP and OP
        
        The tool automatically categorizes services based on predefined rules.
        """)

def login_page():
    """Displays login form"""
    st.title("Hospital Revenue Analysis - Login")
    
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit_button = st.form_submit_button("Login")
        
        if submit_button:
            if check_login(username, password):
                st.session_state.logged_in = True
                st.rerun()
            else:
                st.error("Invalid username or password")

def main():
    """Main function that controls the app flow"""
    if not hasattr(st.session_state, 'logged_in'):
        st.session_state.logged_in = False
    
    if st.session_state.logged_in:
        main_app()
    else:
        login_page()

if __name__ == "__main__":
    main()