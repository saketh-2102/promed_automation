import pandas as pd
from functions import load_and_prepare_data, process_revenue_data, process_pharmacy_sales_data, load_pharmacy_sales_data

def main():
    file_path = "Budet_Data_with_Reference-v2.xlsx"

    # Load revenue and pharmacy data
    revenue_df = load_and_prepare_data(file_path, "REVENUE REPORT for IP&OP")
    pharmacy_df = load_pharmacy_sales_data(file_path, "PHARMACY SALES REPORT ")

    # Process and save results
    process_revenue_data(revenue_df, pharmacy_df)


if __name__ == "__main__":
    main()
