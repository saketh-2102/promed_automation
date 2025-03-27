import pandas as pd

from functions import load_and_prepare_data, process_revenue_data

def main():
    file_path = "Budet_Data_with_Reference-v2.xlsx"
    sheet_name = "REVENUE REPORT for IP&OP"
    df = load_and_prepare_data(file_path, sheet_name)
    process_revenue_data(df)
   

if __name__ == "__main__":
    main()