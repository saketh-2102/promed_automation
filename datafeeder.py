import psycopg2
import pandas as pd
import os
import csv

DB_CONFIG = {
    "user": os.getenv("DB_USER", "postgres"),
    "host": os.getenv("DB_HOST", "35.188.74.32"),
    "database": os.getenv("DB_NAME", "HSN_CODE_CLIENT"),
    "password": os.getenv("DB_PASSWORD", "a55zWlt:CYtAi|FB(|jJSpRA90N}"),
    "port": 5432,
}

CSV_FILE = "client_data.csv"
TEMP_CSV = "temp_data.csv"

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Connected to the database successfully.")

    # Read only the columns we need
    df = pd.read_csv(CSV_FILE, usecols=["cth", "description"], dtype=str)
    
    # Clean the data
    df.fillna("", inplace=True)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Select only the columns we want to upload
    columns = ["cth", "description"]
    df = df[columns]

    print("CSV Columns:", df.columns.tolist())
    print("Number of Columns in CSV:", len(df.columns))
    print("First few rows:")
    print(df.head())

    # Write to temporary CSV
    df.to_csv(TEMP_CSV, index=False, header=False, sep=",", 
              quoting=csv.QUOTE_ALL, escapechar="\\")

    # Use COPY to upload just these two columns
    with open(TEMP_CSV, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            f"COPY hsn_codes (cth, description) FROM STDIN WITH (FORMAT CSV)",
            f
        )

    conn.commit()
    print(f"{len(df)} rows inserted successfully.")

except Exception as e:
    print(f"Error: {e}")
    if conn:
        conn.rollback()

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    if os.path.exists(TEMP_CSV):
        os.remove(TEMP_CSV)
    print("Database connection closed.")