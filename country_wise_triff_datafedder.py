import pandas as pd
import psycopg2

# PostgreSQL Connection Details
DB_HOST = "35.188.74.32"
DB_PORT = "5432"
DB_NAME = "HSN_CODE_CLIENT"
DB_USER = "postgres"
DB_PASSWORD = "a55zWlt:CYtAi|FB(|jJSpRA90N}"
TABLE_NAME = "country_wise_tariff"
CSV_FILE = "country_wise_tariff_data.csv"
TEMP_CSV_FILE = "cleaned_data.csv"  # Temporary file for cleaned data

# Load CSV file
df = pd.read_csv(CSV_FILE, dtype=str, encoding="ISO-8859-1")

# Drop unnamed columns
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

# Drop rows where 'HS Code' is null
df = df.dropna(subset=['HS Code'])

# Trim spaces and remove leading zeros from 'HS Code'
df['HS Code'] = df['HS Code'].str.replace(" ", "").str.lstrip("0")

# Rename columns: Remove spaces, hyphens, slashes, dots, and parentheses
df.columns = df.columns.str.replace(r"[\s\-\/\.\(\)]", "_", regex=True).str.lower()

# Save cleaned data to a temporary CSV file
df.to_csv(TEMP_CSV_FILE, index=False, sep="|")  # Use '|' as delimiter to avoid issues

# Establish PostgreSQL connection
conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)
cursor = conn.cursor()

# Create table dynamically based on column names
column_definitions = ", ".join([f'"{col}" TEXT' for col in df.columns])
create_table_query = f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({column_definitions});'
cursor.execute(create_table_query)
conn.commit()

# Copy data from the cleaned CSV file into the table
with open(TEMP_CSV_FILE, "r", encoding="ISO-8859-1") as f:
    cursor.copy_expert(f"COPY {TABLE_NAME} FROM STDIN WITH CSV HEADER DELIMITER '|'", f)

# Commit and close connection
conn.commit()
cursor.close()
conn.close()

print("Data uploaded successfully using COPY!")
