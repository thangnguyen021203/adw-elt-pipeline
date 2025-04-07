import pyodbc
import os
from dotenv import load_dotenv
import re

load_dotenv()
# --- Config ---
mssql_conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.getenv('SERVER_MSSQL')};"
    f"DATABASE={os.getenv('DATABASE_MSSQL')};"
    f"UID={os.getenv('UID_MSSQL')};"
    f"PWD={os.getenv('PWD_MSSQL')};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

try:
    mssql_conn = pyodbc.connect(mssql_conn_str)
    print("Connected to MSSQL.")
    cursor = mssql_conn.cursor()
except pyodbc.Error as e:
    print(f"Failed to connect to MSSQL: {e}")
    exit(1)

cursor.execute("""
    SELECT TABLE_SCHEMA, TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE'
""")
tables = cursor.fetchall()

def normalize_table_name(name):
    # Insert underscore before each capital letter (except the first one)
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', name)
    # Convert to lowercase
    name = name.lower()
    # Replace spaces and hyphens with underscores
    name = name.replace(' ', '_').replace('-', '_')
    return name


for schema, table in tables:
    stg_model = """
{{ config(materialized='view') }}

SELECT 
    *
FROM {{ source('staging', 'STG_ADW_Table') }}
"""
    # After each CAP letter in the table name, add an underscore
    # and convert to uppercase
    # Example: "TableName" -> "table_name"
    # This is a simple normalization function
    # that converts the table name to lowercase and replaces
    # spaces and hyphens with underscores
    table_path = 'stg_' + normalize_table_name(table) + '.sql'
    stg_model = stg_model.replace("STG_ADW_Table", f"STG_ADW_{table}")
    with open(table_path, 'w') as file:
        file.write(stg_model)
    print(f"Generated staging model for {table}: {table_path}")
