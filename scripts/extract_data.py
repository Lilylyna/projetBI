import pandas as pd
import pyodbc
import os
import re

# ==========================================
# CONFIGURATION
# ==========================================
BASE_DIR = r"C:\Users\MY Laptop\Documents\projetBI\data"
RAW_DIR = os.path.join(BASE_DIR, "raw")

ACCESS_OUTPUT_DIR = os.path.join(RAW_DIR, "access")
SQL_OUTPUT_DIR = os.path.join(RAW_DIR, "sql")

ACCESS_FILENAME = "Northwind 2012.accdb"
SQL_SCRIPT_FILENAME = "scriptNorthwind.txt"

ACCESS_DB_PATH = os.path.join(BASE_DIR, ACCESS_FILENAME)
SQL_SCRIPT_PATH = os.path.join(BASE_DIR, SQL_SCRIPT_FILENAME)

SQL_SERVER_NAME = r'LEOPOOKS\SQLEXPRESS' 
TEMP_DB_NAME = 'Northwind_Temp_Export'
SQL_DRIVER = "ODBC Driver 17 for SQL Server"

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def setup_directories():
    """Create output directories if they don't exist"""
    for folder in [ACCESS_OUTPUT_DIR, SQL_OUTPUT_DIR]:
        if not os.path.exists(folder):
            try:
                os.makedirs(folder)
                print(f"Created folder: {folder}")
            except OSError as e:
                print(f"Error creating folder {folder}: {e}")

def clean_filename(name):
    """Remove special characters from filenames"""
    return "".join([c for c in name if c.isalpha() or c.isdigit() or c==' ' or c=='_']).strip()

# ==========================================
# EXTRACT FROM ACCESS
# ==========================================
def extract_access_data():
    """Extract all tables from Access database to CSV"""
    print(f"\n--- Processing Access File: {ACCESS_FILENAME} ---")
    
    if not os.path.exists(ACCESS_DB_PATH):
        print(f"Error: File not found at {ACCESS_DB_PATH}")
        return

    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={ACCESS_DB_PATH};"
    )

    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        tables = []
        for table_info in cursor.tables(tableType='TABLE'):
            t_name = table_info.table_name
            if not t_name.startswith("MSys") and not t_name.startswith("~"):
                tables.append(t_name)
        
        print(f"Found {len(tables)} tables. Exporting to: {ACCESS_OUTPUT_DIR}")

        for table in tables:
            try:
                query = f"SELECT * FROM [{table}]"
                df = pd.read_sql(query, conn)
                
                csv_name = f"{clean_filename(table)}.csv"
                out_path = os.path.join(ACCESS_OUTPUT_DIR, csv_name)
                
                df.to_csv(out_path, index=False, encoding='utf-8')
                print(f" [OK] {csv_name}")
            except Exception as e:
                print(f" [ERR] Failed to export {table}: {e}")

    except pyodbc.Error as e:
        print(f"Access Connection Error: {e}")
        print("Ensure 'Microsoft Access Database Engine' is installed (64-bit if Python is 64-bit).")
    finally:
        if conn:
            conn.close()

# ==========================================
# EXTRACT FROM SQL SCRIPT
# ==========================================
def extract_sql_script_data():
    """Execute SQL script and extract all tables to CSV"""
    print(f"\n--- Processing SQL Script: {SQL_SCRIPT_FILENAME} ---")
    
    if not os.path.exists(SQL_SCRIPT_PATH):
        print(f"Error: Script file not found at {SQL_SCRIPT_PATH}")
        return

    base_conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER_NAME};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;" 
    )

    try:
        conn = pyodbc.connect(base_conn_str + "DATABASE=master;", autocommit=True)
        cursor = conn.cursor()
        
        cursor.execute(f"IF DB_ID('{TEMP_DB_NAME}') IS NOT NULL DROP DATABASE {TEMP_DB_NAME}")
        cursor.execute(f"CREATE DATABASE {TEMP_DB_NAME}")
        print(f"Created temporary SQL Database: {TEMP_DB_NAME}")
        conn.close()

        conn = pyodbc.connect(base_conn_str + f"DATABASE={TEMP_DB_NAME};", autocommit=True)
        cursor = conn.cursor()

        print("Executing SQL commands from text file...")
        with open(SQL_SCRIPT_PATH, 'r', encoding='utf-8', errors='replace') as f:
            script_content = f.read()

        commands = re.split(r'\bGO\b', script_content, flags=re.IGNORECASE)

        for command in commands:
            if command.strip():
                try:
                    cursor.execute(command)
                except Exception as e:
                    pass

        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = [row.TABLE_NAME for row in cursor.fetchall()]
        
        print(f"Found {len(tables)} tables created. Exporting to: {SQL_OUTPUT_DIR}")

        for table in tables:
            try:
                df = pd.read_sql(f"SELECT * FROM [{table}]", conn)
                
                csv_name = f"{clean_filename(table)}.csv"
                out_path = os.path.join(SQL_OUTPUT_DIR, csv_name)
                
                df.to_csv(out_path, index=False, encoding='utf-8')
                print(f" [OK] {csv_name}")
            except Exception as e:
                print(f" [ERR] Failed to export {table}: {e}")
        
        conn.close()

    except pyodbc.Error as e:
        print(f"SQL Server Error: {e}")
        print(f"Ensure '{SQL_DRIVER}' is installed.")
        print("If you have a newer driver, change the SQL_DRIVER variable at the top of the script to 'ODBC Driver 18 for SQL Server'.")

# ==========================================
# EXECUTION
# ==========================================
if __name__ == "__main__":
    setup_directories()
    extract_access_data()
    extract_sql_script_data()
    print("\nAll tasks completed.")