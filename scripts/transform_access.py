import pandas as pd
import os
import unidecode
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

# ==========================================
# CONFIGURATION
# ==========================================
RAW = "../data/raw/access/" 
OUT = "../data/processed/access/" 
os.makedirs(OUT, exist_ok=True)

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def normalize_text(s):
    """Normalize text: remove accents, lowercase, strip whitespace"""
    if pd.isna(s): return ""
    s = str(s).strip().lower()
    s = unidecode.unidecode(s)
    return " ".join(s.split())

def clean_id(val):
    """Clean ID values to string format"""
    if pd.isna(val): return ""
    try:
        return str(int(float(val)))
    except:
        return str(val).strip()

print("\n--- TRANSFORMING ACCESS DATA ---")

# ==========================================
# CUSTOMERS
# ==========================================
try:
    df = pd.read_csv(os.path.join(RAW, "Customers.csv")) 
    norm = pd.DataFrame()
    norm["customer_source_id"] = df["ID"].apply(clean_id)
    norm["companyname"] = df["Company"]
    
    if "First Name" in df.columns and "Last Name" in df.columns:
        norm["contactname"] = df["First Name"] + " " + df["Last Name"]
    else:
        norm["contactname"] = ""
        
    norm["address"]     = df.get("Address", "")
    norm["city"]        = df.get("City", "")
    norm["region"]      = df.get("State/Province", "")
    norm["postalcode"]  = df.get("ZIP/Postal Code", "")
    norm["country"]     = df.get("Country/Region", "")
    norm["phone"]       = df.get("Business Phone", "")
    norm["fax"]         = df.get("Fax Number", "")
    norm["company_norm"] = norm["companyname"].apply(normalize_text)

    norm.to_csv(os.path.join(OUT, "customers_norm.csv"), index=False)
    print(f"✓ Customers: {len(norm)} rows")
except Exception as e:
    print(f"✘ Error Customers: {e}")

# ==========================================
# EMPLOYEES
# ==========================================
try:
    df = pd.read_csv(os.path.join(RAW, "Employees.csv"))
    norm = pd.DataFrame()
    norm["employee_source_id"] = df["ID"].apply(clean_id)
    norm["firstname"] = df.get("First Name", "")
    norm["lastname"]  = df.get("Last Name", "")
    norm["title"]     = df.get("Job Title", "")
    norm["address"]   = df.get("Address", "")
    norm["city"]      = df.get("City", "")
    norm["region"]    = df.get("State/Province", "")
    norm["postalcode"]= df.get("ZIP/Postal Code", "")
    norm["country"]   = df.get("Country/Region", "")
    norm["notes"]     = df.get("Notes", "")
    norm["emp_norm"]  = (norm["firstname"] + " " + norm["lastname"]).apply(normalize_text)

    norm.to_csv(os.path.join(OUT, "employees_norm.csv"), index=False)
    print(f"✓ Employees: {len(norm)} rows")
except Exception as e:
    print(f"✘ Error Employees: {e}")

# ==========================================
# ORDERS
# ==========================================
try:
    df = pd.read_csv(os.path.join(RAW, "Orders.csv"))
    
    df["Order Date"] = pd.to_datetime(df["Order Date"], dayfirst=True, errors="coerce")
    df["Shipped Date"] = pd.to_datetime(df["Shipped Date"], dayfirst=True, errors="coerce")

    norm = pd.DataFrame()
    norm["order_source_id"] = df["Order ID"].apply(clean_id)
    norm["customer_id_ref"] = df["Customer ID"].apply(clean_id)
    norm["employee_id_ref"] = df["Employee ID"].apply(clean_id)

    norm["orderdate"]  = df["Order Date"]
    norm["shippeddate"] = df["Shipped Date"]
    norm["shipcountry"] = df.get("Ship Country/Region", "")
    norm["freight"]     = pd.to_numeric(df.get("Shipping Fee", 0), errors='coerce').fillna(0)
    norm["delivered"] = norm["shippeddate"].notna().astype(int)

    norm.to_csv(os.path.join(OUT, "orders_norm.csv"), index=False)
    print(f"✓ Orders: {len(norm)} rows (Years: {norm['orderdate'].dt.year.min()}-{norm['orderdate'].dt.year.max()})")
except Exception as e:
    print(f"✘ Error Orders: {e}")