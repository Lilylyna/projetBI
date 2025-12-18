import os
import pandas as pd
import numpy as np
import unidecode

# ==========================================
# CONFIGURATION
# ==========================================
BASE = os.path.join(os.path.dirname(__file__), "..")
RAW_SQL = os.path.join(BASE, "data", "raw", "sql")
RAW_ACCESS = os.path.join(BASE, "data", "raw", "access")
PROC_ACCESS = os.path.join(BASE, "data", "processed", "access")
WAREHOUSE = os.path.join(BASE, "data", "warehouse")
os.makedirs(WAREHOUSE, exist_ok=True)

# ==========================================
# HELPERS
# ==========================================
def find_csv(folder, candidates):
    """Find CSV file from list of candidate names"""
    if not os.path.exists(folder): return None
    files = {f.lower(): f for f in os.listdir(folder)}
    for c in candidates:
        if c.lower() in files: return os.path.join(folder, files[c.lower()])
    return None

def normalize_text(s):
    """Normalize text for matching: remove accents, lowercase, strip whitespace"""
    if pd.isna(s): return ""
    s = unidecode.unidecode(str(s)).lower().strip()
    return " ".join(s.split())

def clean_id(val):
    """Clean ID values to string format"""
    if pd.isna(val): return ""
    try:
        return str(int(float(val)))
    except:
        return str(val).strip()

def load_revenue_map(folder, id_col_name):
    """Load order details and calculate revenue per order"""
    path = find_csv(folder, ["Order Details.csv", "OrderDetails.csv"])
    if not path: return {}
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    oid_col = next((c for c in df.columns if id_col_name.lower() in c), "orderid")
    price_col = next((c for c in df.columns if "price" in c), "unitprice")
    qty_col = next((c for c in df.columns if "quantity" in c), "quantity")
    
    df["oid_clean"] = df[oid_col].apply(clean_id)
    df["rev"] = (pd.to_numeric(df[price_col], errors='coerce').fillna(0) * 
                 pd.to_numeric(df[qty_col], errors='coerce').fillna(0))
    return df.groupby("oid_clean")["rev"].sum().to_dict()

print("\n--- BUILDING DATA WAREHOUSE ---")
rev_sql = load_revenue_map(RAW_SQL, "OrderID")
rev_acc = load_revenue_map(RAW_ACCESS, "Order ID")

# ==========================================
# LOAD RAW DATA
# ==========================================
sql_c = pd.read_csv(find_csv(RAW_SQL, ["Customers.csv"]))
sql_e = pd.read_csv(find_csv(RAW_SQL, ["Employees.csv"]))
sql_o = pd.read_csv(find_csv(RAW_SQL, ["Orders.csv"]))

acc_c = pd.read_csv(os.path.join(PROC_ACCESS, "customers_norm.csv"))
acc_e = pd.read_csv(os.path.join(PROC_ACCESS, "employees_norm.csv"))
acc_o = pd.read_csv(os.path.join(PROC_ACCESS, "orders_norm.csv"))

# ==========================================
# CUSTOMERS DIMENSION
# ==========================================
c1 = pd.DataFrame({
    "customerid": sql_c["CustomerID"].astype(str), 
    "companyname": sql_c["CompanyName"], 
    "country": sql_c.get("Country", ""),
    "city": sql_c.get("City", ""),
    "region": sql_c.get("Region", ""),
    "source": "sql"
})
c1["company_norm"] = c1["companyname"].apply(normalize_text)

c2 = pd.DataFrame({
    "customerid": acc_c["customer_source_id"].astype(str), 
    "companyname": acc_c["companyname"], 
    "country": acc_c["country"],
    "city": acc_c["city"],
    "region": acc_c["region"],
    "source": "access"
})
c2["company_norm"] = acc_c["company_norm"]

dim_c = pd.concat([c1, c2], ignore_index=True)
dim_c = dim_c.sort_values(["company_norm", "source"], ascending=[True, False]).drop_duplicates("company_norm").reset_index(drop=True)
dim_c.insert(0, "customer_key", range(1, len(dim_c)+1))
dim_c.to_csv(os.path.join(WAREHOUSE, "dim_customers.csv"), index=False)

# ==========================================
# EMPLOYEES DIMENSION
# ==========================================
e1 = pd.DataFrame({
    "employeeid": sql_e["EmployeeID"].apply(clean_id), 
    "name": sql_e["FirstName"]+" "+sql_e["LastName"], 
    "title": sql_e.get("Title", ""),
    "country": sql_e.get("Country", ""),
    "source": "sql"
})
e1["emp_norm"] = e1["name"].apply(normalize_text)

e2 = pd.DataFrame({
    "employeeid": acc_e["employee_source_id"].astype(str), 
    "name": acc_e["firstname"]+" "+acc_e["lastname"], 
    "title": acc_e["title"],
    "country": acc_e["country"],
    "source": "access"
})
e2["emp_norm"] = acc_e["emp_norm"]

dim_e = pd.concat([e1, e2], ignore_index=True)
dim_e = dim_e.sort_values(["emp_norm", "source"], ascending=[True, False]).drop_duplicates("emp_norm").reset_index(drop=True)
dim_e.insert(0, "employee_key", range(1, len(dim_e)+1))
dim_e.to_csv(os.path.join(WAREHOUSE, "dim_employees.csv"), index=False)

# ==========================================
# ORDERS FACT TABLE
# ==========================================
o1 = pd.DataFrame()
o1["orderid"] = sql_o["OrderID"].apply(clean_id)
o1["date"] = pd.to_datetime(sql_o["OrderDate"])
o1["shipped"] = pd.to_datetime(sql_o["ShippedDate"])
o1["delivered"] = o1["shipped"].notna().astype(int)
o1["source"] = "sql"
o1["c_ref"] = sql_o["CustomerID"].astype(str)
o1["e_ref"] = sql_o["EmployeeID"].apply(clean_id)
o1["revenue"] = o1["orderid"].map(rev_sql).fillna(0)

o2 = pd.DataFrame()
o2["orderid"] = acc_o["order_source_id"].astype(str)
o2["date"] = pd.to_datetime(acc_o["orderdate"])
o2["shipped"] = pd.to_datetime(acc_o["shippeddate"])
o2["delivered"] = acc_o["delivered"]
o2["source"] = "access"
o2["c_ref"] = acc_o["customer_id_ref"].astype(str)
o2["e_ref"] = acc_o["employee_id_ref"].astype(str)
o2["revenue"] = o2["orderid"].map(rev_acc).fillna(0)

fact = pd.concat([o1, o2], ignore_index=True)

c_map = dict(zip(dim_c["customerid"].astype(str), dim_c["customer_key"]))
e_map = dict(zip(dim_e["employeeid"].astype(str), dim_e["employee_key"]))

fact["customer_key"] = fact["c_ref"].map(c_map)
fact["employee_key"] = fact["e_ref"].map(e_map)

fact = fact.dropna(subset=["customer_key", "employee_key"])
fact.insert(0, "fact_key", range(1, len(fact)+1))

# ==========================================
# TIME DIMENSION
# ==========================================
dates = pd.date_range(fact["date"].min(), fact["date"].max())
dim_t = pd.DataFrame({"date": dates})
dim_t["year"] = dim_t["date"].dt.year
dim_t.to_csv(os.path.join(WAREHOUSE, "dim_temps.csv"), index=False)

fact.to_csv(os.path.join(WAREHOUSE, "fact_orders.csv"), index=False)

print(f"✅ Warehouse Built.")
print(f"   Total Orders: {len(fact)}")
print(f"   Date Range: {fact['date'].dt.year.min()} to {fact['date'].dt.year.max()}")