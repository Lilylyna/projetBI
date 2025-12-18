import os
import pandas as pd

# ==========================================
# CONFIGURATION
# ==========================================
BASE = os.path.join(os.path.dirname(__file__), "..")
WH = os.path.join(BASE, "data", "warehouse")
OUT_DIR = os.path.join(WH, "kpi_summaries")
os.makedirs(OUT_DIR, exist_ok=True)

fact_path = os.path.join(WH, "fact_orders.csv")
dim_c_path = os.path.join(WH, "dim_customers.csv")
dim_e_path = os.path.join(WH, "dim_employees.csv")

# ==========================================
# LOAD DATA
# ==========================================
print("--- Loading Warehouse Data for KPIs ---")
if not os.path.exists(fact_path):
    print("❌ Error: fact_orders.csv not found.")
    exit()

fact = pd.read_csv(fact_path)
dim_c = pd.read_csv(dim_c_path)
dim_e = pd.read_csv(dim_e_path)

df = fact.merge(dim_c, on="customer_key", how="left")
df = df.merge(dim_e, on="employee_key", how="left")

rename_map = {
    "country_x": "country",
    "source_x": "source"
}
df = df.rename(columns=rename_map)

df["orderdate"] = pd.to_datetime(df["date"])

# ==========================================
# GLOBAL KPIs
# ==========================================
total_orders = len(df)
delivered = int(df["delivered"].sum())
not_delivered = total_orders - delivered
delivered_rate = (delivered / total_orders * 100) if total_orders > 0 else 0

print("\n===== 📊 GLOBAL PERFORMANCE =====")
print(f"Total Orders:    {total_orders}")
print(f"Total Revenue:   ${df['revenue'].sum():,.2f}")
print(f"Delivered:       {delivered}")
print(f"Pending:         {not_delivered}")
print(f"Delivery Rate:   {delivered_rate:.2f}%")

# ==========================================
# KPIs BY COUNTRY
# ==========================================
print("... Calculating Country KPIs")
orders_by_country = df.groupby('country').agg(
    total_orders=('fact_key', 'count'), 
    delivered=('delivered', 'sum'),
    total_revenue=('revenue', 'sum')
).reset_index()

orders_by_country['not_delivered'] = orders_by_country['total_orders'] - orders_by_country['delivered']
orders_by_country = orders_by_country.sort_values('total_orders', ascending=False)
orders_by_country.to_csv(os.path.join(OUT_DIR, "orders_by_country.csv"), index=False)

# ==========================================
# KPIs BY EMPLOYEE
# ==========================================
print("... Calculating Employee KPIs")
orders_by_employee = df.groupby('emp_norm').agg(
    total_orders=('fact_key', 'count'), 
    delivered=('delivered', 'sum'),
    total_revenue=('revenue', 'sum')
).reset_index()

orders_by_employee['not_delivered'] = orders_by_employee['total_orders'] - orders_by_employee['delivered']
orders_by_employee = orders_by_employee.sort_values('total_orders', ascending=False)
orders_by_employee.to_csv(os.path.join(OUT_DIR, "orders_by_employee.csv"), index=False)

# ==========================================
# KPIs BY MONTH
# ==========================================
print("... Calculating Timeline")
df['period'] = df['orderdate'].dt.to_period('M')

orders_by_month = df.groupby('period').agg(
    total_orders=('fact_key', 'count'), 
    delivered=('delivered', 'sum'),
    total_revenue=('revenue', 'sum')
).reset_index()

orders_by_month['not_delivered'] = orders_by_month['total_orders'] - orders_by_month['delivered']
orders_by_month = orders_by_month.sort_values('period')
orders_by_month.to_csv(os.path.join(OUT_DIR, "orders_by_month.csv"), index=False)

print(f"✅ All KPI files saved to: {OUT_DIR}")