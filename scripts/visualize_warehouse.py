import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ==========================================
# CONFIGURATION
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, "..")
WH_DIR = os.path.join(PROJECT_ROOT, "data", "warehouse")
NOTEBOOK_DIR = os.path.join(PROJECT_ROOT, "notebook")
FIGURES_DIR = os.path.join(PROJECT_ROOT, "figures")

for d in [NOTEBOOK_DIR, FIGURES_DIR]:
    os.makedirs(d, exist_ok=True)

# ==========================================
# LOAD DATA
# ==========================================
def load_data():
    """Load and merge warehouse data"""
    print("\n--- Loading Data ---")
    fact = pd.read_csv(os.path.join(WH_DIR, "fact_orders.csv"))
    dim_c = pd.read_csv(os.path.join(WH_DIR, "dim_customers.csv"))
    dim_e = pd.read_csv(os.path.join(WH_DIR, "dim_employees.csv"))

    df = pd.merge(fact, dim_c, on="customer_key")
    df = pd.merge(df, dim_e, on="employee_key")
    
    df = df.drop(columns=['source_y', 'source'], errors='ignore')

    df = df.rename(columns={
        "source_x": "source",
        "country_x": "country"
    })

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["status"] = df["delivered"].apply(lambda x: "Delivered" if x == 1 else "Not Delivered")
    
    df["Client"] = df["companyname"]
    df["Employee"] = df["emp_norm"].str.title()
    
    print(f"✓ Loaded {len(df)} orders from {df['year'].min()} to {df['year'].max()}")
    return df

# ==========================================
# 3D VISUALIZATION
# ==========================================
def generate_3d_graph(df):
    """Generate interactive 3D scatter plot"""
    print("--- Generating 3D Sunset Graph ---")
    
    def get_mode(x):
        """Get most common value safely"""
        m = x.mode()
        if not m.empty:
            return m.iloc[0]
        return "mixed"

    df_3d = df.groupby(['year', 'Client', 'Employee', 'status']).agg(
        total_revenue=('revenue', 'sum'),
        order_count=('fact_key', 'count'),
        source=('source', get_mode)
    ).reset_index()

    color_map = {
        "Delivered": "#FF5733",
        "Not Delivered": "#581845"
    }

    fig = px.scatter_3d(
        df_3d,
        x='year',
        y='Client',
        z='Employee',
        color='status',
        size='total_revenue',
        size_max=40,
        opacity=0.9,
        color_discrete_map=color_map,
        hover_data={"total_revenue": ':$,.2f', "order_count": True, "source": True},
        title="<b>Logistics 3D Analysis (1996-2006)</b><br>Size = Revenue | Color = Status",
        labels={"year": "Year", "total_revenue": "Revenue ($)"}
    )

    fig.update_layout(
        scene=dict(
            xaxis=dict(title='Year', dtick=1, backgroundcolor="#F5F5F5"),
            yaxis=dict(title='Client', backgroundcolor="#F5F5F5"),
            zaxis=dict(title='Employee', backgroundcolor="#F5F5F5"),
        ),
        margin=dict(l=0, r=0, b=0, t=50),
        font=dict(family="Arial", size=11),
        paper_bgcolor="white"
    )

    out_path = os.path.join(NOTEBOOK_DIR, "3d_dashboard.html")
    fig.write_html(out_path)
    print(f"✓ 3D Graph saved to: {out_path}")

# ==========================================
# STATIC FIGURES
# ==========================================
def generate_static_figures(df):
    """Generate static analysis figures"""
    print("--- Generating Extra Static Figures ---")
    sns.set_theme(style="whitegrid")
    
    plt.figure(figsize=(10, 6))
    rev_by_year = df.groupby("year")["revenue"].sum().reset_index()
    sns.barplot(data=rev_by_year, x="year", y="revenue", hue="year", palette="Oranges_r", legend=False)
    plt.title("Total Revenue by Year", fontsize=14, fontweight='bold')
    plt.ylabel("Revenue ($)")
    plt.savefig(os.path.join(FIGURES_DIR, "revenue_by_year.png"))
    plt.close()
    print("✓ Saved: revenue_by_year.png")

    plt.figure(figsize=(10, 6))
    top_emp = df.groupby("Employee")["revenue"].sum().sort_values(ascending=False).head(10).reset_index()
    sns.barplot(data=top_emp, x="revenue", y="Employee", hue="Employee", palette="magma", legend=False)
    plt.title("Top 10 Employees by Revenue", fontsize=14, fontweight='bold')
    plt.xlabel("Revenue ($)")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, "top_employees_revenue.png"))
    plt.close()
    print("✓ Saved: top_employees_revenue.png")

    plt.figure(figsize=(8, 8))
    delivery_counts = df['status'].value_counts()
    colors = ["#28B5B9", "#E7183B"]
    plt.pie(delivery_counts, labels=delivery_counts.index, autopct='%1.1f%%', 
            startangle=90, colors=colors, textprops={'fontsize': 12})
    plt.title("Order Delivery Status", fontsize=14, fontweight='bold')
    plt.savefig(os.path.join(FIGURES_DIR, "delivery_status_pie.png"))
    plt.close()
    print("✓ Saved: delivery_status_pie.png")

    plt.figure(figsize=(14, 6))
    top_countries = df.groupby("country")["fact_key"].count().sort_values(ascending=False).head(15).reset_index()
    top_countries.columns = ["country", "order_count"]
    sns.barplot(data=top_countries, x="country", y="order_count", hue="country", palette="viridis", legend=False)
    plt.title("Top 15 Countries by Order Volume", fontsize=14, fontweight='bold')
    plt.xlabel("Country")
    plt.ylabel("Number of Orders")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_DIR, "top_countries_orders.png"))
    plt.close()
    print("✓ Saved: top_countries_orders.png")

# ==========================================
# EXECUTION
# ==========================================
if __name__ == "__main__":
    df = load_data()
    if not df.empty:
        generate_3d_graph(df)
        generate_static_figures(df)