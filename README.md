# Northwind Unified BI Pipeline

## End-to-End Data Warehouse & 3D Analytics

This project implements a complete Business Intelligence (BI) solution designed to unify heterogeneous data sources from Microsoft Access (.accdb) and SQL Server using the Northwind database.

Python is used as the orchestration layer to perform Extraction, Transformation, and Loading (ETL), followed by analytical processing and multidimensional visualization.

The pipeline consolidates fragmented operational data into a Star Schema Data Warehouse, enabling reliable KPI computation and interactive 3D analytical exploration.

---

## 📂 Project Structure

```plaintext
projetBI/
├── data/
│   ├── raw/                # Staging area: raw CSV exports from Access & SQL Server
│   ├── processed/          # Transformed and normalized datasets
│   └── warehouse/          # Final Data Warehouse (Fact & Dimension tables)
├── scripts/                # Python ETL and analytics pipeline
│   ├── extract_data.py     # Data extraction from SQL Server & Microsoft Access
│   ├── datawarehouse.py   # Star Schema construction and revenue mapping
│   ├── kpi_analysis.py     # Computation of global business KPIs
│   └── visualize_warehouse.py # Interactive 3D analytical dashboard
├── figures/                # Static analytical visualizations (PNG)
├── dashboard/              # Interactive HTML dashboard
├── notebook/
│   ├── Dashboard_Analysis.ipynb
│   └── 3d_dashboard.html   # Interactive 3D multidimensional graph
├── report/                 # Word + PDF project report
├── video/                  # Presentation video (screen + voice)
├── venv/                   # Optional Python virtual environment
└── README.md               # Project documentation


🛠️ Installation & Requirements

1. Prerequisites

Python 3.8+

Microsoft Access Database Engine (required for Access ODBC connectivity)

SQL Server (local instance with Northwind database)

SQL script: Northwind.txt

2. Install Dependencies

Run the following command:

pip install pandas pyodbc unidecode plotly matplotlib seaborn

- Library Purpose
  pandas -> Data manipulation and transformation
  pyodbc -> Database connectivity (Access & SQL Server)
  unidecode -> Text normalization and deduplication
  plotly -> Interactive 3D analytics
  matplotlib / seaborn -> Static reporting and trend visualization

3. Run virtual envirement (Optional)
   python -m venv venv
   venv\Scripts\activate

**_🚀Work Flow_**
Run the pipeline in the following order:
**note: when developing the results of data will be stored in directory "C:\Users\MY Laptop\Documents\projetBI\data"**
**And note that the GitHub will have the finished results of the solution**


**1. Extraction**
python scripts/extract_data.py

Extracts data into data/raw/.


**2. Transformation & Loading**
python scripts/transform_access.py
python scripts/datawarehouse.py

Cleans data, resolves duplicates, and builds the Star Schema in data/warehouse/.

**3. Analysis**
python scripts/kpi_analysis.py

Computes and displays key business indicators.

**4. Visualization**
python scripts/visualize_3d.py

Launches the interactive 3D analytical dashboard.

**📊 Core Features**

Duplicate Resolution
Identifies customers present in both Access and SQL Server using text normalization.

Surrogate Key Mapping
Replaces inconsistent source identifiers with unified integer keys.

Revenue Engine
Dynamically computes order totals by joining transaction headers and line items.

3D Multidimensional Analysis
Explores relationships between Time, Customers, and Employees in a single interactive view.

**⚠️ Troubleshooting & Environment Setup**
**1. ODBC Driver Requirements**

If you encounter a Data source name not found error:

Microsoft Access

Install Microsoft Access Database Engine 2016 Redistributable

Ensure the driver architecture matches your Python version
(64-bit Python → 64-bit driver)

SQL Server

Install the ODBC Driver for SQL Server

Verify installed drivers via ODBC Data Sources in Windows

**2. SQL Server Configuration**

Before running extraction scripts:

Open SQL Server Management Studio (SSMS)

Execute the provided Northwind.txt script

Ensure the connection string in extract_sql.py matches your local instance
(e.g. localhost or .\SQLEXPRESS)

**3. Common Error Fixes**

_ModuleNotFoundError_
pip install dependecies mentioned UP

_Permission Denied_
Ensure the .accdb file is not open in Microsoft Access during execution.

_Encoding Issues_
Confirm unidecode is installed and scripts are executed in a UTF-8 compatible terminal.

Business Intelligence Project — 2024/2025
By - Korichi Lyna Racha
```
