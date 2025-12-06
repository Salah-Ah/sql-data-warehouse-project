# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀

This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics using **Medallion Architecture** (Bronze, Silver, and Gold layers).

---

## 📋 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse using **Medallion Architecture** (Bronze, Silver, and Gold layers).
2. **ETL Pipelines**: Extracting, transforming, and loading data from multiple source systems (CRM and ERP) into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries using **Star Schema**.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

💡 **This repository is an excellent resource for professionals and students looking to showcase expertise in:**

- SQL Development
- Data Architecture
- Data Engineering
- ETL Pipeline Development
- Data Modeling
- Data Analytics

---

## 🔗 Important Links & Tools

**Everything is for free!**

- **[Datasets]([datasets/](https://github.com/Salah-Ah/sql-data-warehouse-project/tree/main/datasets))**: Access to the project datasets (CRM and ERP CSV files)
- **[SQL Server Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads)**: Lightweight server for hosting your SQL database.
- **[SQL Server Management Studio (SSMS)](https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms)**: GUI for managing and interacting with databases.
- **[Git Repository](https://github.com/Salah-Ah/sql-data-warehouse-project)**: Set up a GitHub account and repository to manage, version, and collaborate on your code efficiently.
- **[Draw.io](https://app.diagrams.net/)**: Design data architecture, models, flows, and diagrams.
- **[Notion](https://www.notion.so/)**: All-in-one tool for project management and organization.
- **[Project Documentation](https://github.com/Salah-Ah/sql-data-warehouse-project/tree/main/docs)**: Access detailed project phases and documentation.

---

## ⚙️ Prerequisites

Before running this project, ensure you have the following tools installed:

- **SQL Server** (Express or Developer Edition)
- **SQL Server Management Studio (SSMS)**
- **Basic knowledge of SQL** (DDL, DML, DQL)
- **Understanding of data warehousing concepts** (optional but helpful)

---

## 🎯 Project Requirements

### Building the Data Warehouse (Data Engineering)

**Objective:**

Develop a modern data warehouse using SQL Server to consolidate sales data from multiple sources (CRM and ERP systems), enabling analytical reporting and informed decision-making.

**Specifications:**

- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### BI: Analytics & Reporting (Data Analytics)

**Objective:**

Develop SQL-based analytics to deliver detailed insights into:

- Customer Behavior
- Product Performance
- Sales Trends

These insights empower stakeholders with key business metrics, enabling strategic decision-making.

---

## 🖼️ Background & Overview

Building a data warehouse saves end users (like data analysts and data scientists) significant time and effort. It ensures that tables are clean, well-structured, and ready to query whenever needed—especially when dealing with multiple data sources that would otherwise require repetitive joining and cleaning of the same columns.

Using **Medallion Architecture** ensures this goal is achieved by organizing data into three progressive layers:

- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Data cleansing and transformation
- **Gold Layer**: Business-ready analytical models

This layered approach guarantees data quality, traceability, and scalability throughout the data lifecycle.

---

## 🏢 Data Architecture

The data architecture for this project follows **Medallion Architecture** with **Bronze, Silver, and Gold layers**.

### Data Flow & Lineage
<img width="937" height="767" alt="Data FLow" src="https://github.com/user-attachments/assets/ec3a2dca-b340-4bde-ae25-dbf3648af1c7" />


The diagram above illustrates how data flows from the **source systems** (CRM and ERP) through the warehouse layers:

1. **Sources**: CSV files containing CRM and ERP data
2. **Bronze Layer**: Raw data imported as-is into SQL Server tables
3. **Silver Layer**: Cleaned and transformed data with quality checks applied
4. **Gold Layer**: Dimensional model (Star Schema) ready for analytics and reporting

---

### Architecture Diagram
<img width="1337" height="882" alt="Data Architecture" src="https://github.com/user-attachments/assets/fde03f63-fcbd-4a76-84ea-6c8b220a9a9c" />


#### Layer Breakdown:

**🟤 Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.

- **Object Type**: Tables
- **Load**: 
  - Batch Processing
  - Full loads
  - Truncate & Insert
- **No Transformation**
- **Data Model**: None (as-is)

**⚪ Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.

- **Object Type**: Tables
- **Load**: 
  - Batch Processing
  - Full loads
  - Truncate & Insert
- **Transformations**:
  - Data Cleaning
  - Data Standardization
  - Data Normalization
  - Data Enrichment
  - Derived Columns
- **Data Model**: None (as-is)

**🟡 Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

- **Object Type**: Views
- **No Load**
- **Transformations**:
  - Data Integrations
  - Aggregations
  - Business Logics
- **Data Model**:
  - Star Schema
  - Flat Table
  - Aggregated Table

---

### Data Model (Star Schema)
<img width="1213" height="753" alt="Data model (Star schema)" src="https://github.com/user-attachments/assets/c5c8bc37-db45-45c8-8543-bd1355fb2293" />


The **Gold Layer** uses a **Star Schema** consisting of:

- **Fact Table**: `gold.fact_sales` - Contains transactional sales data
- **Dimension Tables**:
  - `gold.dim_customers` - Customer information (demographics, location)
  - `gold.dim_products` - Product details (categories, pricing)

This model optimizes query performance for analytical workloads and provides a clean interface for business intelligence tools.

**Key Design Principles**:
- Surrogate keys for all dimension tables
- Slowly Changing Dimensions (SCD) Type 1 approach
- Denormalized structure for fast query performance
- Clear, business-friendly column names

---

## 📃 Executive Summary

The main expectations and benefits from each layer in this warehouse are:

### 🟤 Bronze Layer
This layer ensures saving the raw data after importing it from different sources without touching the content of the data to keep it as-is, providing a ready-to-use form of the raw data before doing any transformations. This serves as the **single source of truth** and enables data lineage tracking.

### ⚪ Silver Layer
This layer ensures the data is well-prepared for modeling and analysis by:
- Creating surrogate keys
- Handling nulls and missing data
- Ensuring a logical flow with no logical errors in each column
- Standardizing data formats across sources
- Adding technical metadata for audit trails

### 🟡 Gold Layer
This layer takes the data to a different level by modeling the tables and building a solid **Star Schema**. It includes:
- Renaming columns into clear, business-friendly names for end users
- Pre-calculated metrics and aggregations
- Optimized views for common analytical queries
- Ready-to-use tables for BI tools like Power BI, Tableau, or Excel

---

## ⚡ Insights Deep Dive

### 📊 Data Sources

| Source | Table | Brief Description |
|--------|-------|-------------------|
| **CRM** | `cust_info` | Customers' personal info (name, gender, marital status) |
| **CRM** | `prd_info` | Direct products info (name, cost, maintenance) |
| **CRM** | `sales_details` | Transactional table where all orders are recorded (Fact Table) |
| **ERP** | `cust_az12` | Customers' personal info (birth date, gender) |
| **ERP** | `loc_a101` | Customers' geographical info (country, location) |
| **ERP** | `px_cat_g1v2` | Products high-level details (categories, subcategories) |

**[View Source Files](https://github.com/Salah-Ah/sql-data-warehouse-project/tree/main/datasets)**

---

### 🔄 Transformations (Scripts)

#### 🟤 Bronze Layer
The Bronze layer has **no transformations**. Data is imported directly from the sources into the database with schemas and tables ready to deliver the data as-is. This preserves the original state for audit and lineage purposes.

**[View Bronze Scripts](https://github.com/Salah-Ah/sql-data-warehouse-project/tree/main/scripts/Bronze_Layer)**

---

#### ⚪ Silver Layer

Transformations in this layer focus on **data quality and preparation**:

**Stored Procedures** import data from the Bronze layer and perform critical data cleaning:

1. **Handling null values** - Replacing or flagging nulls based on business rules
2. **Handling missing values** - Imputation or exclusion strategies
3. **Fixing logical errors** - Correcting issues in Sales and Date records (e.g., negative quantities, future dates)
4. **Removing duplicates** - Ensuring unique records based on business keys
5. **Following data retention rules** - Removing historical product data to work only with current existing info
6. **Adding metadata columns** - `dwh_create_date`, `dwh_update_date` to record when each record was inserted/updated for technical users
7. **Standardizing values** - Ensuring consistent formatting across different tables and sources (e.g., date formats, text casing)

**[View Silver Transformation Scripts](https://github.com/Salah-Ah/sql-data-warehouse-project/tree/main/scripts/Silver%20Layer)**

---

#### 🟡 Gold Layer

This layer takes cleaned data and makes it **analytics-ready**. The main transformations include:

**Built Views** for each analytical use case to:

1. **Join fact and dimension tables** - Pre-joining tables to eliminate repetitive query logic
2. **Create calculated columns** - Business metrics like profit margin, customer lifetime value
3. **Add aggregation columns** - Summaries at different granularities (daily, monthly, yearly)
4. **Rename columns** - Business-friendly naming conventions (e.g., `prd_name` → `product_name`)

**Benefits**:
1. Well-organized and cleaned views ready for consumption
2. Reduced redundant steps - no need to repeatedly join the same tables
3. All needed columns with built-in calculations and aggregations available to end users
4. Optimized query performance through indexed views

**[View Gold Views & Scripts](https://github.com/Salah-Ah/sql-data-warehouse-project/tree/main/scripts/Gold_Layer)**

---

### ✅ Quality Checks (Tests)

For each data import from one layer to another, the process is prone to technical, logical, or data quality errors. To catch and fix issues immediately, **quality checks** are performed for Silver and Gold layers.

**Test Categories**:
- **Schema Tests**: Validate table structures, data types, and constraints
- **Data Quality Tests**: Check for nulls, duplicates, outliers, and referential integrity
- **Business Logic Tests**: Validate calculations, aggregations, and derived columns
- **Performance Tests**: Ensure queries execute within acceptable time limits

**[View Test Scripts](https://github.com/Salah-Ah/sql-data-warehouse-project/tree/main/test)**

---

## 💫 About Me

Hi there! I'm **Salah Ahmed Mohamed Mohamed**, a **Data Analyst | Aspiring Data Engineer** passionate about transforming raw data into meaningful insights. 

I specialize in building end-to-end data solutions using **SQL**, **Power BI**, **Excel**, and **statistical analysis**. My technical expertise spans advanced topics like DAX, M Code, data modeling, ETL pipelines, stored procedures, and data warehouse design. I'm particularly drawn to projects in **marketing**, **real estate**, and **supply chain** domains, where data-driven decisions create tangible business impact.

I believe in precision, continuous learning, and delivering work that bridges the gap between technical complexity and business value. Currently, I'm expanding my portfolio with diverse projects in data analytics and engineering—because the best way to learn is by building.

---

### 🔗 Let's Connect!

I'm always excited to connect with fellow data enthusiasts, recruiters, and professionals. Feel free to reach out!

- 💼 **LinkedIn**: [linkedin.com/in/salah-ahmed-](https://www.linkedin.com/in/salah-ahmed-/)
- 💻 **GitHub**: [github.com/Salah-Ah](https://github.com/Salah-Ah)
- 📧 **Email**: [salahahmedofficial99@gmail.com](mailto:salahahmedofficial99@gmail.com)
- 🌐 **Portfolio**: [insights-by-salah.lovable.app](https://insights-by-salah.lovable.app/)

---

<div align="center">

**⭐ If you find this project helpful, please consider giving it a star! ⭐**

![Profile Picture](https://i.postimg.cc/dtXVJ8Qj/Recent_001.jpg)

*Built with passion and precision by Salah Ahmed*

</div>
