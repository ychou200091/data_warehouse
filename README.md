# Data Warehouse Project

Welcome to this date warehouse project repository. I don't know who is going to read this but here we go.
This project demonstrates a comprehensive data warehousing solution. This project is designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

---
## Data Architecture
The data architecture for this project uses 3 layer Medallion Architecture(**Bronze**, **Silver**, and **Gold**):
1. **Bronze Layer**: This layer stores raw value to the database. Data is ingested from CSV files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization process to prepare data for analysis.
3. **Gold Layer**: This layer houses business ready data modeled into a star schema required for reporting and analytics.
---

## Project Overview 
1. **Data Architecture**: Designed a modern data warehouse using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL pipelines**: Extract, transform, load data from source system into the warehouse.
3. **Data modeling**: Developing fact and dimension table optimized for analytical queries.

Keywords: SQL, Data Architect, Data Engineering, ETL Pipeline Developer,Data Modeling, Data Analytics. Business insight generation.

---
## Important Tools:
- **Datasets:** From  **Baraa Khatib Salkini**  youtube channel, demostration data for the ETL pipeline.
- **SQL Server Express:** Hosting database.
- **SQL Server Management Studio (SSMS):** GUI for managing and interacting with databases.
- **Github:** manage, version control, and collaborate on the code efficiently.
- **DrawIO:** Design diagrams for data architecture, data models, and data flows.
- **Notion:** For project management.

---
## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---
## 📂 Repository Structure
```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                                # Project documentation and architecture details
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_integration.drawio         # Draw.io file shows the tables are related.
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
└── LICENSE                             # License information for the repository
```
---


## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

--- 
# Attribution
Thanks to **Baraa Khatib Salkini** for the project materials and tutorials. Some parts of this project are similar to his content because I followed his demonstrations. To show my work, I have also uploaded my trial-and-error and thought-process files (for example, files whose names end with "process").  
Baraa's YouTube video: https://www.youtube.com/watch?v=9GVqKuTVANE
