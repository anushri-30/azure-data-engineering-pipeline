\# 🚀 Azure Data Engineering Pipeline (End-to-End)



\## 📌 Overview



An end-to-end data engineering pipeline built on Azure to ingest, transform, and model data using \*\*Medallion Architecture (Bronze → Silver → Gold)\*\*.

The pipeline processes raw data into analytics-ready datasets using scalable and production-style design patterns.



\---



\## ⚙️ Tech Stack



\* Azure Data Factory (ADF) – Data Ingestion

\* Azure Databricks (PySpark) – Data Processing

\* Azure Data Lake Storage Gen2 (ADLS) – Storage

\* Azure SQL Database – Source System

\* Delta Lake – Optimized Storage Format



\---



\## 🔄 Medallion Architecture Flow



```id="flowdiagram1"

Source (Azure SQL Database)

&#x20;       │

&#x20;       ▼

┌───────────────────────┐

│        BRONZE         │  Raw data ingestion using ADF

│   (ADLS - Parquet)    │  Incremental load (watermark)

└─────────┬─────────────┘

&#x20;         │

&#x20;         ▼

┌───────────────────────┐

│        SILVER         │  Transformed data

│   (ADLS - Parquet)    │  Processed using PySpark

└─────────┬─────────────┘

&#x20;         │

&#x20;         ▼

┌───────────────────────┐

│         GOLD          │  Star Schema (Fact \& Dimensions)

│     (Delta Lake)      │  SCD Type 1 implementation

│                       │  Optimized for analytics

└───────────────────────┘

```



\---



\## 🔄 Orchestration Strategy



\* \*\*ADF\*\* → Handles ingestion (SQL → Bronze)

\* \*\*Databricks Workflows\*\* → Orchestrates Silver \& Gold transformations



\---



\## 🔑 Key Features



\* Incremental data loading using watermark logic

\* Medallion Architecture (Bronze → Silver → Gold)

\* Data transformation using PySpark

\* Dimensional modeling using \*\*Star Schema\*\*

\* Implemented \*\*SCD Type 1\*\* for dimension tables

\* Workflow orchestration using Databricks Workflows



\---



\## 📂 Project Structure



```id="projstruct1"

/adf/                → ADF pipeline JSON  

/databricks/         → PySpark notebooks (Silver \& Gold layers)  

/rawdata/            → Sample source data (2 datasets)  

/screenshots/        → Architecture \& execution visuals  

```



\---



\## 🚀 Data Pipeline Flow



1\. Data ingested from Azure SQL Database using ADF

2\. Stored in Bronze layer (raw data in ADLS)

3\. Transformed in Silver layer using Databricks (PySpark)

4\. Modeled into Gold layer using Star Schema

5\. Stored as Delta tables for analytics and reporting



\---



\## 📊 Output



Final curated datasets are stored as \*\*Delta tables\*\* in the Gold layer, ready for downstream analytics and reporting tools (e.g., Power BI).



\---



\## 💡 Key Learnings



\* Designing scalable data pipelines using Azure services

\* Implementing incremental data loading strategies

\* Applying dimensional modeling (Star Schema)

\* Building and orchestrating workflows in Databricks

\* Handling real-world cloud constraints (quota, compute, permissions)



\---



\## 🧠 Future Improvements



\* Implement SCD Type 2 for historical tracking

\* Add data quality validation checks

\* Integrate Power BI dashboard for visualization

\* Add CI/CD pipeline for deployment



\---



⭐ If you found this project useful, feel free to star the repo!



