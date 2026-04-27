\# Azure Data Engineering Pipeline (End-to-End)



\## 📌 Overview



Built an end-to-end data engineering pipeline on Azure to ingest, process, and model data using Medallion Architecture. The pipeline transforms raw data into analytics-ready datasets using scalable cloud services.



\## ⚙️ Tech Stack



\* Azure Data Factory (ADF)

\* Azure Databricks (PySpark)

\* Azure Data Lake Storage (ADLS Gen2)

\* Azure SQL Database

\* Delta Lake



\## 🏗️ Architecture



Data is ingested from Azure SQL Database using ADF and stored in the Bronze layer. It is then transformed and cleaned in Databricks (Silver layer), and finally modeled into a Gold layer using a Star Schema (fact and dimension tables) for analytics.



\## 🔄 Orchestration



\* ADF used for data ingestion (SQL → Bronze)

\* Databricks Workflows used to orchestrate transformation layers (Silver → Gold)



\## 🔑 Key Features



\* Incremental data loading using watermark logic

\* Medallion Architecture (Bronze → Silver → Gold)

\* Data transformation using PySpark

\* Dimensional modeling using Star Schema (Fact \& Dimension tables)

\* Implemented SCD Type 1 for dimension tables (overwrite-based updates)

\* Workflow orchestration using Databricks Workflows



\## 📂 Project Structure



```

/adf/                → ADF pipeline JSON

/databricks/        → PySpark notebooks (Silver \& Gold layers)

/screenshots/       → Pipeline, workflow, and output visuals

```



\## 🚀 Data Flow



1\. Data ingested from Azure SQL Database using ADF

2\. Stored in Bronze layer (raw data in ADLS)

3\. Cleaned and transformed in Silver layer using Databricks

4\. Modeled into Gold layer with fact and dimension tables

5\. Stored as Delta tables for analytics



\## 📊 Output



Final curated datasets are stored in the Gold layer as Delta tables, optimized for reporting and downstream analytics.



\## 💡 Key Learnings



\* Designing scalable data pipelines using Azure services

\* Implementing incremental data loading strategies

\* Applying dimensional modeling (Star Schema)

\* Building and orchestrating workflows in Databricks

\* Handling real-world cloud constraints (quota, compute, permissions)







