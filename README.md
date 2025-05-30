# Apple-ETL-Data-Pipeline

Project Objective

Business Problem:

Analyze customer and product data (inspired by Apple Inc.) to answer real-world business questions, such as:

1. Which customers purchased AirPods after buying an iPhone?

2. Which customers bought both AirPods and iphone?

3. What is the percentage of customers who bought both AirPods and iPhone? (Inprogress)

4. What is the average time between iPhone and AirPods purchases?(Inprogress)

5. What are the top-selling products in each category?(Inprogress)

Technical Goals:

1. Build an end-to-end ETL pipeline using PySpark.

2. Handle multiple data sources (CSV, Parquet, Delta Table).
   
3. Apply advanced Spark transformations and optimizations.

4. Load processed data into both Data Lake and Lakehouse architectures.

5. Follow modular, production-grade coding practices.

Tools & Technologies
PySpark (Apache Spark with Python)

Databricks Community Edition

Delta Lake

Parquet

Git (for version control)

Jupyter/Databricks Notebooks (for exploration and documentation)



ETL Pipeline Flow
Extract

Data can be read from multiple sources:

CSV files

Parquet files

Delta tables

A Factory Pattern is used to dynamically select the appropriate extractor for each data source type.

Transform

The extracted data is processed and transformed using PySpark (Spark SQL).

Load

The transformed data is then loaded into one or both of the following destinations:

Data Lake

Delta Table

In summary:
CSV / Parquet / Delta Table → [Factory Pattern] → PySpark (Spark SQL) → Data Lake / Delta Table

Stages:

Extract (from various sources using Factory Pattern)

Transform (with PySpark/Spark SQL)

Load (to Data Lake or Delta Table)
