# Weekly Active Users (WAU) Pipelines — ETL vs ELT (Airflow)

This repository contains two Apache Airflow DAGs that compute Weekly Active Users (WAU) using two different pipeline patterns:

- **ETL**: Extract → Transform → Load (transform happens before loading into the warehouse)
- **ELT**: Extract → Load → Transform (transform happens inside the warehouse using SQL)

## Files
- `dag_etl_wau.py` — ETL-style WAU pipeline
- `dag_elt_wau.py` — ELT-style WAU pipeline

## WAU Definition
WAU is calculated as the **count of distinct active users** in a 7-day window, typically aggregated at a weekly grain.

## When to use which
### ETL
- Transformations are performed in Python or a processing layer before loading
- Useful when the source requires heavy parsing/cleaning prior to warehousing

### ELT
- Raw data is loaded first, then transformed using warehouse compute (SQL)
- Scales well with modern cloud warehouses (e.g., Snowflake)

## Tech
- Python
- Apache Airflow
- SQL / Data Warehouse (Snowflake or Postgres-style)
