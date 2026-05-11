Olist E-Commerce Data Engineering Pipeline
 
An end-to-end data engineering pipeline built on the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). This project simulates a real-world data warehouse pipeline — from raw CSV ingestion to analytics-ready views — using industry-standard tools.
 
---
 
## Architecture
 
```
Raw CSVs (Olist Dataset)
        │
        ▼
  PySpark Transform          transform_dim.py  /  transform_fact.py
  (clean, aggregate,         ─────────────────────────────────────
   build date keys)          Writes Parquet staging files
        │
        ▼
  PostgreSQL Loader          loadscript.py
  (upsert dimensions         ─────────────
   + fact table)             Resolves surrogate keys via SQL JOIN
        │
        ▼
  Data Quality Checks        quality_checks.py
  (row counts, nulls,        ─────────────────
   referential integrity)    Exits 1 on failure → Airflow marks task failed
        │
        ▼
  Analytics Views            analytics_views.sql
  (pre-aggregated KPIs       ───────────────────
   for BI consumption)       Refreshed after every successful load
        │
        ▼
  Airflow DAG                pipeline.py
  (orchestrates all          ───────────
   steps on schedule)        Runs daily at 01:00
```
 
---
 
## Star Schema
 
```
                    dim_time
                       │
dim_location ──── dim_customer
      │                │
dim_location ──── dim_seller ──── fact_order ──── dim_product
                                       │
                                   dim_time (approved, delivered)
```
 
| Table | Description |
|---|---|
| `fact_order` | One row per order item — all measures (price, freight, payment, review) |
| `dim_customer` | Customer info linked to location via zip code |
| `dim_seller` | Seller info linked to location via zip code |
| `dim_product` | Product catalog with English category translation |
| `dim_location` | Zip code, city, state, avg lat/lng from geolocation data |
| `dim_time` | Full calendar breakdown from all order timestamps |
 
---
 
## Tech Stack
 
| Tool | Purpose |
|---|---|
| PySpark | Large-scale data transformation |
| PostgreSQL | Data warehouse |
| Apache Airflow | Pipeline orchestration and scheduling |
| Docker Compose | Local infrastructure |
| pandas + psycopg2 | Loading parquet staging files into PostgreSQL |
 
