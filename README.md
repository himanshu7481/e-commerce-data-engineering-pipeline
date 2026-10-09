# E-Commerce Data Engineering Pipeline

An end-to-end data engineering project using PySpark, Apache Airflow, and AWS S3 to build a reliable ETL pipeline for e-commerce customer, product, and order data.

## Architecture & Data Flow

Raw CSV Files (AWS S3)
        |
        v
PySpark: Cleaning & Validation
        |
        +----> Invalid Records (Quarantine)
        |
        v
Processed Data (Parquet)
        |
        v
PySpark: Deduplication, Joins & Aggregations
        |
        v
Curated Analytics Data (AWS S3)

Apache Airflow → Orchestrates the pipeline


## Pipeline Stages

**1. Raw to Processed**

* Reads customer, product, and order CSV files from S3.
* Cleans data, validates records, and standardizes values.
* Separates valid and invalid records.
* Stores processed datasets in Parquet format and invalid records in quarantine locations.

**2. Processed to Curated**

* Removes duplicate records.
* Validates referential integrity between customers, products, and orders.
* Handles orphan orders referencing missing customers or products.
* Joins datasets and calculates business metrics.
* Produces analytics-ready fact, dimension, and summary datasets.

**3. Workflow Orchestration**

* Uses Apache Airflow to run PySpark jobs in sequence.
* Manages task dependencies and ensures the curated stage runs after successful processing.

## Curated Outputs (AWS S3)

| Dataset            | Purpose                           |
| ------------------ | --------------------------------- |
| `fact_orders`      | Order-level transactional data    |
| `dim_customers`    | Clean customer dimension          |
| `dim_products`     | Clean product dimension           |
| `customer_metrics` | Customer-level analytical metrics |
| `product_metrics`  | Product-level analytical metrics  |
| `sales_summary`    | Aggregated sales insights         |

## Technologies Used

* **Python & PySpark** – Data processing, cleaning, joins, and aggregations
* **Apache Airflow** – Pipeline orchestration and task dependencies
* **AWS S3** – Storage for raw, processed, quarantine, and curated data
* **Parquet** – Efficient columnar storage for processed datasets

## Key Features

* Data quality validation and invalid-record quarantine
* Duplicate detection and removal
* Referential integrity checks
* Multi-stage ETL processing
* Analytics-ready fact and dimension tables
* Automated workflow orchestration with Airflow

## Execution

Run the PySpark jobs individually using `spark-submit` or trigger the complete workflow through the Apache Airflow DAG.

**Outcome:** A structured, quality-checked e-commerce data pipeline that transforms raw CSV files into curated datasets ready for downstream analytics and reporting.
