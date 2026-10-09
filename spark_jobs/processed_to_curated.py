import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when


# ============================================================
# 1. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("ECommerceTransformation")
    .getOrCreate()
)


# ============================================================
# 2. S3 Bucket Configuration
# ============================================================

# Bucket names are supplied through environment variables.
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
CURATED_BUCKET = os.environ["CURATED_BUCKET"]

# Processed input datasets
CUSTOMERS_INPUT_PATH = f"s3a://{PROCESSED_BUCKET}/customers/"
PRODUCTS_INPUT_PATH = f"s3a://{PROCESSED_BUCKET}/products/"
ORDERS_INPUT_PATH = f"s3a://{PROCESSED_BUCKET}/orders/"

# Quarantine outputs
ORPHAN_CUSTOMERS_PATH = (
    f"s3a://{PROCESSED_BUCKET}/quarantine/orphan_customers/"
)
ORPHAN_PRODUCTS_PATH = (
    f"s3a://{PROCESSED_BUCKET}/quarantine/orphan_products/"
)

# Curated outputs
CUSTOMER_METRICS_PATH = f"s3a://{CURATED_BUCKET}/customer_metrics/"
PRODUCT_METRICS_PATH = f"s3a://{CURATED_BUCKET}/product_metrics/"
SALES_SUMMARY_PATH = f"s3a://{CURATED_BUCKET}/sales_summary/"
DIM_CUSTOMERS_PATH = f"s3a://{CURATED_BUCKET}/dim_customers/"
DIM_PRODUCTS_PATH = f"s3a://{CURATED_BUCKET}/dim_products/"
FACT_ORDERS_PATH = f"s3a://{CURATED_BUCKET}/fact_orders/"


# ============================================================
# 3. Reusable Helper Functions
# ============================================================

def read_parquet(path):
    """Read a Parquet dataset from S3."""
    return spark.read.parquet(path)


def write_parquet(dataframe, path):
    """Write a DataFrame to S3 in Parquet format."""
    dataframe.write.mode("overwrite").parquet(path)


def get_duplicate_summary(dataframe, dataset_name, id_column):
    """Summarize duplicate business keys before or after deduplication."""

    total_count = dataframe.count()

    distinct_id_count = (
        dataframe.agg(F.countDistinct(col(id_column)))
        .first()[0]
    )

    duplicate_count = (
        dataframe.groupBy(id_column)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    return spark.createDataFrame(
        [(dataset_name, total_count, distinct_id_count, duplicate_count)],
        [
            "dataset",
            "total_count",
            "distinct_id_count",
            "duplicate_id_count",
        ],
    )


# ============================================================
# 4. Read Processed Datasets
# ============================================================

customers_df = read_parquet(CUSTOMERS_INPUT_PATH)
products_df = read_parquet(PRODUCTS_INPUT_PATH)
orders_df = read_parquet(ORDERS_INPUT_PATH)

print("Processed datasets loaded successfully.")


# ============================================================
# 5. Standardize Data Types
# ============================================================

# Customers
customers_df = (
    customers_df
    .withColumn(
        "lifetime_value_cleaned",
        col("lifetime_value_cleaned").cast("decimal(12,2)"),
    )
    .withColumn(
        "signup_date_cleaned",
        F.to_date(col("signup_date_cleaned")),
    )
)

# Products
products_df = (
    products_df
    .withColumn(
        "price_cleaned",
        col("price_cleaned").cast("decimal(12,2)"),
    )
    .withColumn(
        "stock_qty",
        col("stock_qty").cast("int"),
    )
)

# Orders
orders_df = (
    orders_df
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn(
        "order_date_cleaned",
        F.to_date(col("order_date_cleaned")),
    )
)


# ============================================================
# 6. Duplicate Analysis - Before Deduplication
# ============================================================

duplicates_before_drop = (
    get_duplicate_summary(customers_df, "customers", "customer_id")
    .unionByName(
        get_duplicate_summary(products_df, "products", "product_id")
    )
    .unionByName(
        get_duplicate_summary(orders_df, "orders", "order_id")
    )
)

print("Duplicate summary before deduplication:")
duplicates_before_drop.show(truncate=False)


# ============================================================
# 7. Remove Duplicate Records
# ============================================================

# Keep one record per business key.
# For conflicting duplicates, define a deterministic selection rule.
customers_df = customers_df.dropDuplicates(["customer_id"])
products_df = products_df.dropDuplicates(["product_id"])
orders_df = orders_df.dropDuplicates(["order_id"])


# ============================================================
# 8. Duplicate Analysis - After Deduplication
# ============================================================

duplicates_after_drop = (
    get_duplicate_summary(customers_df, "customers", "customer_id")
    .unionByName(
        get_duplicate_summary(products_df, "products", "product_id")
    )
    .unionByName(
        get_duplicate_summary(orders_df, "orders", "order_id")
    )
)

print("Duplicate summary after deduplication:")
duplicates_after_drop.show(truncate=False)


# ============================================================
# 9. Referential Integrity Validation
# ============================================================

# Find orders referencing missing customers or products.
orphan_customer_orders = orders_df.join(
    customers_df,
    on="customer_id",
    how="left_anti",
)

orphan_product_orders = orders_df.join(
    products_df,
    on="product_id",
    how="left_anti",
)

# Quarantine orphan records for investigation.
write_parquet(orphan_customer_orders, ORPHAN_CUSTOMERS_PATH)
write_parquet(orphan_product_orders, ORPHAN_PRODUCTS_PATH)

# Retain only orders with valid customer and product references.
orders_df = orders_df.join(
    customers_df,
    on="customer_id",
    how="left_semi",
)

orders_df = orders_df.join(
    products_df,
    on="product_id",
    how="left_semi",
)


# ============================================================
# 10. Join Orders, Customers, and Products
# ============================================================

combined_orders = (
    orders_df
    .join(customers_df, on="customer_id", how="left")
    .join(products_df, on="product_id", how="left")
    .select(
        "order_id",
        "customer_id",
        "customer_name",
        "email",
        "country",
        "product_id",
        "product_name",
        "category",
        "price_cleaned",
        "quantity",
        "order_date_cleaned",
        "order_status",
    )
)


# ============================================================
# 11. Business Transformations
# ============================================================

# Calculate the value of each order line.
combined_orders = combined_orders.withColumn(
    "order_amount",
    col("quantity") * col("price_cleaned"),
)

# Separate gross, recognized, and returned sales amounts.
combined_orders = (
    combined_orders
    .withColumn("gross_order_amount", col("order_amount"))
    .withColumn(
        "recognized_sales",
        when(
            col("order_status").isin("Completed", "Shipped"),
            col("order_amount"),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "returned_amount",
        when(
            col("order_status") == "Returned",
            col("order_amount"),
        ).otherwise(F.lit(0)),
    )
)


# ============================================================
# 12. Customer Metrics
# ============================================================

customer_metrics = (
    combined_orders
    .groupBy("customer_id", "customer_name", "country")
    .agg(
        # Counts rows; use countDistinct if orders contain multiple lines.
        F.count("*").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("order_amount").alias("total_spend"),
        F.min("order_date_cleaned").alias("first_order_date"),
        F.max("order_date_cleaned").alias("last_order_date"),
    )
)


# ============================================================
# 13. Product Metrics
# ============================================================

product_metrics = (
    combined_orders
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.sum("quantity").alias("total_units_sold"),
        F.sum("order_amount").alias("total_sales"),
        F.countDistinct("order_id").alias("number_of_orders"),
    )
)


# ============================================================
# 14. Sales Summary
# ============================================================

sales_summary = (
    combined_orders
    .groupBy("order_date_cleaned", "country", "category")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("recognized_sales").alias("total_sales"),
        F.sum(
            when(
                col("order_status").isin("Completed", "Shipped"),
                1,
            ).otherwise(0)
        ).alias("completed_orders"),
        F.sum(
            when(col("order_status") == "Returned", 1).otherwise(0)
        ).alias("returned_orders"),
        F.sum(
            when(col("order_status") == "Cancelled", 1).otherwise(0)
        ).alias("cancelled_orders"),
    )
)


# ============================================================
# 15. Create Fact and Dimension Datasets
# ============================================================

# Fact table: order-level transactional details.
fact_orders = combined_orders.select(
    "order_id",
    "customer_id",
    "product_id",
    "order_date_cleaned",
    "order_status",
    "quantity",
    col("price_cleaned").alias("unit_price"),
    "order_amount",
)

# Customer dimension: descriptive customer attributes.
dim_customers = customers_df.select(
    "customer_id",
    "customer_name",
    "email",
    "country",
    "signup_date_cleaned",
    "lifetime_value_cleaned",
)

# Product dimension: descriptive product attributes.
dim_products = products_df.select(
    "product_id",
    "product_name",
    "category",
    "price_cleaned",
    "stock_qty",
)


# ============================================================
# 16. Write Curated Parquet Datasets
# ============================================================

curated_datasets = {
    "customer_metrics": (customer_metrics, CUSTOMER_METRICS_PATH),
    "product_metrics": (product_metrics, PRODUCT_METRICS_PATH),
    "sales_summary": (sales_summary, SALES_SUMMARY_PATH),
    "dim_customers": (dim_customers, DIM_CUSTOMERS_PATH),
    "dim_products": (dim_products, DIM_PRODUCTS_PATH),
    "fact_orders": (fact_orders, FACT_ORDERS_PATH),
}

for dataset_name, (dataframe, output_path) in curated_datasets.items():
    write_parquet(dataframe, output_path)
    print(f"Successfully wrote {dataset_name}: {output_path}")


# ============================================================
# 17. Stop Spark Session
# ============================================================

print("E-commerce transformation pipeline completed successfully.")

spark.stop()
