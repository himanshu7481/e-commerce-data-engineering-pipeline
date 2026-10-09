import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import pyspark.sql.functions as F

# ============================================================
# 1. Spark Session
# ============================================================

spark = SparkSession.builder.appName("ECommerceRawToProcessed").getOrCreate()

# ============================================================
# 2. Input Paths
# ============================================================

RAW_BUCKET = os.environ["RAW_BUCKET"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
customers_input_path = f"s3a://{RAW_BUCKET}/customers/customers.csv"
orders_input_path = f"s3a://{RAW_BUCKET}/orders/orders.csv"
products_input_path = f"s3a://{RAW_BUCKET}/products/products.csv"

# ============================================================
# 3. Read Data from S3
# ============================================================

customers_df = spark.read.csv(
    customers_input_path,
    header=True,
    inferSchema=False
)
products_df = spark.read.csv(
    products_input_path,
    header=True,
    inferSchema=False
)
orders_df = spark.read.csv(
    orders_input_path,
    header=True,
    inferSchema=False
)

# ============================================================
# 4. Customers - Standardization
# ============================================================

customers_df = customers_df \
    .withColumn(
        "customer_id",
        F.upper(F.trim(col("customer_id")))
    ) \
    .withColumn(
        "customer_name",
        F.trim(col("customer_name"))
    ) \
    .withColumn(
        "email",
        F.lower(F.trim(col("email")))
    ) \
    .withColumn(
        "country",
        F.initcap(F.trim(col("country")))
    ) \
    .withColumn(
        "lifetime_value_cleaned",
        F.regexp_replace(
            F.regexp_replace(
                F.trim(col("lifetime_value")),
                r"\$",
                ""
            ),
            ",",
            ""
        ).try_cast("decimal(12,2)")
    ) \
    .withColumn(
        "signup_date_cleaned",
        F.coalesce(
            F.try_to_date(
                F.trim(col("signup_date")),
                "dd/MM/yyyy"
            ),
            F.try_to_date(
                F.trim(col("signup_date")),
                "dd-MM-yyyy"
            )
        )
    )
customers_df = customers_df.withColumn(
    "country",
    F.when(F.upper(F.col("country")).isin("IN", "BHARAT", "INDIA"), "India")
     .when(F.upper(F.col("country")).isin("US", "USA", "U.S.A", "UNITED STATES"), "United States")
     .when(F.upper(F.col("country")).isin("UK", "U.K", "UNITED KINGDOM", "GREAT BRITAIN"), "United Kingdom")
     .when(F.upper(F.col("country")).isin("ES", "SPAIN"), "Spain")
     .otherwise(F.col("country"))
)

# ============================================================
# 5. Products - Standardization
# ============================================================

products_df = products_df \
    .withColumn(
        "product_id",
        F.upper(F.trim(col("product_id")))
    ) \
    .withColumn(
        "product_name",
        F.trim(col("product_name"))
    ) \
    .withColumn(
        "category",
        F.initcap(F.trim(col("category")))
    ) \
    .withColumn(
        "price_cleaned",
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.trim(col("price")),
                    r"\$",
                    ""
                ),
                r",",
                ""
            ),
            r"\s*USD\s*$",
            ""
        ).try_cast("decimal(12,2)")
    ) \
    .withColumn(
        "stock_qty",
        col("stock_qty").try_cast("int")
    )
products_df = products_df.withColumn(
    "category",
    F.when(F.col("category") == "Electronic", "Electronics")
     .when(F.col("category") == "Sport", "Sports")
     .when(F.col("category") == "Clothes", "Clothing")
     .when(F.col("category") == "Book", "Books")
     .when(F.col("category") == "Home", "Home & Kitchen")
     .otherwise(F.col("category"))
)

# ============================================================
# 6. Orders - Standardization
# ============================================================

orders_df = orders_df \
    .withColumn(
        "order_id",
        F.upper(F.trim(col("order_id")))
    ) \
    .withColumn(
        "customer_id",
        F.upper(F.trim(col("customer_id")))
    ) \
    .withColumn(
        "product_id",
        F.upper(F.trim(col("product_id")))
    ) \
    .withColumn(
        "quantity",
        col("quantity").try_cast("int")
    ) \
    .withColumn(
        "order_status",
        F.initcap(F.trim(col("order_status")))
    ) \
    .withColumn(
        "order_date_cleaned",
        F.coalesce(
            F.try_to_date(
                F.trim(col("order_date")),
                "dd/MM/yyyy"
            ),
            F.try_to_date(
                F.trim(col("order_date")),
                "dd-MM-yyyy"
            )
        )
    )
orders_df = orders_df.withColumn(
    "order_status",
    F.when(F.lower(F.col("order_status")) == "canceled", "Cancelled")
     .otherwise(F.col("order_status"))
)

# ============================================================
# 7. CUSTOMERS - DATA QUALITY VALIDATION
# ============================================================

customers_df = customers_df.withColumn(
    "record_status",
    F.lit("Valid")
)
customers_df = customers_df.withColumn(
    "error_reason",
    F.lit(None).try_cast("string")
)
# -------------------------
# Customer ID
# -------------------------
customers_df = customers_df.withColumn(
    "customer_id",
    when(
        col("customer_id").isNull()
        | (col("customer_id") == "")
        | col("customer_id").isin("NA", "N/A", "NULL", "NAN"),
        None
    ).otherwise(col("customer_id"))
)
customers_df = customers_df.withColumn(
    "record_status",
    when(
        col("customer_id").isNull(),
        "Invalid"
    ).when(
        ~col("customer_id").rlike(r"^C\d{4}$"),
        "Invalid"
    ).otherwise(col("record_status"))
)
customers_df = customers_df.withColumn(
    "error_reason",
    when(
        col("customer_id").isNull(),
        "CUSTOMER_ID_MISSING"
    ).when(
        ~col("customer_id").rlike(r"^C\d{4}$"),
        "CUSTOMER_ID_INVALID_FORMAT"
    ).otherwise(col("error_reason"))
)
# -------------------------
# Customer Name
# -------------------------
customers_df = customers_df.withColumn(
    "customer_name",
    when(
        col("customer_name").isNull()
        | (col("customer_name") == "")
        | col("customer_name").isin("NA", "N/A", "NULL", "NAN"),
        None
    ).otherwise(col("customer_name"))
)
customers_df = customers_df.withColumn(
    "record_status",
    when(
        col("customer_name").isNull(),
        "Invalid"
    ).otherwise(col("record_status"))
)
customers_df = customers_df.withColumn(
    "error_reason",
    when(
        col("customer_name").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("CUSTOMER_NAME_MISSING")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Email
# -------------------------
customers_df = customers_df.withColumn(
    "email",
    when(
        col("email").isNull()
        | (col("email") == "")
        | col("email").isin("NA", "N/A", "NULL", "NAN"),
        None
    ).otherwise(col("email"))
)
customers_df = customers_df.withColumn(
    "record_status",
    when(
        col("email").isNull()
        | ~col("email").rlike(
            r"^[A-Za-z0-9.\_%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        ),
        "Invalid"
    ).otherwise(col("record_status"))
)
customers_df = customers_df.withColumn(
    "error_reason",
    when(
        col("email").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("EMAIL_MISSING")
        )
    ).when(
        ~col("email").rlike(
            r"^[A-Za-z0-9.\_%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        ),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("EMAIL_INCORRECT_FORMAT")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Country
# -------------------------
customers_df = customers_df.withColumn(
    "country",
    when(
        col("country").isNull()
        | (col("country") == "")
        | col("country").isin("NA", "N/A", "NULL", "NAN"),
        None
    ).otherwise(col("country"))
)
customers_df = customers_df.withColumn(
    "record_status",
    when(
        col("country").isNull(),
        "Invalid"
    ).otherwise(col("record_status"))
)
customers_df = customers_df.withColumn(
    "error_reason",
    when(
        col("country").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("COUNTRY_MISSING")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Lifetime Value
# -------------------------
customers_df = customers_df.withColumn(
    "record_status",
    when(
        col("lifetime_value_cleaned").isNull()
        | (col("lifetime_value_cleaned") < 0),
        "Invalid"
    ).otherwise(col("record_status"))
)
customers_df = customers_df.withColumn(
    "error_reason",
    when(
        col("lifetime_value_cleaned").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_LIFETIME")
        )
    ).when(
        col("lifetime_value_cleaned") < 0,
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("NEGATIVE_LIFETIME")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Signup Date
# -------------------------
customers_df = customers_df.withColumn(
    "record_status",
    when(
        col("signup_date_cleaned").isNull(),
        "Invalid"
    ).otherwise(col("record_status"))
)
customers_df = customers_df.withColumn(
    "error_reason",
    when(
        col("signup_date_cleaned").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_OR_INVALID_SIGNUP_DATE")
        )
    ).otherwise(col("error_reason"))
)
# Split Customers
invalid_customers_df = customers_df.filter(
    col("record_status") == "Invalid"
)
valid_customers_df = customers_df.filter(
    col("record_status") == "Valid"
)
# ============================================================
# 8. PRODUCTS - DATA QUALITY VALIDATION
# ============================================================
products_df = products_df.withColumn(
    "record_status",
    F.lit("Valid")
)
products_df = products_df.withColumn(
    "error_reason",
    F.lit(None).try_cast("string")
)
# -------------------------
# Product ID
# -------------------------
products_df = products_df.withColumn(
    "product_id",
    when(
        col("product_id").isNull()
        | (col("product_id") == "")
        | col("product_id").isin("NA", "N/A", "NULL", "NAN"),
        None
    ).otherwise(col("product_id"))
)
products_df = products_df.withColumn(
    "record_status",
    when(
        col("product_id").isNull()
        | ~col("product_id").rlike(r"^P\d{3}$"),
        "Invalid"
    ).otherwise(col("record_status"))
)
products_df = products_df.withColumn(
    "error_reason",
    when(
        col("product_id").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_PRODUCT_ID")
        )
    ).when(
        ~col("product_id").rlike(r"^P\d{3}$"),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("PRODUCT_ID_INCORRECT_FORMAT")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Product Name
# -------------------------
products_df = products_df.withColumn(
    "product_name",
    when(
        col("product_name").isNull()
        | (col("product_name") == "")
        | col("product_name").isin("NA", "N/A", "NULL", "NAN"),
        None
    ).otherwise(col("product_name"))
)
products_df = products_df.withColumn(
    "record_status",
    when(
        col("product_name").isNull(),
        "Invalid"
    ).otherwise(col("record_status"))
)
products_df = products_df.withColumn(
    "error_reason",
    when(
        col("product_name").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_PRODUCT_NAME")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Category
# -------------------------
products_df = products_df.withColumn(
    "category",
    when(
        col("category").isNull()
        | (col("category") == "")
        | col("category").isin("NA", "N/A", "NULL", "NAN"),
        None
    ).otherwise(col("category"))
)
products_df = products_df.withColumn(
    "record_status",
    when(
        col("category").isNull(),
        "Invalid"
    ).otherwise(col("record_status"))
)
products_df = products_df.withColumn(
    "error_reason",
    when(
        col("category").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_CATEGORY")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Price
# -------------------------
products_df = products_df.withColumn(
    "record_status",
    when(
        (col("price_cleaned").isNull()) | (col("price_cleaned") < 0),
        "Invalid"
    ).otherwise(col("record_status"))
)
products_df = products_df.withColumn(
    "error_reason",
    when(
        col("price_cleaned").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_PRICE")
        )
    ).when(
        col("price_cleaned") < 0,
        F.concat_ws(";", col("error_reason"), F.lit("NEGATIVE_PRICE"))
    ).otherwise(col("error_reason"))
)
# -------------------------
# Stock Quantity
# -------------------------
products_df = products_df.withColumn(
    "record_status",
    when(
        (col("stock_qty").isNull()) | ((col("stock_qty") < 0)),
        "Invalid"
    ).otherwise(col("record_status"))
)
products_df = products_df.withColumn(
    "error_reason",
    when(
        col("stock_qty").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_STOCK_QUANTITY")
        )
    ).when(
    col("stock_qty") < 0,
    F.concat_ws(";", col("error_reason"), F.lit("NEGATIVE_STOCK_QUANTITY"))
).otherwise(col("error_reason"))
)
# Split Products
invalid_products_df = products_df.filter(
    col("record_status") == "Invalid"
)
valid_products_df = products_df.filter(
    col("record_status") == "Valid"
)

# ============================================================
# 9. ORDERS - DATA QUALITY VALIDATION
# ============================================================

orders_df = orders_df.withColumn(
    "record_status",
    F.lit("Valid")
)
orders_df = orders_df.withColumn(
    "error_reason",
    F.lit(None).try_cast("string")
)
# -------------------------
# Order ID
# -------------------------
orders_df = orders_df.withColumn(
    "order_id",
    F.when(
        col("order_id").isNull()
        | (col("order_id") == "")
        | col("order_id").isin("NA", "N/A", "NULL", "NAN"),
        None
    ).otherwise(col("order_id"))
)
orders_df = orders_df.withColumn(
    "record_status",
    F.when(
        col("order_id").isNull()
        | ~col("order_id").rlike(r"^O\d{5}$"),
        "Invalid"
    ).otherwise(col("record_status"))
)
orders_df = orders_df.withColumn(
    "error_reason",
    F.when(
        col("order_id").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_ORDER_ID")
        )
    ).when(
        ~col("order_id").rlike(r"^O\d{5}$"),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("ORDER_ID_INCORRECT_FORMAT")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Customer ID
# -------------------------
orders_df = orders_df.withColumn(
    "customer_id",
    F.when(
        col("customer_id").isNull()
        | (col("customer_id") == "")
        | col("customer_id").isin("NA", "N/A", "NULL", "NAN"),
        None
    ).otherwise(col("customer_id"))
)
orders_df = orders_df.withColumn(
    "record_status",
    F.when(
        col("customer_id").isNull()
        | ~col("customer_id").rlike(r"^C\d{4}$"),
        "Invalid"
    ).otherwise(col("record_status"))
)
orders_df = orders_df.withColumn(
    "error_reason",
    F.when(
        col("customer_id").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_CUSTOMER_ID")
        )
    ).when(
        ~col("customer_id").rlike(r"^C\d{4}$"),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("CUSTOMER_ID_INCORRECT_FORMAT")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Product ID
# -------------------------
orders_df = orders_df.withColumn(
    "product_id",
    F.when(
        col("product_id").isNull()
        | (col("product_id") == "")
        | col("product_id").isin("NA", "N/A", "NULL", "NAN"),
        None
    ).otherwise(col("product_id"))
)
orders_df = orders_df.withColumn(
    "record_status",
    F.when(
        col("product_id").isNull()
        | ~col("product_id").rlike(r"^P\d{3}$"),
        "Invalid"
    ).otherwise(col("record_status"))
)
orders_df = orders_df.withColumn(
    "error_reason",
    F.when(
        col("product_id").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_PRODUCT_ID")
        )
    ).when(
        ~col("product_id").rlike(r"^P\d{3}$"),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("PRODUCT_ID_INCORRECT_FORMAT")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Quantity
# -------------------------
orders_df = orders_df.withColumn(
    "record_status",
    F.when(
        col("quantity").isNull()
        | (col("quantity") < 1),
        "Invalid"
    ).otherwise(col("record_status"))
)
orders_df = orders_df.withColumn(
    "error_reason",
    F.when(
        col("quantity").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_QUANTITY")
        )
    ).when(
        col("quantity") < 1,
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("INVALID_QUANTITY")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Order Date
# -------------------------
orders_df = orders_df.withColumn(
    "record_status",
    F.when(
        col("order_date_cleaned").isNull(),
        "Invalid"
    ).otherwise(col("record_status"))
)
orders_df = orders_df.withColumn(
    "error_reason",
    F.when(
        col("order_date_cleaned").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_OR_INVALID_ORDER_DATE")
        )
    ).otherwise(col("error_reason"))
)
# -------------------------
# Order Status
# -------------------------
orders_df = orders_df.withColumn(
    "order_status",
    F.when(
        col("order_status").isNull()
        | (col("order_status") == "")
        | col("order_status").isin("NA", "N/A", "NULL", "NAN"),
        None
    ).otherwise(col("order_status"))
)
orders_df = orders_df.withColumn(
    "record_status",
    F.when(
        col("order_status").isNull()
        | ~col("order_status").isin(
            "Shipped",
            "Completed",
            "Returned",
            "Cancelled",
            "Pending"
        ),
        "Invalid"
    ).otherwise(col("record_status"))
)
orders_df = orders_df.withColumn(
    "error_reason",
    F.when(
        col("order_status").isNull(),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("MISSING_ORDER_STATUS")
        )
    ).when(
        ~col("order_status").isin(
            "Shipped",
            "Completed",
            "Returned",
            "Cancelled",
            "Pending"
        ),
        F.concat_ws(
            ";",
            col("error_reason"),
            F.lit("INVALID_ORDER_STATUS")
        )
    ).otherwise(col("error_reason"))
)

# ============================================================
# 10. Split Valid / Invalid Orders
# ============================================================

valid_orders_df = orders_df.filter(
    col("record_status") == "Valid"
)
invalid_orders_df = orders_df.filter(
    col("record_status") == "Invalid"
)
total_customers_cnt=customers_df.count()
valid_customers_cnt=valid_customers_df.count()
invalid_customers_cnt=invalid_customers_df.count()
invalid_customers_perctn = (invalid_customers_cnt * 100.0 / total_customers_cnt) if total_customers_cnt else 0.0
total_products_cnt=products_df.count()
invalid_products_cnt=invalid_products_df.count()
valid_products_cnt=valid_products_df.count()
invalid_products_perctn = (invalid_products_cnt * 100.0 / total_products_cnt) if total_products_cnt else 0.0
total_orders_cnt=orders_df.count()
valid_orders_cnt=valid_orders_df.count()
invalid_orders_cnt=invalid_orders_df.count()
invalid_orders_perctn = (invalid_orders_cnt * 100.0 / total_orders_cnt) if total_orders_cnt else 0.0
customer_summary_df=spark.createDataFrame([("customers",total_customers_cnt,valid_customers_cnt,invalid_customers_cnt,invalid_customers_perctn)],["Dataset","Total_count","Valid_count","Invalid_count","Invalid_percentage"])
products_summary_df=spark.createDataFrame([("products",total_products_cnt,valid_products_cnt,invalid_products_cnt,invalid_products_perctn)],["Dataset","Total_count","Valid_count","Invalid_count","Invalid_percentage"])
orders_summary_df=spark.createDataFrame([("orders",total_orders_cnt,valid_orders_cnt,invalid_orders_cnt,invalid_orders_perctn)],["Dataset","Total_count","Valid_count","Invalid_count","Invalid_percentage"])
validation_summary_df=customer_summary_df.union(products_summary_df).union(orders_summary_df)
validation_summary_df.show(truncate=False)

# ============================================================
# 11. Configurable Parquet Output Paths
# ============================================================

output_paths = {
    "valid_customers": f"s3a://{PROCESSED_BUCKET}/customers/",
    "invalid_customers": f"s3a://{PROCESSED_BUCKET}/quarantine/customers/",
    "valid_products": f"s3a://{PROCESSED_BUCKET}/products/",
    "invalid_products": f"s3a://{PROCESSED_BUCKET}/quarantine/products/",
    "valid_orders": f"s3a://{PROCESSED_BUCKET}/orders/",
    "invalid_orders": f"s3a://{PROCESSED_BUCKET}/quarantine/orders/",
    "validation_summary": f"s3a://{PROCESSED_BUCKET}/validation-summary/",
}

# ============================================================
# 12. Write DataFrames as Parquet
# ============================================================

dataframes = {
    "valid_customers": valid_customers_df,
    "invalid_customers": invalid_customers_df,
    "valid_products": valid_products_df,
    "invalid_products": invalid_products_df,
    "valid_orders": valid_orders_df,
    "invalid_orders": invalid_orders_df,
    "validation_summary": validation_summary_df,
}
for name, dataframe in dataframes.items():
    output_path = output_paths[name]
    dataframe.write.mode("overwrite").parquet(output_path)
    print(f"Successfully wrote {name} to {output_path}")
print("Raw-to-processed pipeline completed successfully.")
spark.stop()
