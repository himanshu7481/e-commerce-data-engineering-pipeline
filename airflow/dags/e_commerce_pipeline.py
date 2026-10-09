from airflow.sdk import dag, task
from datetime import datetime
from pathlib import Path

# ============================================================
# 1. Project Paths
# ============================================================

AIRFLOW_DIR = Path(__file__).resolve().parents[1]
SPARK_JOBS_DIR = AIRFLOW_DIR / "spark_jobs"


# ============================================================
# 2. S3 Bucket Configuration
# ============================================================

RAW_BUCKET = "your-raw-bucket-name"
PROCESSED_BUCKET = "your-processed-bucket-name"
CURATED_BUCKET = "your-curated-bucket-name"


# ============================================================
# 3. E-Commerce Pipeline DAG
# ============================================================

@dag(
    dag_id="e_commerce_pipeline",
    start_date=datetime(2026, 10, 9),
    schedule=None,
    catchup=False,
    tags=["e-commerce", "pyspark", "aws", "s3"],
)
def e_commerce_pipeline():

    # --------------------------------------------------------
    # Task 1: Raw CSV to Processed Parquet
    # --------------------------------------------------------

    @task.bash
    def raw_to_processed():
        script_path = SPARK_JOBS_DIR / "raw_to_processed.py"

        return f"""
        set -e

        export AWS_PROFILE=default
        export RAW_BUCKET="{RAW_BUCKET}"
        export PROCESSED_BUCKET="{PROCESSED_BUCKET}"

        spark-submit \
            --packages org.apache.hadoop:hadoop-aws:3.4.2 \
            --conf spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider \
            "{script_path}"
        """

    # --------------------------------------------------------
    # Task 2: Processed Parquet to Curated Parquet
    # --------------------------------------------------------

    @task.bash
    def processed_to_curated():
        script_path = SPARK_JOBS_DIR / "processed_to_curated.py"

        return f"""
        set -e

        export AWS_PROFILE=default
        export PROCESSED_BUCKET="{PROCESSED_BUCKET}"
        export CURATED_BUCKET="{CURATED_BUCKET}"

        spark-submit \
            --packages org.apache.hadoop:hadoop-aws:3.4.2 \
            --conf spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider \
            "{script_path}"
        """

    # --------------------------------------------------------
    # Task Dependencies
    # --------------------------------------------------------

    raw_task = raw_to_processed()
    curated_task = processed_to_curated()

    # Run the curated transformation only after raw processing succeeds.
    raw_task >> curated_task


e_commerce_pipeline()
