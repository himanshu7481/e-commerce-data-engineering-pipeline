from airflow.sdk import dag, task
from datetime import datetime
from pathlib import Path

AIRFLOW_DIR = Path(__file__).resolve().parents[1]
SPARK_JOBS_DIR = AIRFLOW_DIR / "spark_jobs"

@dag(
    dag_id="e_commerce_pipeline",
    start_date=datetime(2026, 10, 9),
    schedule=None,
    catchup=False,
)
def e_commerce_pipeline():

    @task.bash
    def raw_to_processed():
        script_path = SPARK_JOBS_DIR / "raw_to_processed.py"

        return f"""
        set -e
        export AWS_PROFILE=default

        spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.4.2 \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider \
        "{script_path}"
        """

    @task.bash
    def processed_to_curated():
        script_path = SPARK_JOBS_DIR / "processed_to_curated.py"

        return f"""
        set -e
        export AWS_PROFILE=default

        spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.4.2 \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider \
        "{script_path}"
        """

    raw_task = raw_to_processed()
    curated_task = processed_to_curated()

    raw_task >> curated_task


e_commerce_pipeline()
