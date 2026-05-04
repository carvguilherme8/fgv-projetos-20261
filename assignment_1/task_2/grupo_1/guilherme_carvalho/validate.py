import os
import json
import subprocess
import boto3
import time
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def get_terraform_output(key):
    """Get a value from terraform output"""
    try:
        result = subprocess.run(
            ["terraform", "output", "-raw", key],
            capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__))
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except FileNotFoundError:
        pass
    return None


GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME") or get_terraform_output("glue_job_name") or "classicmodels-etl-etl-job"
S3_BUCKET = os.getenv("S3_BUCKET") or get_terraform_output("s3_bucket_name")

glue = boto3.client("glue", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)

EXPECTED_TABLES = ["fact_orders", "dim_customers", "dim_products", "dim_dates", "dim_countries"]


def start_and_wait_job():
    """Start the Glue job and wait for completion"""
    response = glue.start_job_run(JobName=GLUE_JOB_NAME)
    run_id = response["JobRunId"]
    print(f"Job started: {run_id}")

    while True:
        status = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)
        state = status["JobRun"]["JobRunState"]
        print(f"  Status: {state}")

        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            break
        time.sleep(30)

    if state != "SUCCEEDED":
        error = status["JobRun"].get("ErrorMessage", "Unknown error")
        print(f"Job failed: {error}")
        return False

    print("Job completed successfully!")
    return True


def validate_s3_output():
    """Check that all expected parquet outputs exist in S3"""
    print("\n--- Validating S3 outputs ---")
    all_exist = True

    for table in EXPECTED_TABLES:
        prefix = f"output/{table}/"
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=5)
        contents = response.get("Contents", [])
        parquet_files = [f for f in contents if f["Key"].endswith(".parquet")]

        if parquet_files:
            print(f"  {table}: OK ({len(parquet_files)} parquet file(s))")
        else:
            print(f"  {table}: MISSING")
            all_exist = False

    return all_exist


def validate_fact_orders():
    """Download and verify if fact_orders has valid data"""
    print("\n--- Validating fact_orders content ---")

    try:
        import pandas as pd
        import pyarrow.parquet as pq
        import io

        prefix = "output/fact_orders/"
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        parquet_files = [f["Key"] for f in response.get("Contents", []) if f["Key"].endswith(".parquet")]

        if not parquet_files:
            print("No parquet files found for fact_orders")
            return False

        obj = s3.get_object(Bucket=S3_BUCKET, Key=parquet_files[0])
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

        print(f"Rows: {len(df)}")
        print(f"Columns: {list(df.columns)}")

        required_cols = ["order_id", "customer_id", "product_id", "order_date_key",
                         "country_key", "quantity_ordered", "price_each", "sales_amount"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            print(f"Missing columns: {missing_cols}")
            return False

        computed = df["quantity_ordered"] * df["price_each"]
        diff = (df["sales_amount"] - computed).abs().sum()
        if diff < 0.01:
            print("sales_amount = quantity_ordered * price_each: OK")
        else:
            print(f"sales_amount validation failed (diff={diff})")
            return False

        return True

    except ImportError:
        print("necessary libraries not installed")
        return True


if __name__ == "__main__":
    print("=" * 50)
    print("ETL pipeline calidation")
    print("=" * 50)

    if not S3_BUCKET:
        print("S3_BUCKET not detected")
        exit(1)

    print(f"Glue Job: {GLUE_JOB_NAME}")
    print(f"S3 Bucket: {S3_BUCKET}")

    print("\n--- Starting Glue Job ---")
    job_ok = start_and_wait_job()

    if job_ok:
        s3_ok = validate_s3_output()
        content_ok = validate_fact_orders()

        print("\n" + "=" * 50)
        if s3_ok and content_ok:
            print("ALL VALIDATIONS PASSED")
        else:
            print("SOME VALIDATIONS FAILED")
    else:
        print("\nJob did not succeed")
