# Placeholder for Great Expectations hooks
# Example for quality/dq_runner.py
import sys
from great_expectations.dataset import SparkDFDataset
from spark.jobs.common_spark import get_spark # Assumes common_spark is importable
import os

# Database connection info from env
pg_url = "jdbc:postgresql://{}:{}/{}".format(
    os.getenv("POSTGRES_HOST","postgres"), 
    os.getenv("POSTGRES_PORT","5432"), 
    os.getenv("POSTGRES_DB","retail")
)
pg_props = {
    "user": os.getenv("POSTGRES_USER","retail"),
    "password": os.getenv("POSTGRES_PASSWORD","retail"),
    "driver": "org.postgresql.Driver",
}

spark = get_spark("retail_dq_check")

# 1. Read the data that was just ingested
df = (
    spark.read.format("jdbc")
    .option("url", pg_url)
    .options(**pg_props)
    .option("dbtable", "stage.online_retail_ii")
    .load()
)

# 2. Wrap DataFrame in a GE Dataset
ge_df = SparkDFDataset(df)

# 3. Load the Expectation Suite
# (Path assumes Spark job is run from /opt/spark/ as in docker-compose.yml)
suite = ge_df.get_expectation_suite(
    discard_failed_expectations=False,
    expectation_suite="/opt/quality/expectations/retail_expectations.json"
)

# 4. Validate
validation_results = ge_df.validate(expectation_suite=suite)

# 5. Act on results
if not validation_results["success"]:
    print("Data Quality Check FAILED:")
    print(validation_results)
    sys.exit(1) # Fail the job
else:
    print("Data Quality Check PASSED")

spark.stop()