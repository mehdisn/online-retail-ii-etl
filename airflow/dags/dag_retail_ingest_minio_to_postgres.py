from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from sensors_minio import MinioKeySensor
from retail_config import S3_BUCKET_RAW

with DAG(
    dag_id="retail_ingest_minio_to_postgres",
    schedule_interval="@daily",
    start_date=datetime(2020, 1, 1),
    catchup=False,
    default_args={"retries": 1},
    tags=["retail", "ingest"],
) as dag:
    wait_for_raw = MinioKeySensor(
        task_id="wait_for_raw",
        bucket=S3_BUCKET_RAW,
        prefix="online_retail_ii/",
        poke_interval=60,
        timeout=60*60,
        mode="reschedule",
    )
run_spark_ingest = BashOperator(
        task_id="spark_ingest",
        bash_command=(
            "spark-submit --master local[4] /opt/spark/jobs/ingest_csv_to_postgres.py "
            "--prefix online_retail_ii/"
        ),
    )

    # New task
    run_spark_dq_check = BashOperator(
        task_id="spark_dq_check",
        bash_command=(
            "spark-submit --master local[4] /opt/quality/dq_runner.py"
        ),
    )

    # Modify the dependency chain
    wait_for_raw >> run_spark_ingest >> run_spark_dq_check

