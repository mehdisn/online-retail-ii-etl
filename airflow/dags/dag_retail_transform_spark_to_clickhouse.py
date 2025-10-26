from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="retail_transform_spark_to_clickhouse",
    schedule_interval="@daily",
    start_date=datetime(2020, 1, 1),
    catchup=False,
    default_args={"retries": 1},
    tags=["retail", "transform"],
) as dag:
    transform = BashOperator(
        task_id="spark_transform",
        # Pass data interval dates to the job
        bash_command=(
            "spark-submit --master local[4] /opt/spark/jobs/transform_conform_to_clickhouse.py "
            "--start '{{ data_interval_start }}' "
            "--end '{{ data_interval_end }}' "
            "--mode incremental" # Use 'incremental' mode
        ),
    )
