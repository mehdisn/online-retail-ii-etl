from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="retail_backfill",
    schedule_interval=None,
    start_date=datetime(2020,1,1),
    catchup=False,
) as dag:
    backfill = BashOperator(
        task_id="spark_backfill",
        bash_command=(
            "spark-submit /opt/spark/jobs/transform_conform_to_clickhouse.py "
            "--start 2010-01-01 --end 2011-12-31 --mode backfill"
        ),
    )
