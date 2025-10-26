FROM bitnami/spark:3.5.1
USER root
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install pyspark==3.5.1 boto3 pandas pyarrow psycopg2-binary clickhouse-connect great_expectations
USER 1001
COPY jobs /opt/spark/jobs
