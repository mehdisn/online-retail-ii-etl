FROM bitnami/spark:3.5.1
USER root
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install pyspark==3.5.1 boto3 pandas pyarrow psycopg2-binary clickhouse-connect great_expectations

# Add ClickHouse Spark connector compatible with Spark 3.5 and clickhouse-connect 0.7.18
RUN curl -o /opt/spark/jars/clickhouse-spark-3.5-0.7.18-shaded.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-spark-3.5/0.7.18/clickhouse-spark-3.5-0.7.18-shaded.jar

USER 1001
COPY jobs /opt/spark/jobs
