FROM apache/airflow:2.9.2-python3.10
USER root
RUN apt-get update && apt-get install -y build-essential default-jre && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --retries 10 --timeout 60 -r /requirements.txt
COPY dags /opt/airflow/dags
#COPY ../quality /opt/airflow/quality
