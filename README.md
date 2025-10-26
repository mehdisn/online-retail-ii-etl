
# Online Retail II – Production-Ready ETL

End-to-end, containerized data platform for the UCI **Online Retail II** dataset.  
Built with **PostgreSQL, Airflow, PySpark, ClickHouse, MinIO, Docker**, and ready for **Metabase** dashboards or ML pipelines.

---

## ✨ Features
- Raw data lands in **MinIO (S3)** buckets
- **Airflow DAGs** orchestrate ingestion and transformation
- **PySpark** handles data validation and transformations
- **PostgreSQL** serves as staging database
- **ClickHouse** serves as analytics warehouse (star schema)
- **Metabase** for BI dashboards
- **MinIO Parquet** outputs for ML training
- **Docker Compose** for local reproducibility
- Data Quality checks and unit tests included

---

## 🏗️ Architecture

1. **Raw landing** → MinIO (`retail-raw/online_retail_ii/*.csv`)
2. **Ingest** → Spark job loads into PostgreSQL staging (`stage.online_retail_ii`)
3. **Transform** → Spark builds star schema → ClickHouse (`dim_*`, `fact_sales`)
4. **Serve** → Metabase connects to ClickHouse; Spark writes parquet features to MinIO (`retail-features`)
5. **Ops** → Airflow DAGs manage orchestration, backfills, and quality checks

---

## 📂 Repo Layout

```
online-retail-ii-etl/
├─ docker-compose.yml
├─ .env.example
├─ Makefile
├─ README.md
├─ airflow/          # Airflow Docker image & DAGs
├─ spark/            # Spark jobs for ETL + ML features
├─ postgres/         # Staging DB init scripts
├─ clickhouse/       # Warehouse DDLs
├─ minio/            # Bucket bootstrap script
├─ quality/          # Data Quality expectations
├─ tests/            # Pytest tests for jobs & schemas
└─ notebooks/        # EDA and ML starter notebook
```

---

## 🚀 Quickstart

### 1. Clone & Configure
```bash
git clone https://github.com/your-org/online-retail-ii-etl.git
cd online-retail-ii-etl
cp .env.example .env
```

### 2. Start the stack
```bash
make up
```

### 3. Seed Data
Download the UCI Online Retail II CSV and place under `./data/`. Then run:
```bash
make seed
```

### 4. Access Services
- **Airflow** → http://localhost:8080  (admin/admin)
- **MinIO Console** → http://localhost:9001  (admin/adminadmin)
- **Metabase** → http://localhost:3000

---

## 🗄️ Warehouse Schema (ClickHouse)

- **dim_customer(CustomerID, customer_country)**
- **dim_product(StockCode, product_desc)**
- **dim_date(date)**
- **fact_sales(InvoiceNo, CustomerID, StockCode, date, Quantity, UnitPrice, amount)**  
  Partitioned monthly by `date`.

Materialized View:
- **mv_daily_revenue(date, revenue)**

---

## 🧪 Tests
Run unit tests:
```bash
pytest tests/
```

---

## 🔒 Production Notes
- Switch Airflow executor to Celery/K8s for scale
- Use external object storage (AWS S3, GCS, etc.)
- Secure secrets with Vault/SSM (don’t commit `.env`)
- Monitor Airflow, Spark, and ClickHouse with Prometheus
- Expand Data Quality checks (Great Expectations)

---

## 📊 Next Steps
- Build Metabase dashboards (Revenue trends, RFM segments, Cohorts)
- Generate ML features via Spark jobs (stored in MinIO Parquet)
- Extend schema with SCDs, embeddings, or vector search in ClickHouse

---

## 📖 Dataset License
The **Online Retail II** dataset is provided by UCI ML Repository. Review license/terms before production use.

---

## 🙌 Acknowledgements
- Apache Airflow, Apache Spark, PostgreSQL, ClickHouse, MinIO, Docker, Metabase
