include .env

up: ## start stack
	docker compose up -d --build

logs: ## tail airflow
	docker compose logs -f airflow

down: ## stop
	docker compose down -v

seed: ## put dataset into MinIO (expects local CSV at ./data/online_retail_ii.csv)
	docker run --rm -v $$PWD/data:/data --network=$$(docker network ls --filter name=$${COMPOSE_PROJECT_NAME} -q) \
	  minio/mc:latest sh -c "mc alias set minio http://minio:9000 $$MINIO_ROOT_USER $$MINIO_ROOT_PASSWORD && mc cp /data/online_retail_ii.csv minio/$$S3_BUCKET_RAW/online_retail_ii/"

backfill: ## run backfill
	docker compose exec airflow airflow dags trigger retail_backfill -r manual -c '{"start":"2010-01-01","end":"2011-12-31"}'
