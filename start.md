### Требования

Перед запуском должны быть установлены:

- Docker Desktop
- Git
- Опционально: Python 3.12 — только для локального запуска dbt вне контейнеров

---
## 1. Клонировать репозиторий

```
git clone <URL_РЕПОЗИТОРИЯ>
cd mini-data-platform-clickhouse
````
## 2. Поднять базовые сервисы. ClickHouse и MinIO:
```
docker compose up -d clickhouse minio
```
После запуска будут доступны:
- MinIO UI: http://localhost:9001
- ClickHouse HTTP: http://localhost:8123
## 3. Airflow
Инициализация Airflow
````
docker compose --profile airflow up airflow-init
````
Запуск сервисов Airflow
````
docker compose --profile airflow up -d airflow-api-server airflow-scheduler airflow-dag-processor
````
- Airflow UI будет доступен по адресу:
http://localhost:8080

## 4. Metabase
````
docker compose --profile bi up -d metabase-postgres metabase
````
- Metabase будет доступен по адресу:
http://localhost:3000


Параметры подключения к ClickHouse в Metabase:

- Host: clickhouse
- Port: 8123
- Database: warehouse
- Username: ch_user
- Password: ch_pass_123

## 5. Kafka
````
docker compose --profile stream up -d kafka
````
## 6. Создать topic для streaming-событий
````
docker compose --profile stream exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic app_events --partitions 1 --replication-factor 1
````
Проверить список topic:
````
docker compose --profile stream exec kafka kafka-topics --bootstrap-server kafka:29092 --list
````

## 7. Запустить batch-конвейер

После запуска Airflow открыть UI и выполнить DAG:

- check_clickhouse
- dbt_run_staging
- dbt_run_core
- dbt_run_marts
- dbt_run_tests
- validate_marts

Или просто запусти весь DAG olist_pipeline через Trigger.

## 8. Проверить данные в ClickHouse

Подключение к ClickHouse:
````
docker exec -it clickhouse clickhouse-client --user ch_user --password ch_pass_123
````
Примеры проверок:
````
SHOW DATABASES;
SHOW TABLES FROM warehouse;
SELECT count() FROM warehouse.mart_sales_daily;
SELECT count() FROM warehouse.mart_seller_quality;
SELECT count() FROM warehouse.mart_customer_activity;
````
## 9. Проверить realtime-контур
Подключиться к Kafka producer:
````
docker compose --profile stream exec kafka kafka-console-producer --bootstrap-server kafka:29092 --topic app_events
````
Пример события:
````
{"event_time":"2026-04-12 16:05:10","user_id":"u003","event_name":"purchase","product_id":"p300","session_id":"s003","price":219.00}
````

Проверить в ClickHouse:
````
SELECT *
FROM realtime.mart_realtime_funnel_1h;
````
## 10. Остановка проекта

Остановить всё:
````
docker compose --profile airflow --profile bi --profile stream stop
docker compose stop
````
