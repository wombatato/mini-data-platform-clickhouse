from __future__ import annotations

from datetime import datetime
from pathlib import Path
import subprocess

from airflow.sdk import dag, task


DBT_PROJECT_DIR = "/opt/airflow/dbt_olist"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profile"


def run_dbt_command(args: list[str]) -> None:
    full_cmd = ["dbt", *args, "--profiles-dir", DBT_PROFILES_DIR]
    result = subprocess.run(
        full_cmd,
        cwd=DBT_PROJECT_DIR,
        text=True,
        capture_output=True,
        check=False,
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"dbt command failed: {' '.join(full_cmd)}")


@dag(
    dag_id="olist_pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["clickhouse", "dbt", "demo"],
)
def olist_pipeline():
    @task()
    def check_clickhouse() -> str:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username="ch_user",
            password="ch_pass_123",
        )

        result = client.query("SELECT count() AS cnt FROM raw.orders_raw")
        count_value = result.result_rows[0][0]
        message = f"ClickHouse is reachable, raw.orders_raw rows = {count_value}"
        print(message)
        return message

    @task()
    def dbt_run_staging() -> str:
        run_dbt_command(["run", "--select", "path:models/staging"])
        return "staging done"

    @task()
    def dbt_run_core() -> str:
        run_dbt_command(["run", "--select", "path:models/core"])
        return "core done"

    @task()
    def dbt_run_marts() -> str:
        run_dbt_command(["run", "--select", "path:models/marts"])
        return "marts done"

    @task()
    def dbt_run_tests() -> str:
        run_dbt_command(["test"])
        return "tests done"

    @task()
    def validate_marts() -> dict:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username="ch_user",
            password="ch_pass_123",
        )

        queries = {
            "mart_sales_daily": "SELECT count() FROM warehouse.mart_sales_daily",
            "mart_seller_quality": "SELECT count() FROM warehouse.mart_seller_quality",
            "mart_customer_activity": "SELECT count() FROM warehouse.mart_customer_activity",
        }

        output = {}
        for name, query in queries.items():
            output[name] = client.query(query).result_rows[0][0]

        print(output)
        return output

    start = check_clickhouse()
    staging = dbt_run_staging()
    core = dbt_run_core()
    marts = dbt_run_marts()
    tests = dbt_run_tests()
    validate = validate_marts()

    start >> staging >> core >> marts >> tests >> validate


olist_pipeline()