from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "google_fit_postgres"
GOOGLE_FIT_CONN_ID = "google_fit_api"


@dag(
    dag_id="google_fit_sync",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["google_fit", "medallion"],
    description="Extract → Transform → Load Google Fit pipeline.",
)
def google_fit_sync():
    @task
    def extract(**context) -> dict:
        from google_fit.api.client import fetch_fitness_aggregate
        from google_fit.auth import get_access_token
        from google_fit.db.loader import ensure_schemas, save_bronze

        logical_date = context["logical_date"]
        start = logical_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        ensure_schemas(hook)

        payload = fetch_fitness_aggregate(get_access_token(GOOGLE_FIT_CONN_ID), start, end)
        save_bronze(hook, start.date(), payload)
        return payload

    @task
    def transform(payload: dict) -> dict:
        from google_fit.api.client import parse_aggregate_to_silver

        return parse_aggregate_to_silver(payload)

    @task
    def load(records: dict) -> int:
        from google_fit.db.loader import build_gold, save_silver

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        save_silver(hook, records)
        return build_gold(hook)

    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    extracted >> transformed >> loaded


google_fit_sync()
