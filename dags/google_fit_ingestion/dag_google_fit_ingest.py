from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from google_fit.dag_utils import (
    GOOGLE_FIT_CONN_ID,
    LOOKBACK_DAYS,
    POSTGRES_CONN_ID,
    bronze_run_date,
    lookback_window,
)


@dag(
    dag_id="google_fit_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["google_fit", "bronze"],
    description="Pull Google Fit API data into bronze.fitness_raw.",
)
def google_fit_ingest():
    @task
    def extract_to_bronze(**context) -> int:
        from google_fit.api.client import fetch_fitness_aggregate
        from google_fit.auth import get_access_token
        from google_fit.db.loader import ensure_schemas, save_bronze

        start, end = lookback_window(context)
        run_date = bronze_run_date(context)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        ensure_schemas(hook)

        token = get_access_token(GOOGLE_FIT_CONN_ID)
        payload = fetch_fitness_aggregate(token, start, end)
        bucket_count = len(payload.get("bucket", []))
        last_day = (end - timedelta(days=1)).date()

        print(
            f"Fetching last {LOOKBACK_DAYS} days: {start.date()} → {last_day} (UTC) "
            f"| token={'yes' if token else 'NO — stub data'}"
        )
        print(f"API returned {bucket_count} bucket(s), source={payload.get('source')}")

        if bucket_count == 0:
            print("WARNING: empty bucket — no fitness data for this date range.")

        save_bronze(hook, run_date, payload)
        print(f"Saved bronze.fitness_raw for run_date={run_date}")
        return bucket_count

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="google_fit_transform",
        logical_date="{{ logical_date }}",
        wait_for_completion=False,
    )

    extract_to_bronze() >> trigger_transform


google_fit_ingest()
