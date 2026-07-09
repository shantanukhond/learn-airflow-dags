from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from google_fit.callbacks import alert_on_failure
from google_fit.dag_utils import (
    GOOGLE_FIT_CONN_ID,
    LOOKBACK_DAYS,
    POSTGRES_CONN_ID,
    lookback_days_from_context,
)


def _choose_branch(**context) -> str:
    # --branch: read upstream XCom and return the next task_id to run
    bucket_count = context["ti"].xcom_pull(task_ids="extract_to_bronze")
    if bucket_count == 0:
        return "skip_transform"
    return "trigger_transform"


@dag(  # --dag definition
    dag_id="google_fit_ingest",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",  # --scheduling: daily runs
    catchup=False,
    params={"lookback_days": LOOKBACK_DAYS},  # --params: override per run from Airflow UI
    tags=["google_fit", "bronze"],
    description="Pull Google Fit API data into bronze.fitness_raw.",
)
def google_fit_ingest():
    @task(  # --taskflow: python function becomes an Airflow task
        retries=2,  # --retries: re-run up to 2 times on failure
        retry_delay=timedelta(minutes=2),  # --retry_delay: wait before each retry
        on_failure_callback=alert_on_failure,  # --on_failure_callback: alert when task fails
    )
    def extract_to_bronze(**context) -> int:  # --context: Airflow injects runtime metadata dict
        from google_fit.api.client import fetch_fitness_aggregate
        from google_fit.auth import get_access_token
        from google_fit.db.loader import ensure_schemas, save_bronze

        run_day = context["data_interval_end"].replace(hour=0, minute=0, second=0, microsecond=0)  # --context read: schedule interval end
        lookback_days = lookback_days_from_context(context)  # --params + --variable
        start = run_day - timedelta(days=lookback_days)
        end = run_day

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)  # --hook: uses Airflow Connection
        ensure_schemas(hook)

        token = get_access_token(GOOGLE_FIT_CONN_ID)
        payload = fetch_fitness_aggregate(token, start, end)
        bucket_count = len(payload.get("bucket", []))

        print(
            f"Fetching last {lookback_days} days: {start.date()} → {end.date()} (UTC) "
            f"| token={'yes' if token else 'NO — stub data'}"
        )
        print(f"API returned {bucket_count} bucket(s), source={payload.get('source')}")

        if bucket_count == 0:
            print("WARNING: empty bucket — no fitness data for this date range.")

        save_bronze(hook, start, end, payload)  # --cross-dag handoff: bronze table in Postgres
        print(f"Saved bronze.fitness_raw for start={start.date()} end={end.date()}")
        return bucket_count  # --xcom push: TaskFlow stores return value as XCom

    @task  # --branch path: runs when bucket_count is 0
    def skip_transform() -> None:
        print("Skipping transform — bucket_count is 0, nothing to process.")

    branch_on_bucket_count = BranchPythonOperator(  # --branch operator: pick one downstream path
        task_id="branch_on_bucket_count",
        python_callable=_choose_branch,
    )

    trigger_transform = TriggerDagRunOperator(  # --operator: classic Airflow operator
        task_id="trigger_transform",
        trigger_dag_id="google_fit_transform",  # --cross-dag trigger: start another DAG
        logical_date="{{ logical_date }}",  # --jinja template: pass same run date to child DAG
        conf={"bucket_count": "{{ ti.xcom_pull(task_ids='extract_to_bronze') | int }}"},  # --xcom read + conf: pass small metadata to child DAG run
        wait_for_completion=False,  # fire-and-forget trigger
    )

    extract = extract_to_bronze()
    skip = skip_transform()

    extract >> branch_on_bucket_count >> [trigger_transform, skip]  # --branch: one path runs, other is skipped


google_fit_ingest()
