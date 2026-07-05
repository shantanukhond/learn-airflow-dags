from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "google_fit_postgres"
GOOGLE_FIT_CONN_ID = "google_fit_api"
LOOKBACK_DAYS = 7


def _lookback_window(context: dict, days: int = LOOKBACK_DAYS) -> tuple[datetime, datetime]:
    """Last N complete UTC days ending yesterday (excludes today)."""
    ref = context["data_interval_end"] or context["data_interval_start"]
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    else:
        ref = ref.astimezone(timezone.utc)

    run_day = ref.replace(hour=0, minute=0, second=0, microsecond=0)
    end = run_day
    start = run_day - timedelta(days=days)
    return start, end


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

        start, end = _lookback_window(context)
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
            print("WARNING: empty bucket — no fitness data for this date range in Google Fit.")

        save_bronze(hook, last_day, payload)
        return payload

    @task
    def transform(payload: dict) -> dict:
        from google_fit.api.client import parse_aggregate_to_silver

        records = parse_aggregate_to_silver(payload)
        steps = records.get("steps", [])
        if steps:
            dates = [r["date"] for r in steps]
            print(f"Parsed {len(steps)} day(s): {dates[0]} … {dates[-1]}")
        else:
            print("WARNING: transform produced no rows — load will write nothing to silver/gold.")
        return records

    @task
    def load(records: dict) -> int:
        from google_fit.db.loader import build_gold, save_silver

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        silver_rows = save_silver(hook, records)
        gold_rows = build_gold(hook)

        print(f"Loaded {silver_rows} silver row(s), {gold_rows} gold row(s)")
        print("Verify: SELECT * FROM gold.daily_summary; on fitness DB (localhost:5434)")

        if silver_rows == 0:
            print("WARNING: nothing written — check extract logs for empty API response.")

        return gold_rows

    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    extracted >> transformed >> loaded


google_fit_sync()
