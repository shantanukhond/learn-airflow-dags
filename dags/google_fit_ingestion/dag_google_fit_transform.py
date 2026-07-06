from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from google_fit.dag_utils import POSTGRES_CONN_ID, bronze_run_date


@dag(
    dag_id="google_fit_transform",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google_fit", "silver", "gold"],
    description="Bronze → silver → gold with explicit task dependencies.",
)
def google_fit_transform():
    @task
    def bronze_to_silver(**context) -> int:
        from google_fit.api.client import parse_aggregate_to_silver
        from google_fit.db.loader import ensure_schemas, load_bronze, save_silver

        run_date = bronze_run_date(context)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        ensure_schemas(hook)

        payload = load_bronze(hook, run_date)
        if not payload:
            print(f"WARNING: no bronze row for run_date={run_date} — run google_fit_ingest first.")
            return 0

        records = parse_aggregate_to_silver(payload)
        steps = records.get("steps", [])
        if steps:
            dates = [r["date"] for r in steps]
            print(f"Parsed {len(steps)} day(s) from bronze: {dates[0]} … {dates[-1]}")
        else:
            print("WARNING: bronze payload produced no silver rows.")

        silver_rows = save_silver(hook, records)
        print(f"Loaded {silver_rows} silver row(s)")
        return silver_rows

    @task
    def silver_to_gold(silver_rows: int) -> int:
        from google_fit.db.loader import build_gold, ensure_schemas

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        ensure_schemas(hook)

        if silver_rows == 0:
            print("WARNING: skipping gold build — no silver rows were written.")

        gold_rows = build_gold(hook)
        print(f"Built {gold_rows} gold row(s) in gold.daily_summary")
        print("Verify: SELECT * FROM gold.daily_summary ORDER BY date DESC; (localhost:5434)")
        return gold_rows

    silver = bronze_to_silver()
    gold = silver_to_gold(silver)

    # Explicit dependency chain (same as TaskFlow args above — shown for learning)
    silver >> gold


google_fit_transform()
