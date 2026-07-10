from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook

from google_fit.callbacks import alert_on_failure, alert_on_sla_miss
from google_fit.dag_utils import LOOKBACK_DAYS, POSTGRES_CONN_ID, lookback_days_from_context


@dag(  # --dag definition
    dag_id="google_fit_transform",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # --scheduling: only runs when triggered
    catchup=False,
    params={"lookback_days": LOOKBACK_DAYS},  # --params: override per run from Airflow UI
    sla_miss_callback=alert_on_sla_miss,  # --sla_miss_callback: DAG-level; fires when any task misses SLA
    tags=["google_fit", "silver", "gold"],
    description="Bronze → silver → gold with explicit task dependencies.",
)
def google_fit_transform():
    @task(  # --taskflow: python function becomes an Airflow task
        sla=timedelta(minutes=30),  # --sla: flag task if not done within 30 minutes
        on_failure_callback=alert_on_failure,  # --on_failure_callback: alert when task fails
    )
    def bronze_to_silver(**context) -> int:  # --context: Airflow injects runtime metadata dict
        from google_fit.api.client import parse_aggregate_to_silver
        from google_fit.db.loader import ensure_schemas, save_silver

        run_day = context["data_interval_end"].replace(hour=0, minute=0, second=0, microsecond=0)  # --context read: same interval as parent ingest run
        lookback_days = lookback_days_from_context(context)  # --params + --variable
        start = run_day - timedelta(days=lookback_days)
        end = run_day

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)  # --hook: uses Airflow Connection
        ensure_schemas(hook)

        row = hook.get_first(  # --cross-dag handoff: read bronze written by ingest DAG
            "SELECT payload FROM bronze.fitness_raw WHERE start = %s AND end = %s",
            parameters=(start.date(), end.date()),
        )
        payload = row[0] if row else {}
        if not payload:
            print(
                f"WARNING: no bronze row for start={start.date()} end={end.date()} "
                "— run google_fit_ingest first."
            )
            return 0  # --xcom push: return 0 if no bronze data

        expected_bucket_count = int((context["dag_run"].conf or {}).get("bucket_count", -1))  # --conf read: value sent by ingest trigger
        bronze_bucket_count = len(payload.get("bucket", []))
        if expected_bucket_count >= 0 and bronze_bucket_count != expected_bucket_count:
            print(
                "WARNING: bucket_count mismatch between ingest trigger and bronze payload: "
                f"expected={expected_bucket_count}, actual={bronze_bucket_count}"
            )
        else:
            print(
                "Bucket count check passed: "
                f"expected={expected_bucket_count}, actual={bronze_bucket_count}"
            )

        records = parse_aggregate_to_silver(payload)
        steps = records.get("steps", [])
        if steps:
            dates = [r["date"] for r in steps]
            print(f"Parsed {len(steps)} day(s) from bronze: {dates[0]} … {dates[-1]}")
        else:
            print("WARNING: bronze payload produced no silver rows.")

        silver_rows = save_silver(hook, records)
        print(f"Loaded {silver_rows} silver row(s)")
        return silver_rows  # --xcom push: pass row count to next task

    @task  # --taskflow: downstream task
    def silver_to_gold(silver_rows: int) -> int:  # --xcom read: TaskFlow pulls upstream return value
        from google_fit.db.loader import build_gold, ensure_schemas

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)  # --hook: uses Airflow Connection
        ensure_schemas(hook)

        if silver_rows == 0:
            print("WARNING: skipping gold build — no silver rows were written.")

        gold_rows = build_gold(hook)
        print(f"Built {gold_rows} gold row(s) in gold.daily_summary")
        print("Verify: SELECT * FROM gold.daily_summary ORDER BY date DESC; (localhost:5434)")
        return gold_rows  # --xcom push: final task return value

    @task
    def emit_metric(metric: str) -> str:
        print(f"Metric mapped task ran for: {metric}")
        return metric

    @task(trigger_rule="all_done")
    def finalize_run(metrics: list[str]) -> None:
        print(f"Finalize transform run. mapped_metrics={metrics}")

    with TaskGroup(group_id="refine_layer"):
        silver = bronze_to_silver()  # --taskflow dependency: create upstream task
        gold = silver_to_gold(silver)  # --taskflow + xcom read: pass upstream output to downstream task
        silver >> gold  # --task dependency: explicit graph edge (same as TaskFlow arg above)

    mapped_metrics = emit_metric.expand(metric=["steps", "calories", "active_minutes"])  # --dynamic task mapping
    gold >> mapped_metrics >> finalize_run(mapped_metrics)


google_fit_transform()
