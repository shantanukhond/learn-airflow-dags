# Airflow Concepts — Google Fit Project

Each concept from the [Core Concepts](https://airflow.atwish.org/docs/core-concepts) docs, with a **real example from this codebase**.

**Legend:** ✅ covered in this project · 🔜 planned for a follow-up lesson

---

## ✅ DAGs

A **DAG** (Directed Acyclic Graph) is a workflow definition — tasks and the order they run in.

This project has **two DAGs**:

| DAG | File | Purpose |
|-----|------|---------|
| `google_fit_ingest` | `dag_google_fit_ingest.py` | Pull API data → bronze |
| `google_fit_transform` | `dag_google_fit_transform.py` | bronze → silver → gold |

```python
@dag(
    dag_id="google_fit_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["google_fit", "bronze"],
    description="Pull Google Fit API data into bronze.fitness_raw.",
)
def google_fit_ingest():
    ...
```

---

## ✅ Operators

An **operator** is a template for a single unit of work. You configure it; Airflow runs it.

| Operator | Where | What it does |
|----------|-------|--------------|
| `@task` (Python) | Both DAG files | Run Python functions |
| `TriggerDagRunOperator` | `dag_google_fit_ingest.py` | Start `google_fit_transform` after ingest |

```python
# Python operator via TaskFlow decorator
@task
def extract_to_bronze(**context) -> int:
    ...

# Classic operator — trigger another DAG
trigger_transform = TriggerDagRunOperator(
    task_id="trigger_transform",
    trigger_dag_id="google_fit_transform",
    logical_date="{{ logical_date }}",
    wait_for_completion=False,
)
```

---

## ✅ PythonOperator / `@task` (TaskFlow API)

The **TaskFlow API** turns Python functions into tasks with `@task`. Return values are automatically pushed to XCom.

**Ingest — single task:**

```python
@task
def extract_to_bronze(**context) -> int:
    ...
    return bucket_count
```

**Transform — two tasks with data passed between them:**

```python
@task
def bronze_to_silver(**context) -> int:
    ...
    return silver_rows

@task
def silver_to_gold(silver_rows: int) -> int:
    ...
    return gold_rows

silver = bronze_to_silver()
gold = silver_to_gold(silver)   # silver_rows passed via TaskFlow / XCom
```

---

## ✅ Task dependencies

Tasks run in an order defined by **dependencies**. Two equivalent ways to express them:

### 1. TaskFlow arguments (implicit)

```python
gold = silver_to_gold(silver)   # gold waits for silver
```

### 2. Bit-shift operator `>>` (explicit — shows in Graph view)

```python
# dag_google_fit_ingest.py
extract_to_bronze() >> trigger_transform

# dag_google_fit_transform.py
silver >> gold
```

Both patterns are used in `dag_google_fit_transform.py` on purpose — same dependency, two syntaxes.

---

## ✅ XComs

**XCom** (cross-communication) lets tasks pass small values within a DAG run.

| Pattern | Example in this project |
|---------|-------------------------|
| TaskFlow return value | `bronze_to_silver` returns `silver_rows` → `silver_to_gold(silver_rows)` |
| View in UI | Task → XCom tab → see `return_value` |

```python
@task
def bronze_to_silver(**context) -> int:
    ...
    return silver_rows          # pushed to XCom automatically

@task
def silver_to_gold(silver_rows: int) -> int:
    if silver_rows == 0:        # read from XCom via TaskFlow arg
        print("WARNING: skipping gold build ...")
```

**Cross-DAG data** does not use XCom here — bronze/silver/gold tables in Postgres pass data between DAGs (medallion pattern).

---

## ✅ Hooks + Connections

A **Hook** is how tasks talk to external systems. A **Connection** stores credentials in Airflow (UI or env vars).

### PostgresHook

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
engine = hook.get_sqlalchemy_engine()
```

Used in every DAG task that reads/writes the fitness database.

### BaseHook (custom Google OAuth)

```python
# plugins/google_fit/auth.py
from airflow.sdk.bases.hook import BaseHook

conn = BaseHook.get_connection("google_fit_api")
token = conn.extra_dejson.get("token")
```

### Connections in this project

| Connection ID | Type | Set up |
|---------------|------|--------|
| `google_fit_postgres` | Postgres | Auto via `AIRFLOW_CONN_GOOGLE_FIT_POSTGRES` in docker-compose |
| `google_fit_api` | Generic | Manual — paste `token` from `token.json` into Extra |

```json
{
  "token": "ya29.PASTE_FROM_token.json"
}
```

---

## ✅ Scheduling (cron + catchup)

**Schedule** controls when a DAG runs. **Catchup** controls whether Airflow backfills missed runs.

```python
# Runs every day at midnight (UTC)
@dag(schedule="@daily", catchup=False, ...)
def google_fit_ingest():
    ...

# No schedule — only runs when triggered (by ingest or manually)
@dag(schedule=None, catchup=False, ...)
def google_fit_transform():
    ...
```

| Setting | `google_fit_ingest` | `google_fit_transform` |
|---------|---------------------|------------------------|
| `schedule` | `@daily` | `None` (triggered) |
| `catchup` | `False` | `False` |

`catchup=False` means: don't replay every day since `start_date=2024-01-01` — only run forward.

---

## ✅ Context (`**context`)

Airflow injects a **context** dict into tasks. Use it for run metadata like dates.

```python
# plugins/google_fit/dag_utils.py
def lookback_window(context: dict, days: int = LOOKBACK_DAYS):
    ref = context["data_interval_end"] or context["data_interval_start"]
    ...
```

```python
# dag_google_fit_ingest.py
@task
def extract_to_bronze(**context) -> int:
    start, end = lookback_window(context)
    run_date = bronze_run_date(context)
```

| Context key | Used for |
|-------------|----------|
| `data_interval_end` | Anchor date for the 7-day lookback window |
| `data_interval_start` | Fallback if end is missing |
| `logical_date` | Passed to `TriggerDagRunOperator` template |

---

## ✅ Plugins

**Plugins** are shared Python modules Airflow loads from `plugins/`. DAGs import them like any package.

```
plugins/google_fit/
├── dag_utils.py        # lookback window, connection IDs
├── auth.py             # get_access_token()
├── api/client.py       # Google Fit API
└── db/loader.py        # bronze / silver / gold writes
```

```python
# Inside a DAG task
from google_fit.api.client import fetch_fitness_aggregate
from google_fit.auth import get_access_token
from google_fit.db.loader import save_bronze
```

Keeps DAG files thin — business logic lives in plugins.

---

## ✅ Executors

An **executor** is what actually runs your tasks (local process, Celery worker, Kubernetes pod, etc.).

This project does not configure an executor in code — it uses whatever the [learn-airflow](https://github.com/shantanukhond/learn-airflow) Docker Compose stack provides (**LocalExecutor**).

**Where to observe it:** Airflow UI → task instance → logs show `executor=LocalExecutor`.

---

## 🔜 Sensors

A **sensor** waits for an external condition (file exists, API ready, partition landed).

**Not used yet.** A natural addition for this project:

```python
# Example — wait for bronze row before transform
from airflow.sensors.sql import SqlSensor

wait_for_bronze = SqlSensor(
    task_id="wait_for_bronze",
    conn_id="google_fit_postgres",
    sql="SELECT 1 FROM bronze.fitness_raw WHERE run_date = %(date)s",
    params={"date": "{{ ds }}"},
    mode="reschedule",
    poke_interval=60,
    timeout=60 * 30,
)
```

See [Sensors — poke vs reschedule](https://airflow.atwish.org/docs/core-concepts#sensors) in the docs.

---

## 🔜 Task Groups

**Task Groups** visually group related tasks in the Graph view.

**Not used yet.** Could wrap transform tasks:

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("medallion_refine") as refine:
    silver = bronze_to_silver()
    gold = silver_to_gold(silver)
    silver >> gold
```

---

## 🔜 Dynamic Task Mapping

**Dynamic task mapping** spawns one task instance per item (e.g. one task per day for backfill).

**Not used yet.** Could map over a date range:

```python
@task
def fetch_one_day(day: str) -> dict:
    ...

days = ["2026-06-28", "2026-06-29", ...]
fetch_one_day.expand(day=days)
```

---

## 🔜 Trigger Rules

**Trigger rules** control when a task runs based on upstream task states (default: `all_success`).

**Not used yet.** Example — run gold even if silver wrote zero rows:

```python
@task(trigger_rule="all_done")
def silver_to_gold(silver_rows: int) -> int:
    ...
```

---

## 🔜 Retries + `retry_delay`

**Retries** re-run a failed task automatically after a delay.

**Not used yet.** Good fit for flaky Google Fit API calls:

```python
@task(retries=3, retry_delay=timedelta(minutes=5))
def extract_to_bronze(**context) -> int:
    ...
```

---

## 🔜 Variables & Params

**Variables** are global key-value config. **Params** are per-DAG run inputs from the UI.

**Not used yet.** `LOOKBACK_DAYS` in `dag_utils.py` could become a Variable:

```python
from airflow.models import Variable

LOOKBACK_DAYS = int(Variable.get("google_fit_lookback_days", default_var=7))
```

Or a DAG param:

```python
@dag(params={"lookback_days": 7}, ...)
def google_fit_ingest():
    @task
    def extract_to_bronze(**context) -> int:
        days = context["params"]["lookback_days"]
```

---

## 🔜 SLAs / `sla_miss_callback`

An **SLA** (Service Level Agreement) flags tasks that take too long.

**Not used yet.**

```python
@task(sla=timedelta(hours=2))
def extract_to_bronze(**context) -> int:
    ...
```

---

## 🔜 `on_failure_callback`

A **callback** runs when a task fails — useful for Slack/email alerts.

**Not used yet.**

```python
def alert_on_failure(context):
    print(f"Task {context['task_instance'].task_id} failed!")

@task(on_failure_callback=alert_on_failure)
def extract_to_bronze(**context) -> int:
    ...
```

---

## 🔜 BranchPythonOperator

**Branching** runs only one downstream path based on a condition.

**Not used yet.** Could skip gold when bronze is empty:

```python
from airflow.operators.python import BranchPythonOperator

def choose_branch(**context):
    if bronze_is_empty(context):
        return "skip_gold"
    return "bronze_to_silver"
```

---

## Quick reference — concept → file

| Concept | File(s) |
|---------|---------|
| DAG definition | `dag_google_fit_ingest.py`, `dag_google_fit_transform.py` |
| `@task` | Both DAG files |
| Task dependencies `>>` | `dag_google_fit_ingest.py:61`, `dag_google_fit_transform.py:63` |
| TaskFlow / XCom | `dag_google_fit_transform.py:59-60` |
| TriggerDagRunOperator | `dag_google_fit_ingest.py:54-59` |
| PostgresHook | All DAG tasks |
| BaseHook + Connection | `plugins/google_fit/auth.py` |
| Context / dates | `plugins/google_fit/dag_utils.py` |
| Scheduling | `@daily` on ingest, `schedule=None` on transform |
| Plugins | `plugins/google_fit/` |

---

Back to [project readme](readme.md) · [Core Concepts docs](https://airflow.atwish.org/docs/core-concepts)
