from datetime import date, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

SCHEMAS = ("bronze", "silver", "gold")

SILVER_UPSERT_KEYS: dict[str, tuple[str, str]] = {
    "steps": ("SilverDailySteps", "steps"),
    "distance": ("SilverDailyDistance", "distance_m"),
    "calories": ("SilverDailyCalories", "calories"),
    "active_minutes": ("SilverDailyActiveMinutes", "active_minutes"),
    "heart_rate": ("SilverDailyHeartRate", "avg_bpm"),
    "sleep": ("SilverDailySleep", "sleep_minutes"),
}


def _models():
    """Return models module, reloading if a long-lived worker cached an old version."""
    import importlib

    import google_fit.db.models as models

    if not hasattr(models, "SilverDailyActiveMinutes"):
        models = importlib.reload(models)
    return models


def _migrate_bronze_fitness_raw(conn) -> None:
    """Recreate bronze.fitness_raw when it still has the pre-lookback run_date PK."""
    exists = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'bronze' AND table_name = 'fitness_raw'
            """
        )
    ).scalar()
    if not exists:
        return

    has_run_date = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = 'fitness_raw'
              AND column_name = 'run_date'
            """
        )
    ).scalar()
    has_start = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'bronze'
              AND table_name = 'fitness_raw'
              AND column_name = 'start'
            """
        )
    ).scalar()
    if has_run_date and not has_start:
        conn.execute(text("DROP TABLE bronze.fitness_raw"))


def ensure_schemas(hook: PostgresHook) -> None:
    models = _models()
    engine = hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        for schema in SCHEMAS:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        _migrate_bronze_fitness_raw(conn)
    models.Base.metadata.create_all(engine)


def _as_date(value: date | datetime | str) -> date:
    if isinstance(value, str):
        return date.fromisoformat(value)
    if isinstance(value, datetime):
        return value.date()
    return value


def save_bronze(
    hook: PostgresHook,
    start: date | datetime | str,
    end: date | datetime | str,
    payload: dict,
) -> None:
    models = _models()
    engine = hook.get_sqlalchemy_engine()
    start_date = _as_date(start)
    end_date = _as_date(end)
    with Session(engine) as session:
        stmt = insert(models.BronzeFitnessRaw).values(
            start=start_date, end=end_date, payload=payload
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["start", "end"],
            set_={"payload": stmt.excluded.payload, "loaded_at": func.now()},
        )
        session.execute(stmt)
        session.commit()


def save_silver(hook: PostgresHook, records: dict[str, list[dict]]) -> int:
    models = _models()
    engine = hook.get_sqlalchemy_engine()
    count = 0

    with Session(engine) as session:
        for key, (model_name, field) in SILVER_UPSERT_KEYS.items():
            model = getattr(models, model_name)
            for row in records.get(key, []):
                values = {
                    "date": _as_date(row["date"]),
                    field: row[field],
                    "source": row["source"],
                }
                stmt = insert(model).values(**values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["date"],
                    set_={
                        field: getattr(stmt.excluded, field),
                        "source": stmt.excluded.source,
                    },
                )
                session.execute(stmt)
                count += 1

        session.commit()

    return count


def build_gold(hook: PostgresHook) -> int:
    engine = hook.get_sqlalchemy_engine()
    sql = text(
        """
        INSERT INTO gold.daily_summary (
            date, steps, calories, active_minutes, avg_bpm, sleep_minutes, distance_m
        )
        SELECT
            s.date,
            s.steps,
            COALESCE(c.calories, 0),
            COALESCE(am.active_minutes, 0),
            COALESCE(hr.avg_bpm, 0),
            COALESCE(sl.sleep_minutes, 0),
            COALESCE(d.distance_m, 0)
        FROM silver.daily_steps s
        LEFT JOIN silver.daily_calories c USING (date)
        LEFT JOIN silver.daily_active_minutes am USING (date)
        LEFT JOIN silver.daily_heart_rate hr USING (date)
        LEFT JOIN silver.daily_sleep sl USING (date)
        LEFT JOIN silver.daily_distance d USING (date)
        ON CONFLICT (date) DO UPDATE SET
            steps = EXCLUDED.steps,
            calories = EXCLUDED.calories,
            active_minutes = EXCLUDED.active_minutes,
            avg_bpm = EXCLUDED.avg_bpm,
            sleep_minutes = EXCLUDED.sleep_minutes,
            distance_m = EXCLUDED.distance_m
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql)
        return result.rowcount
