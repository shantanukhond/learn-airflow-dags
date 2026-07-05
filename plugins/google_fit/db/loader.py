from datetime import date

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from google_fit.db.models import (
    Base,
    BronzeFitnessRaw,
    GoldDailySummary,
    SilverDailyCalories,
    SilverDailyDistance,
    SilverDailySteps,
)

SCHEMAS = ("bronze", "silver", "gold")


def ensure_schemas(hook: PostgresHook) -> None:
    engine = hook.get_sqlalchemy_engine()
    with engine.begin() as conn:
        for schema in SCHEMAS:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    Base.metadata.create_all(engine)


def _as_date(value: date | str) -> date:
    return date.fromisoformat(value) if isinstance(value, str) else value


def save_bronze(hook: PostgresHook, run_date: date | str, payload: dict) -> None:
    engine = hook.get_sqlalchemy_engine()
    row_date = _as_date(run_date)
    with Session(engine) as session:
        stmt = insert(BronzeFitnessRaw).values(run_date=row_date, payload=payload)
        stmt = stmt.on_conflict_do_update(
            index_elements=["run_date"],
            set_={"payload": stmt.excluded.payload, "loaded_at": func.now()},
        )
        session.execute(stmt)
        session.commit()


def load_bronze(hook: PostgresHook, run_date: date | str) -> dict:
    engine = hook.get_sqlalchemy_engine()
    row_date = _as_date(run_date)
    with Session(engine) as session:
        row = session.get(BronzeFitnessRaw, row_date)
        return row.payload if row else {}


def save_silver(hook: PostgresHook, records: dict[str, list[dict]]) -> int:
    engine = hook.get_sqlalchemy_engine()
    count = 0

    with Session(engine) as session:
        for row in records.get("steps", []):
            stmt = insert(SilverDailySteps).values(
                date=_as_date(row["date"]),
                steps=row["steps"],
                source=row["source"],
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["date"],
                set_={"steps": stmt.excluded.steps, "source": stmt.excluded.source},
            )
            session.execute(stmt)
            count += 1

        for row in records.get("distance", []):
            stmt = insert(SilverDailyDistance).values(
                date=_as_date(row["date"]),
                distance_m=row["distance_m"],
                source=row["source"],
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["date"],
                set_={
                    "distance_m": stmt.excluded.distance_m,
                    "source": stmt.excluded.source,
                },
            )
            session.execute(stmt)
            count += 1

        for row in records.get("calories", []):
            stmt = insert(SilverDailyCalories).values(
                date=_as_date(row["date"]),
                calories=row["calories"],
                source=row["source"],
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["date"],
                set_={"calories": stmt.excluded.calories, "source": stmt.excluded.source},
            )
            session.execute(stmt)
            count += 1

        session.commit()

    return count


def build_gold(hook: PostgresHook) -> int:
    engine = hook.get_sqlalchemy_engine()
    sql = text(
        """
        INSERT INTO gold.daily_summary (date, steps, distance_m, calories)
        SELECT
            s.date,
            s.steps,
            COALESCE(d.distance_m, 0),
            COALESCE(c.calories, 0)
        FROM silver.daily_steps s
        LEFT JOIN silver.daily_distance d USING (date)
        LEFT JOIN silver.daily_calories c USING (date)
        ON CONFLICT (date) DO UPDATE SET
            steps = EXCLUDED.steps,
            distance_m = EXCLUDED.distance_m,
            calories = EXCLUDED.calories
        """
    )
    with engine.begin() as conn:
        result = conn.execute(sql)
        return result.rowcount
