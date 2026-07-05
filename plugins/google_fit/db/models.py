from datetime import date, datetime

from sqlalchemy import Date, DateTime, Float, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class BronzeFitnessRaw(Base):
    __tablename__ = "fitness_raw"
    __table_args__ = {"schema": "bronze"}

    run_date: Mapped[date] = mapped_column(Date, primary_key=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    loaded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class SilverDailySteps(Base):
    __tablename__ = "daily_steps"
    __table_args__ = {"schema": "silver"}

    date: Mapped[date] = mapped_column(Date, primary_key=True)
    steps: Mapped[int] = mapped_column(Integer, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)


class SilverDailyDistance(Base):
    __tablename__ = "daily_distance"
    __table_args__ = {"schema": "silver"}

    date: Mapped[date] = mapped_column(Date, primary_key=True)
    distance_m: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)


class SilverDailyCalories(Base):
    __tablename__ = "daily_calories"
    __table_args__ = {"schema": "silver"}

    date: Mapped[date] = mapped_column(Date, primary_key=True)
    calories: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)


class GoldDailySummary(Base):
    __tablename__ = "daily_summary"
    __table_args__ = {"schema": "gold"}

    date: Mapped[date] = mapped_column(Date, primary_key=True)
    steps: Mapped[int] = mapped_column(Integer, nullable=False)
    distance_m: Mapped[float] = mapped_column(Float, nullable=False)
    calories: Mapped[float] = mapped_column(Float, nullable=False)
