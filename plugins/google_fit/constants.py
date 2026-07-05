from dataclasses import dataclass
from typing import Literal

ValueKind = Literal["int", "float"]
AggregateMode = Literal["sum_int", "sum_float", "avg_float", "sleep_minutes"]


@dataclass(frozen=True)
class AggregateMetric:
    """Google Fit aggregate metric config (order matches dataset[] in API response)."""

    key: str
    data_type_name: str
    value_kind: ValueKind
    silver_field: str
    aggregate: AggregateMode


AGGREGATE_METRICS: tuple[AggregateMetric, ...] = (
    AggregateMetric("steps", "com.google.step_count.delta", "int", "steps", "sum_int"),
    AggregateMetric("calories", "com.google.calories.expended", "float", "calories", "sum_float"),
    AggregateMetric(
        "active_minutes", "com.google.active_minutes", "int", "active_minutes", "sum_int"
    ),
    AggregateMetric("heart_rate", "com.google.heart_rate.bpm", "float", "avg_bpm", "avg_float"),
    AggregateMetric("sleep", "com.google.sleep.segment", "float", "sleep_minutes", "sleep_minutes"),
)

# Google Fit API
FITNESS_API = "fitness"
FITNESS_API_VERSION = "v1"
FITNESS_USER_ID = "me"
DAILY_BUCKET_MS = 86_400_000
