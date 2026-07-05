from dataclasses import dataclass
from typing import Literal

ValueKind = Literal["int", "float"]


@dataclass(frozen=True)
class AggregateMetric:
    """Google Fit aggregate metric config (order matches dataset[] in API response)."""

    key: str
    data_type_name: str
    value_kind: ValueKind
    silver_field: str


AGGREGATE_METRICS: tuple[AggregateMetric, ...] = (
    AggregateMetric("steps", "com.google.step_count.delta", "int", "steps"),
    AggregateMetric("distance", "com.google.distance.delta", "float", "distance_m"),
    AggregateMetric("calories", "com.google.calories.expended", "float", "calories"),
)

# Google Fit API
FITNESS_API = "fitness"
FITNESS_API_VERSION = "v1"
FITNESS_USER_ID = "me"
DAILY_BUCKET_MS = 86_400_000
