from __future__ import annotations

from datetime import datetime, timedelta, timezone

from google_fit.constants import (
    AGGREGATE_METRICS,
    DAILY_BUCKET_MS,
    FITNESS_API,
    FITNESS_API_VERSION,
    FITNESS_USER_ID,
)


def fetch_fitness_aggregate(
    access_token: str | None, start: datetime, end: datetime
) -> dict:
    """Fetch raw Google Fit data (bronze layer). Single aggregate API call."""
    if not access_token:
        return _stub_payload(start, end)

    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    service = build(
        FITNESS_API,
        FITNESS_API_VERSION,
        credentials=Credentials(token=access_token),
        cache_discovery=False,
    )

    response = service.users().dataset().aggregate(
        userId=FITNESS_USER_ID,
        body={
            "bucketByTime": {"durationMillis": DAILY_BUCKET_MS},
            "startTimeMillis": int(start.timestamp() * 1000),
            "endTimeMillis": int(end.timestamp() * 1000),
            "aggregateBy": [{"dataTypeName": m.data_type_name} for m in AGGREGATE_METRICS],
        },
    ).execute()

    return {"bucket": response.get("bucket", []), "source": "google_fit"}


def _stub_payload(start: datetime, end: datetime) -> dict:
    buckets = []
    for i in range((end.date() - start.date()).days + 1):
        day = start + timedelta(days=i)
        ms = int(day.timestamp() * 1000)
        datasets = []
        for metric in AGGREGATE_METRICS:
            if metric.value_kind == "int":
                datasets.append({"point": [{"value": [{"intVal": 0}]}]})
            else:
                datasets.append({"point": [{"value": [{"fpVal": 0.0}]}]})
        buckets.append({"startTimeMillis": ms, "dataset": datasets})
    return {"bucket": buckets, "source": "stub"}


def _sum_int(dataset: dict) -> int:
    total = 0
    for point in dataset.get("point", []):
        for value in point.get("value", []):
            total += int(value.get("intVal", 0))
    return total


def _sum_float(dataset: dict) -> float:
    total = 0.0
    for point in dataset.get("point", []):
        for value in point.get("value", []):
            total += float(value.get("fpVal", 0))
    return total


def parse_aggregate_to_silver(payload: dict) -> dict[str, list[dict]]:
    """Turn bronze JSON into rows for silver tables."""
    source = payload.get("source", "google_fit")
    records = {metric.key: [] for metric in AGGREGATE_METRICS}

    for bucket in payload.get("bucket", []):
        day = datetime.fromtimestamp(
            int(bucket["startTimeMillis"]) / 1000, tz=timezone.utc
        ).date().isoformat()
        datasets = bucket.get("dataset", [])

        for metric, dataset in zip(AGGREGATE_METRICS, datasets):
            if metric.value_kind == "int":
                value = _sum_int(dataset)
            else:
                value = _sum_float(dataset)
            records[metric.key].append(
                {"date": day, metric.silver_field: value, "source": source}
            )

    return records
