from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _constants():
    """Return constants module, reloading if a worker cached an old AGGREGATE_METRICS."""
    import importlib

    import google_fit.constants as constants

    # Reload if a long-lived worker cached a metrics list missing current keys.
    # distance is intentionally omitted: it needs fitness.location.read and 403s otherwise.
    expected = {"steps", "calories", "active_minutes", "heart_rate", "sleep"}
    if {m.key for m in constants.AGGREGATE_METRICS} != expected:
        constants = importlib.reload(constants)
    return constants


def fetch_fitness_aggregate(
    access_token: str | None, start: datetime, end: datetime
) -> dict:
    """Fetch raw Google Fit data (bronze layer). Single aggregate API call."""
    constants = _constants()

    if not access_token:
        return _stub_payload(start, end, constants.AGGREGATE_METRICS)

    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    service = build(
        constants.FITNESS_API,
        constants.FITNESS_API_VERSION,
        credentials=Credentials(token=access_token),
        cache_discovery=False,
    )

    response = service.users().dataset().aggregate(
        userId=constants.FITNESS_USER_ID,
        body={
            "bucketByTime": {"durationMillis": constants.DAILY_BUCKET_MS},
            "startTimeMillis": int(start.timestamp() * 1000),
            "endTimeMillis": int(end.timestamp() * 1000),
            "aggregateBy": [
                {"dataTypeName": m.data_type_name} for m in constants.AGGREGATE_METRICS
            ],
        },
    ).execute()

    return {"bucket": response.get("bucket", []), "source": "google_fit"}


def _stub_payload(start: datetime, end: datetime, metrics) -> dict:
    buckets = []
    num_days = max((end.date() - start.date()).days, 1)
    for i in range(num_days):
        day = start + timedelta(days=i)
        ms = int(day.timestamp() * 1000)
        datasets = []
        for metric in metrics:
            if metric.aggregate == "sleep_minutes":
                datasets.append({"point": []})
            elif metric.value_kind == "int":
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


def _avg_float(dataset: dict) -> float:
    values: list[float] = []
    for point in dataset.get("point", []):
        for value in point.get("value", []):
            if "fpVal" in value:
                values.append(float(value["fpVal"]))
            elif "intVal" in value:
                values.append(float(value["intVal"]))
    return sum(values) / len(values) if values else 0.0


def _sleep_minutes(dataset: dict) -> float:
    total_ns = 0
    for point in dataset.get("point", []):
        start = int(point.get("startTimeNanos", 0))
        end = int(point.get("endTimeNanos", 0))
        if end > start:
            total_ns += end - start
    return total_ns / 60_000_000_000


_AGGREGATORS = {
    "sum_int": _sum_int,
    "sum_float": _sum_float,
    "avg_float": _avg_float,
    "sleep_minutes": _sleep_minutes,
}


def parse_aggregate_to_silver(payload: dict) -> dict[str, list[dict]]:
    """Turn bronze JSON into rows for silver tables."""
    metrics = _constants().AGGREGATE_METRICS
    source = payload.get("source", "google_fit")
    records = {metric.key: [] for metric in metrics}

    for bucket in payload.get("bucket", []):
        day = datetime.fromtimestamp(
            int(bucket["startTimeMillis"]) / 1000, tz=timezone.utc
        ).date().isoformat()
        datasets = bucket.get("dataset", [])

        for metric, dataset in zip(metrics, datasets):
            value = _AGGREGATORS[metric.aggregate](dataset)
            records[metric.key].append(
                {"date": day, metric.silver_field: value, "source": source}
            )

    return records
