from datetime import date, datetime, timedelta, timezone

POSTGRES_CONN_ID = "google_fit_postgres"
GOOGLE_FIT_CONN_ID = "google_fit_api"
LOOKBACK_DAYS = 7


def lookback_window(context: dict, days: int = LOOKBACK_DAYS) -> tuple[datetime, datetime]:
    """Last N complete UTC days ending yesterday (excludes today)."""
    ref = context["data_interval_end"] or context["data_interval_start"]
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    else:
        ref = ref.astimezone(timezone.utc)

    run_day = ref.replace(hour=0, minute=0, second=0, microsecond=0)
    end = run_day
    start = run_day - timedelta(days=days)
    return start, end


def bronze_run_date(context: dict, days: int = LOOKBACK_DAYS) -> date:
    """Bronze PK date — yesterday (last day in the lookback window)."""
    _, end = lookback_window(context, days)
    return (end - timedelta(days=1)).date()
