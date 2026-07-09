POSTGRES_CONN_ID = "google_fit_postgres"
GOOGLE_FIT_CONN_ID = "google_fit_api"
LOOKBACK_DAYS = 7
LOOKBACK_DAYS_VAR = "google_fit_lookback_days"


def lookback_days_from_context(context: dict) -> int:
    """--params + --variable: param overrides per run; Variable is global fallback."""
    from airflow.models import Variable

    param_days = context.get("params", {}).get("lookback_days")
    if param_days is not None:
        return int(param_days)
    return int(Variable.get(LOOKBACK_DAYS_VAR, default_var=LOOKBACK_DAYS))
