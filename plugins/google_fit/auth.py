from __future__ import annotations

from airflow.sdk.bases.hook import BaseHook
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

DEFAULT_CONN_ID = "google_fit_api"


def get_access_token(conn_id: str = DEFAULT_CONN_ID) -> str | None:
    """Return a Google Fit access token from the Airflow connection.

    Simplest setup — paste token from token.json into Connection Extra:
        {"token": "ya29...."}

    Auto-refresh setup — include refresh_token + client_id + client_secret in Extra
    (or client_id/client_secret in Login/Password). Token is refreshed when expired.
    """
    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception:
        return None

    extra = conn.extra_dejson
    token = extra.get("access_token") or extra.get("token")

    refresh_token = extra.get("refresh_token")
    client_id = conn.login or extra.get("client_id")
    client_secret = conn.password or extra.get("client_secret")

    if refresh_token and client_id and client_secret:
        creds = Credentials(
            token=token,
            refresh_token=refresh_token,
            token_uri=extra.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=client_id,
            client_secret=client_secret,
            scopes=extra.get("scopes"),
        )
        if not creds.valid:
            creds.refresh(Request())
        return creds.token

    return token
