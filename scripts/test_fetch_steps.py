#!/usr/bin/env python3
"""
Test Google Fit credentials by fetching real step counts.

Run from airflow-code/ with venv active:
    source .venv/bin/activate
    python scripts/test_fetch_steps.py

Requires scripts/token.json (from get_token.py) or pass --token-file.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "plugins"))

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from google_fit.api.client import fetch_fitness_aggregate, parse_aggregate_to_silver


def resolve_token_path(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()

    for candidate in (Path(__file__).parent / "token.json", ROOT / "token.json", Path("token.json")):
        if candidate.exists():
            return candidate.resolve()

    raise FileNotFoundError(
        "token.json not found. Run `python scripts/get_token.py` first "
        "or pass --token-file /path/to/token.json"
    )


def get_access_token(token_path: Path) -> str:
    data = json.loads(token_path.read_text(encoding="utf-8"))

    required = ("refresh_token", "client_id", "client_secret")
    missing = [key for key in required if not data.get(key)]
    if missing:
        raise ValueError(f"token.json is missing required fields: {', '.join(missing)}")

    creds = Credentials(
        token=data.get("token"),
        refresh_token=data["refresh_token"],
        token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=data["client_id"],
        client_secret=data["client_secret"],
        scopes=data.get("scopes"),
    )

    if not creds.valid:
        creds.refresh(Request())
        print("Refreshed access token successfully.")
    else:
        print("Using existing access token (still valid).")

    return creds.token


def main() -> None:
    parser = argparse.ArgumentParser(description="Test Google Fit credentials with a live API call.")
    parser.add_argument("--token-file", help="Path to token.json (default: scripts/token.json)")
    parser.add_argument("--days", type=int, default=7, help="Number of days to fetch (default: 7)")
    args = parser.parse_args()

    token_path = resolve_token_path(args.token_file)
    print(f"Using credentials from: {token_path}")

    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=args.days - 1)

    print(f"Fetching fitness data from {start.date()} to {end.date()} ...")
    print()

    access_token = get_access_token(token_path)
    payload = fetch_fitness_aggregate(access_token, start, end)
    silver = parse_aggregate_to_silver(payload)

    if payload.get("source") == "stub":
        print("WARNING: Got stub data — access token was not used.")
        sys.exit(1)

    print(f"{'Date':<12} {'Steps':>8} {'Distance m':>12} {'Calories':>10}")
    print("-" * 48)
    for row in silver["steps"]:
        day = row["date"]
        dist = next((d["distance_m"] for d in silver["distance"] if d["date"] == day), 0)
        cal = next((c["calories"] for c in silver["calories"] if c["date"] == day), 0)
        print(f"{day:<12} {row['steps']:>8} {dist:>12.0f} {cal:>10.0f}")

    print()
    print("Credentials work — live Google Fit data received.")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
