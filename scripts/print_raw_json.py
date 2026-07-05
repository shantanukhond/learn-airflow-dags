#!/usr/bin/env python3
"""
Print full raw Google Fit JSON (same shape stored in bronze.fitness_raw).

Run from airflow-code/ with venv active:
    source .venv/bin/activate
    python scripts/print_raw_json.py

Requires token.json (from get_token.py) or pass --token-file.
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

    return creds.token


def main() -> None:
    parser = argparse.ArgumentParser(description="Print full Google Fit API JSON.")
    parser.add_argument("--token-file", help="Path to token.json")
    parser.add_argument("--days", type=int, default=7, help="Days to fetch (default: 7)")
    parser.add_argument("--silver", action="store_true", help="Also print parsed silver JSON")
    args = parser.parse_args()

    token_path = resolve_token_path(args.token_file)
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=args.days - 1)

    print(f"Using credentials from: {token_path}")
    print(f"Date range: {start.date()} to {end.date()}")
    print()

    payload = fetch_fitness_aggregate(get_access_token(token_path), start, end)

    print("=== BRONZE (full raw JSON — saved to bronze.fitness_raw.payload) ===")
    print(json.dumps(payload, indent=2))

    if args.silver:
        print()
        print("=== SILVER (parsed rows) ===")
        print(json.dumps(parse_aggregate_to_silver(payload), indent=2))


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
