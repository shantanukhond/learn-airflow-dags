"""
ONE-TIME, MANUAL script — run on your machine, NOT inside Airflow.
Opens a browser for login/consent, then writes token.json with your refresh token.

Usage:
    pip install google-auth-oauthlib google-auth google-api-python-client
    python get_token.py

Requires client_secret.json in the same folder (from Google Cloud Console).
"""

import json

from google_auth_oauthlib.flow import InstalledAppFlow

# Fitness API scopes (deprecated end of 2026; fine for learning/prototyping).
SCOPES = [
    "https://www.googleapis.com/auth/fitness.activity.read",
    "https://www.googleapis.com/auth/fitness.sleep.read",
    "https://www.googleapis.com/auth/fitness.heart_rate.read",
]

# Google Health API (long-term replacement):
# SCOPES = [
#     "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly",
#     "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly",
# ]


def main() -> None:
    flow = InstalledAppFlow.from_client_secrets_file("client_secret.json", scopes=SCOPES)
    creds = flow.run_local_server(port=0)

    token_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }

    with open("token.json", "w", encoding="utf-8") as f:
        json.dump(token_data, f, indent=2)

    print("Saved token.json")
    print()
    print("Copy these into Airflow Connection 'google_fit_api' (type: Generic):")
    print(f"  Login (client_id):     {creds.client_id}")
    print(f"  Password (secret):     {creds.client_secret}")
    print(f"  Extra refresh_token:   {creds.refresh_token}")
    print()
    print("Extra JSON example:")
    print(
        json.dumps(
            {
                "refresh_token": creds.refresh_token,
                "token": creds.token,
                "token_uri": creds.token_uri,
                "scopes": creds.scopes,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
