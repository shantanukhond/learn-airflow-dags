from datetime import datetime, timezone
from unittest.mock import MagicMock

from google_fit.api.client import fetch_fitness_aggregate, parse_aggregate_to_silver
from google_fit.constants import AGGREGATE_METRICS

START = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
END = datetime(2024, 6, 4, 0, 0, 0, tzinfo=timezone.utc)  # exclusive end → 3 buckets from Jun 1


class TestFetchFitnessStub:
    def test_stub_returns_buckets(self):
        payload = fetch_fitness_aggregate(None, START, END)

        assert len(payload["bucket"]) == 3
        assert payload["source"] == "stub"

    def test_parse_stub_to_silver(self):
        payload = fetch_fitness_aggregate(None, START, START)
        silver = parse_aggregate_to_silver(payload)

        assert silver["steps"][0]["date"] == "2024-06-01"
        assert silver["steps"][0]["steps"] == 0
        assert silver["sleep"][0]["sleep_minutes"] == 0.0


class TestFetchFitnessApi:
    def test_parse_api_response(self, fitness_api_mocks):
        mock_build, _ = fitness_api_mocks
        bucket_start_ms = int(datetime(2024, 6, 1, tzinfo=timezone.utc).timestamp() * 1000)
        sleep_start_ns = bucket_start_ms * 1_000_000
        sleep_end_ns = sleep_start_ns + 420 * 60 * 1_000_000_000
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        mock_service.users.return_value.dataset.return_value.aggregate.return_value.execute.return_value = {
            "bucket": [
                {
                    "startTimeMillis": bucket_start_ms,
                    "dataset": [
                        {"point": [{"value": [{"intVal": 1000}]}]},
                        {"point": [{"value": [{"fpVal": 250.0}]}]},
                        {"point": [{"value": [{"intVal": 45}]}]},
                        {"point": [{"value": [{"fpVal": 72.0}, {"fpVal": 68.0}]}]},
                        {
                            "point": [
                                {
                                    "startTimeNanos": sleep_start_ns,
                                    "endTimeNanos": sleep_end_ns,
                                }
                            ]
                        },
                    ],
                }
            ]
        }

        payload = fetch_fitness_aggregate("fake-token", START, START)
        silver = parse_aggregate_to_silver(payload)

        assert silver["steps"] == [{"date": "2024-06-01", "steps": 1000, "source": "google_fit"}]
        assert silver["calories"] == [
            {"date": "2024-06-01", "calories": 250.0, "source": "google_fit"}
        ]
        assert silver["active_minutes"] == [
            {"date": "2024-06-01", "active_minutes": 45, "source": "google_fit"}
        ]
        assert silver["heart_rate"] == [
            {"date": "2024-06-01", "avg_bpm": 70.0, "source": "google_fit"}
        ]
        assert silver["sleep"] == [
            {"date": "2024-06-01", "sleep_minutes": 420.0, "source": "google_fit"}
        ]

    def test_single_aggregate_call(self, fitness_api_mocks):
        mock_build, _ = fitness_api_mocks
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        mock_service.users.return_value.dataset.return_value.aggregate.return_value.execute.return_value = {
            "bucket": []
        }

        fetch_fitness_aggregate("fake-token", START, END)

        assert mock_service.users.return_value.dataset.return_value.aggregate.call_count == 1
        body = mock_service.users.return_value.dataset.return_value.aggregate.call_args.kwargs["body"]
        assert len(body["aggregateBy"]) == len(AGGREGATE_METRICS)
        data_types = [entry["dataTypeName"] for entry in body["aggregateBy"]]
        assert "com.google.sleep.segment" in data_types
        assert "com.google.distance.delta" not in data_types
