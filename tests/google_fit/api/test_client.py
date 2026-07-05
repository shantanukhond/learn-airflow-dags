from datetime import datetime, timezone
from unittest.mock import MagicMock

from google_fit.api.client import fetch_fitness_aggregate, parse_aggregate_to_silver
from google_fit.constants import AGGREGATE_METRICS

START = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
END = datetime(2024, 6, 3, 0, 0, 0, tzinfo=timezone.utc)


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


class TestFetchFitnessApi:
    def test_parse_api_response(self, fitness_api_mocks):
        mock_build, _ = fitness_api_mocks
        bucket_start_ms = int(datetime(2024, 6, 1, tzinfo=timezone.utc).timestamp() * 1000)
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        mock_service.users.return_value.dataset.return_value.aggregate.return_value.execute.return_value = {
            "bucket": [
                {
                    "startTimeMillis": bucket_start_ms,
                    "dataset": [
                        {"point": [{"value": [{"intVal": 1000}]}]},
                        {"point": [{"value": [{"fpVal": 1500.0}]}]},
                        {"point": [{"value": [{"fpVal": 250.0}]}]},
                    ],
                }
            ]
        }

        payload = fetch_fitness_aggregate("fake-token", START, START)
        silver = parse_aggregate_to_silver(payload)

        assert silver["steps"] == [{"date": "2024-06-01", "steps": 1000, "source": "google_fit"}]
        assert silver["distance"] == [
            {"date": "2024-06-01", "distance_m": 1500.0, "source": "google_fit"}
        ]
        assert silver["calories"] == [
            {"date": "2024-06-01", "calories": 250.0, "source": "google_fit"}
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
