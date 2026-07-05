import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PLUGINS_DIR = Path(__file__).resolve().parents[1] / "plugins"
sys.path.insert(0, str(PLUGINS_DIR))


@pytest.fixture
def fitness_api_mocks():
    mock_build = MagicMock()
    mock_credentials_cls = MagicMock()

    mock_discovery = MagicMock()
    mock_discovery.build = mock_build

    mock_oauth2_credentials = MagicMock()
    mock_oauth2_credentials.Credentials = mock_credentials_cls

    mock_oauth2 = MagicMock()
    mock_oauth2.credentials = mock_oauth2_credentials

    modules = {
        "googleapiclient": MagicMock(),
        "googleapiclient.discovery": mock_discovery,
        "google": MagicMock(),
        "google.oauth2": mock_oauth2,
        "google.oauth2.credentials": mock_oauth2_credentials,
    }

    with patch.dict(sys.modules, modules):
        yield mock_build, mock_credentials_cls
