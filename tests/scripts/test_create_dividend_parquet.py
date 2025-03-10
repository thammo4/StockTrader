#
# FILE: `StockTrader/tests/scripts/test_create_dividend_parquet.py`
#

import os
import requests
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, call
from scripts.create_dividend_parquet import create_dividend_parquet


@pytest.fixture(autouse=True)
def mock_env():
    """Mock environment variables"""
    with patch.dict(os.environ, {"STOCK_TRADER_MARKET_DATA": "/mock/data"}):
        yield


@pytest.fixture
def mock_valid_api_response():
    """Mock Valid Dividend Data from API"""
    return {
        "results": [
            {
                "tables": {
                    "cash_dividends": [
                        {"cash_amount": 0.55, "ex_date": "2024-01-10", "frequency": "quarterly"},
                        {"cash_amount": 0.679, "ex_date": "2024-04-10", "frequency": "quarterly"},
                    ]
                }
            }
        ]
    }


@pytest.fixture
def mock_fallback_api_response():
    """Mock Response Where Needed Data is in Second Element of 'results' List"""
    return {
        "results": [
            {"tables": {"cash_dividends": None}},
            {"tables": {"cash_dividends": [{"cash_amount": 0.56, "ex_date": "2024-06-01", "frequency": "quarterly"}]}},
        ]
    }


@pytest.fixture
def mock_empty_api_response():
    """Mock Empty API Response"""
    return {"results": []}


@pytest.fixture
def mock_invalid_api_response():
    """Mock Invalid API Response"""
    return None


@pytest.fixture
def mock_malformed_api_response():
    """Mock Garbage API Response"""
    return {"yo_mama_so_fat": "her_blood_type_is_ragu"}


@pytest.fixture
def mock_failed_api_request():
    """Simulate Failed API Request Raising RequestException"""

    def raise_request_exception(*args, **kwargs):
        raise requests.RequestException("Mock API Failure")

    return raise_request_exception


@pytest.fixture
def mock_http_error():
    """Simulated HTTP Error"""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("404 Client Error")
    return mock_response


@patch("scripts.create_dividend_parquet.requests.get")
def test_create_dividend_parquet_ok(mock_get, mock_valid_api_response):
    """Happy Path Test: Good Response -> Good Parquet"""
    mock_response = MagicMock()
    mock_response.json.return_value = [mock_valid_api_response]
    mock_get.return_value = mock_response

    result = create_dividend_parquet("TEST", return_df=True)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert set(result.columns) == {"cash_amount", "ex_date", "frequency", "symbol"}
    assert result["symbol"].iloc[0] == "TEST"
    assert result["cash_amount"].iloc[0] == 0.55
    assert mock_get.called


@patch("scripts.create_dividend_parquet.requests.get")
def test_create_dividend_parquet_empty_response(mock_get, mock_empty_api_response):
    """Test Empty API Response"""
    mock_response = MagicMock()
    mock_response.json.return_value = [mock_empty_api_response]
    mock_get.return_value = mock_response

    result = create_dividend_parquet("TEST", return_df=True)

    assert result is None
    assert mock_get.called


@patch("scripts.create_dividend_parquet.requests.get")
def test_create_dividend_parquet_invalid_response(mock_get, mock_invalid_api_response):
    """Test Empty API Response"""
    mock_response = MagicMock()
    mock_response.json.return_value = mock_invalid_api_response
    mock_get.return_value = mock_response

    result = create_dividend_parquet("TEST", return_df=True)

    assert result is None
    assert mock_get.called


@patch("scripts.create_dividend_parquet.requests.get")
def test_create_dividend_parquet_malformed_response(mock_get, mock_malformed_api_response):
    """Test for Malformed API Response"""
    mock_response = MagicMock()
    mock_response.json.return_value = [mock_malformed_api_response]
    mock_get.return_value = mock_response

    result = create_dividend_parquet("TEST", return_df=True)

    assert result is None
    assert mock_get.called


@patch("scripts.create_dividend_parquet.requests.get")
def test_create_dividend_parquet_failed_request(mock_get, mock_failed_api_request):
    """Test Failed API Request"""
    mock_get.side_effect = mock_failed_api_request

    result = create_dividend_parquet("TEST", return_df=True)

    assert result is None
    assert mock_get.called


@patch("scripts.create_dividend_parquet.requests.get")
def test_create_dividend_parquet_http_error(mock_get, mock_http_error):
    """Test HTTP Error Handling"""
    mock_get.return_value = mock_http_error

    result = create_dividend_parquet("TEST", return_df=True)

    assert result is None
    assert mock_get.called
    mock_http_error.raise_for_status.assert_called_once()


@patch("scripts.create_dividend_parquet.requests.get")
@patch("scripts.create_dividend_parquet.pd.DataFrame.to_parquet")
def test_create_dividend_parquet_write_ok(mock_to_parquet, mock_get, mock_valid_api_response):
    """Test Parquet Writes Fine"""
    mock_response = MagicMock()
    mock_response.json.return_value = [mock_valid_api_response]
    mock_get.return_value = mock_response
    mock_to_parquet.return_value = None

    result = create_dividend_parquet("TEST", return_df=True)

    assert isinstance(result, pd.DataFrame)
    assert mock_to_parquet.called
    assert mock_get.called


@patch("scripts.create_dividend_parquet.requests.get")
@patch("scripts.create_dividend_parquet.pd.DataFrame.to_parquet")
def test_create_dividend_parquet_write_failed(mock_to_parquet, mock_get, mock_valid_api_response):
    """Test Parquet Write Failures"""
    mock_response = MagicMock()
    mock_response.json.return_value = [mock_valid_api_response]
    mock_get.return_value = mock_response
    mock_to_parquet.side_effect = Exception("Parquet Write Error")

    result = create_dividend_parquet("TEST", return_df=True)

    assert isinstance(result, pd.DataFrame)
    assert mock_to_parquet.called
    assert mock_get.called


@patch("scripts.create_dividend_parquet.requests.get")
def test_create_dividend_parquet_fallback_ok(mock_get, mock_fallback_api_response):
    """Test When Dividend Data Contained in 'results' Second Element"""
    mock_response = MagicMock()
    mock_response.json.return_value = [mock_fallback_api_response]
    mock_get.return_value = mock_response

    result = create_dividend_parquet("TEST", return_df=True)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result["cash_amount"].iloc[0] == 0.56
    assert mock_get.called


@patch("scripts.create_dividend_parquet.requests.get")
def test_create_dividend_parquet_none(mock_get, mock_valid_api_response):
    """Test None returned when return_df False"""
    mock_response = MagicMock()
    mock_response.json.return_value = [mock_valid_api_response]
    mock_get.return_value = mock_response

    result = create_dividend_parquet("TEST", return_df=False)

    assert result is None
    assert mock_get.called


@patch("scripts.create_dividend_parquet.requests.get")
@patch("scripts.create_dividend_parquet.logger")
def test_create_dividend_parquet_logger_warning(mock_logger, mock_get, mock_empty_api_response):
    """Test Log Warnings"""
    mock_response = MagicMock()
    mock_response.json.return_value = [mock_empty_api_response]
    mock_get.return_value = mock_response

    result = create_dividend_parquet("TEST", return_df=True)

    assert result is None
    mock_logger.warning.assert_any_call("No data, symbol=TEST [create_dividend_parquet]")


@patch("scripts.create_dividend_parquet.requests.get")
@patch("scripts.create_dividend_parquet.logger")
def test_create_dividend_parquet_logger_error(mock_logger, mock_get, mock_failed_api_request):
    """Test Log Errors"""
    mock_response = MagicMock()
    mock_response.json.return_value = [mock_empty_api_response]
    mock_get.return_value = mock_response

    result = create_dividend_parquet("TEST", return_df=True)

    assert result is None
    assert mock_logger.error.call_count >= 1


@patch("scripts.create_dividend_parquet.requests.get")
@patch("scripts.create_dividend_parquet.pd.DataFrame.to_parquet")
@patch("scripts.create_dividend_parquet.logger")
def test_create_dividend_parquet_file_error(mock_logger, mock_to_parquet, mock_get, mock_valid_api_response):
    """Fuck FIFO, Fuck LIFO, and Fuck yo wife yo"""
    mock_response = MagicMock()
    mock_response.json.return_value = [mock_valid_api_response]
    mock_get.return_value = mock_response
    mock_to_parquet.side_effect = Exception("Parquet Write Error")

    result = create_dividend_parquet("TEST", return_df=True)

    assert isinstance(result, pd.DataFrame)
    mock_logger.error.assert_any_call("Parquet failed: Parquet Write Error [create_dividend_parquet]")
