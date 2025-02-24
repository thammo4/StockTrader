#
# FILE: `StockTrader/tests/scripts/test_create_ohlcv_parquet.py`
#

import os
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, mock_open, MagicMock
from scripts.create_ohlcv_parquet import create_ohlcv_parquet


@pytest.fixture
def mock_quotes():
    """Mock Tradier API Hisotrical OHLCV Quotes"""
    mock_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=5, freq="D"),
            "open": np.random.randn(5) * 100,
            "high": np.random.randn(5) * 100,
            "low": np.random.randn(5) * 100,
            "close": np.random.randn(5) * 100,
            "volume": np.random.randint(1000, 5000, 5),
        }
    )
    mock_api = MagicMock()
    mock_api.get_historical_quotes.return_value = mock_df
    return mock_api


@pytest.fixture(autouse=True)
def mock_env():
    with patch.dict(os.environ, {"STOCK_TRADER_MARKET_DATA": "/mock/data"}):
        yield


@patch("os.path.exists", return_value=True)
@patch("scripts.create_ohlcv_parquet.quotes", new_callable=MagicMock)
def test_ohlcv_parquet(mock_quotes, mock_exists):
    """Test create_ohlcv_parquet function"""
    mock_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=5, freq="D"),
            "open": np.abs(np.random.randn(5) * 100),
            "high": np.abs(np.random.randn(5) * 100),
            "low": np.abs(np.random.randn(5) * 100),
            "close": np.abs(np.random.randn(5) * 100),
            "volume": np.abs(np.random.randint(1000, 5000, 5)),
        }
    )
    mock_quotes.get_historical_quotes.return_value = mock_df
    result = create_ohlcv_parquet("TEST", return_df=True)

    assert isinstance(result, pd.DataFrame)
    assert "log_return" in result.columns
    assert result.shape[0] == 4

    mock_quotes.get_historical_quotes.assert_called_once()
