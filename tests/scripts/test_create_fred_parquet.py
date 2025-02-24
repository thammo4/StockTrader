#
# FILE: `StockTrader/tests/scripts/test_create_fred_parquet.py`
#

import os
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from scripts.create_fred_parquet import create_fred_parquet


@pytest.fixture(autouse=True)
def mock_env():
    """Mock environment variables for data storage"""
    with patch.dict(os.environ, {"STOCK_TRADER_MARKET_DATA": "/mock/data"}):
        yield


@pytest.fixture
def mock_fred_data():
    """Mock FRED API DataFrame Response"""
    rates = [5.24, 5.20, 5.15, 5.10, 5.05, 5.00, 4.95, 4.90, 4.85, 4.90, 4.33, 4.55]
    dates = pd.date_range(start="2024-01-01", end="2024-12-31", freq="MS")
    return pd.Series(rates, dates)


@pytest.fixture
def expected_formatted_data(mock_fred_data):
    df = mock_fred_data.reset_index()
    df.columns = ["fred_date", "fred_rate"]
    df["fred_date"] = pd.to_datetime(df["fred_date"]).astype(np.int64)
    df["fred_rate"] /= 100
    return df


def test_create_fred_parquet_df(mock_fred_data, expected_formatted_data):
    with patch("scripts.create_fred_parquet.fred.get_series", return_value=mock_fred_data), patch(
        "scripts.create_fred_parquet.pd.DataFrame.to_parquet"
    ):
        result = create_fred_parquet(return_df=True)

        #
        # Verify DataFrame Structure
        #

        assert list(result.columns) == ["fred_date", "fred_rate"]

        #
        # Verify Rate Column Contains Decimal Representation
        #

        assert result["fred_rate"].iloc[0] == mock_fred_data.iloc[0] / 100

        #
        # Verify Overall DataFrame Contents
        #

        pd.testing.assert_frame_equal(result, expected_formatted_data)


def test_create_fred_parquet_file(mock_fred_data, tmp_path):
    """Confirm fred parquet file creation and validate contents"""
    test_fpath = os.path.join(tmp_path, "fred_data.parquet")
    with patch("scripts.create_fred_parquet.fred.get_series", return_value=mock_fred_data), patch(
        "scripts.create_fred_parquet.STOCK_TRADER_MARKET_DATA", tmp_path
    ), patch("scripts.create_fred_parquet.os.path.join", return_value=test_fpath):
        create_fred_parquet()

        #
        # Confirm existence of parquet output
        #

        assert os.path.exists(test_fpath)

        #
        # Validate parquet contents
        #

        df_file = pd.read_parquet(test_fpath)
        assert "fred_date" in df_file.columns
        assert "fred_rate" in df_file.columns
        assert df_file["fred_rate"].iloc[0] == mock_fred_data.iloc[0] / 100


def test_create_fred_parquet_nofile_error():
    """Test that empty data from FRED raises ValueError"""
    with patch("scripts.create_fred_parquet.fred.get_series", return_value=pd.Series()), pytest.raises(
        ValueError, match="ERROR \\[create_fred_parquet\\]: ghosted by FRED"
    ):
        create_fred_parquet()


def test_create_fred_parquet_exceptions():
    """Test that create_fred_parquet exception handling ok"""
    with patch("scripts.create_fred_parquet.fred.get_series", side_effect=Exception("FRED API error")), pytest.raises(
        Exception
    ):
        create_fred_parquet()


def test_create_fred_parquet_custom_dates():
    """Verify that custom date parameters ok to pass"""
    mock_get_series = MagicMock(return_value=pd.Series([5.0], index=[pd.Timestamp("2023-01-01")]))

    with patch("scripts.create_fred_parquet.fred.get_series", mock_get_series), patch(
        "scripts.create_fred_parquet.pd.DataFrame.to_parquet"
    ):
        start_date = "2023-01-01"
        end_date = "2023-01-31"
        series_id = "GS10"

        create_fred_parquet(series=series_id, start_date=start_date, end_date=end_date)

        mock_get_series.assert_called_once_with(
            series_id=series_id, observation_start=start_date, observation_end=end_date
        )
