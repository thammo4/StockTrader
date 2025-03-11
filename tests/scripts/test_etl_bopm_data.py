#
# FILE: `StockTrader/tests/scripts/test_etl_bopm_data.py`
#

import os
import pytest
from datetime import date
from unittest.mock import patch, MagicMock, mock_open
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType
import pyspark.sql.functions as F
from scripts.etl_bopm_data import etl_bopm_data

# from StockTrader.settings import STOCK_TRADER_MARKET_DATA


@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder.master("local[*]").appName("TEST_ETL_BOPM").getOrCreate()
    yield spark
    spark.stop()


def test_etl_bopm_data_ok(spark_session):
    """Verify that etl_bopm_data produces the expected merged dataframe"""

    #
    # Call the etl_bopm_data Function with Test Data
    #

    symbol = "MMM"
    result = etl_bopm_data(symbol, return_df=True)

    #
    # Verify that the function call was successful
    #

    assert result is not None, "ETL BOPM Empty DataFrame"

    #
    # Validate Schema
    #

    cols_expected = [
        "date",
        "expiration",
        "ttm",
        "midprice",
        "strike",
        "call_put",
        "act_symbol",
        "div_amt",
        "div_count",
        "close",
        "vol_estimate",
        "fred_rate",
    ]
    assert all(
        c in result.columns for c in cols_expected
    ), f"ETL BOPM DataFrame Missing Columns, Required: {cols_expected}"

    #
    # Validate that output contains data
    #

    assert result.count() > 0, "Empty ETL BOPM DataFrame"
