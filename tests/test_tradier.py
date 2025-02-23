#
# FILE: `StockTrader/tests/test_tradier.py`
#

import os
import importlib
import pytest
from unittest.mock import patch


@pytest.fixture(autouse=True)
def mock_env():
    with patch.dict(
        os.environ,
        {
            "tradier_acct": "mock_acct",
            "tradier_token": "mock_token",
            "tradier_acct_live": "mock_acct_live",
            "tradier_token_live": "mock_token_live",
        },
    ):
        yield


def test_tradier_import():
    """Test that uvatradier Tradier Client Library variables ok"""
    from StockTrader import tradier

    importlib.reload(tradier)

    assert tradier.tradier_acct == "mock_acct"
    assert tradier.tradier_token == "mock_token"
    assert tradier.tradier_acct_live == "mock_acct_live"
    assert tradier.tradier_token_live == "mock_token_live"

    assert tradier.acct is not None
    assert tradier.quotes is not None
    assert tradier.equity_order is not None
    assert tradier.options_order is not None
    assert tradier.options_data is not None

    assert tradier.acctL is not None
    assert tradier.quotesL is not None
    assert tradier.equity_orderL is not None
    assert tradier.options_orderL is not None
    assert tradier.options_dataL is not None
    assert tradier.streamL is not None
