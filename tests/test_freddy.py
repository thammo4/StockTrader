#
# FILE: `StockTrader/tests/test_freddy.py`
#

import os
import importlib
import pytest
from unittest.mock import patch

@pytest.fixture(autouse=True)
def mock_env ():
	with patch.dict(os.environ, {"fred_api_key":"mock_fred_api_key"}):
		yield

def test_fred_import ():
	'''Test that FRED API client instantiation ok'''
	from StockTrader import freddy
	importlib.reload(freddy)

	assert freddy.fred_api_key == "mock_fred_api_key"
	assert freddy.fred is not None
