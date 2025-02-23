#
# FILE: `StockTrader/tests/scripts/test_dolt_historical_ticker.py` (test: `StockTrader/scripts/dolt_historical_ticker.sh`)
# 	• Confirm that the script runs (e.g. is ok to download historical options chain data from DoltHub)
# 	• Confirm that the script runs with ticker symbol argument
#

import os
import subprocess
import pytest

from ..config import logger

STOCK_TRADER_MARKET_DATA = os.environ.get("STOCK_TRADER_MARKET_DATA", os.path.join(os.getcwd(), "data"))
SCRIPT_PATH = os.path.abspath("scripts/dolt_historical_ticker.sh")

if not os.path.exists(STOCK_TRADER_MARKET_DATA):
	os.makedirs(STOCK_TRADER_MARKET_DATA, exist_ok=True)

@pytest.fixture(scope="function", autouse=True)
def setup_teardown():
	os.makedirs(STOCK_TRADER_MARKET_DATA, exist_ok=True)
	created_files = []
	yield created_files

def test_script_exec ():
	ticker = "KO"
	result = subprocess.run(
		[SCRIPT_PATH, ticker],
		stdout=subprocess.PIPE,
		stderr=subprocess.PIPE,
		text=True,
		env={"STOCK_TRADER_MARKET_DATA":STOCK_TRADER_MARKET_DATA, "PATH":os.environ['PATH']},
	)
	assert result.returncode == 0, f"Script failed: {result.stderr}"


def test_usage_ok ():
	result = subprocess.run(
		[SCRIPT_PATH],
		stdout=subprocess.PIPE,
		stderr=subprocess.PIPE,
		text=True,
		env={"STOCK_TRADER_MARKET_DATA":STOCK_TRADER_MARKET_DATA}
	)
	assert result.returncode != 0
	assert "Usage:" in result.stdout

def cleanup_test_file ():
	global TEST_FILE_PATH
	yield

	if TEST_FILE_PATH and os.path.exists(TEST_FILE_PATH):
		subprocess.run(["rm", "-f", TEST_FILE_PATH])


