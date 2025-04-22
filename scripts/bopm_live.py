#
# FILE: `StockTrader/scripts/bopm_live.py`
#
# This script performs pre-processing and pricing (NPV,Delta,Gamma,Theta,IV) for current options chain
#

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from utils.dividend_table import dividend_table
from utils.parse_occ import parse_occ
from scripts.fetch_active_options import fetch_active_options
from StockTrader.freddy import fred
from StockTrader.tradier import options_data, quotes, quotesL
from StockTrader.settings import STOCK_TRADER_MARKET_DATA, logger


def bopm_live(symbol, return_df=False, create_parquet=False, is_live=False):

    #
    # Estimate the annualized historical rolling volatility
    #

    def vol_estimate(quotes_obj, weeks=2, window_days=9):
        df = quotes_obj.get_historical_quotes(
            symbol=symbol, start_date=(datetime.today() - timedelta(weeks=weeks)).strftime("%Y-%m-%d")
        )
        df["log_return"] = np.log(df["close"]).diff()
        df["vol_estimate"] = df["log_return"].rolling(window=window_days).std() * np.sqrt(252)
        return float(df.iloc[-1]["vol_estimate"])

    #
    # Price Option Contract in the Chain
    #

    def price_option(row, expiry_date, settlement_date):
        pass

    #
    # Retrieve Closing Prices (Volatility), Recent Dividend and Fred Rate
    # These should be global variables because they will be common values to every contract in the options chain
    #

    # fetch_active_options(symbol, return_df=False, create_parquet=False, is_live=False)
    # def vol_estimate (quotes_obj, weeks=2, window_days=9):
    # get_quote_day(symbol, last_price=False) method of uvatradier.quotes.Quotes instance
    # dividend_table(symbol, start_date=None, end_date=None, current=False, ex_date_dtype='date')

    q = quotesL if is_live else quotes

    df_options = fetch_active_options(
        symbol=symbol, return_df=True
    )  # this can later be replaced with a local parquet read
    stock_price = quotes.get_quote_day(symbol=symbol, last_price=True)
    stock_dividend = dividend_table(symbol=symbol, current=True)
    stock_volatility = vol_estimate(q)

    try:
        fred_data = pd.read_parquet("data/fred_data.parquet")
        fred_rate = fred_data.iloc[-1]["fred_rate"]
    except Exception as e:
        fred_data = fred.get_series(
            series_id=series, observation_start=(datetime.today() - timedelta(weeks=5)).strftime("%Y-%m-%d")
        )
        fred_rate = float(fred_data.iloc[0])

    print(f"OPTIONS:\n{df_options}")
    print(f"PRICE: {stock_price}")
    print(f"DIVIDEND: {stock_dividend}")
    print(f"VOL: {stock_volatility}")
    print(f"FRED: {fred_rate}")

    #
    # Iterate over Each Contract in Options Chain and Compute NPV, Greeks, IV
    #

    # df_options = get_standard_options(symbol)

    # return df_options


if __name__ == "__main__":
    idk = bopm_live("WY")
