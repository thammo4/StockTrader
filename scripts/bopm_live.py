#
# FILE: `StockTrader/scripts/bopm_live.py`
#
# This script performs pre-processing and pricing (NPV,Delta,Gamma,Theta,IV) for current options chain
#


from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from utils.dividend_table import dividend_table
from utils.parse_occ import parse_occ
from StockTrader.freddy import fred
from StockTrader.tradier import options_data, quotes, quotesL
from StockTrader.settings import STOCK_TRADER_MARKET_DATA, logger


def bopm_live(symbol, return_df=False, create_parquet=False, is_live=False):

    #
    # Retrieve and Standardize Schema for Current Options Chain
    #

    def get_standard_options(symbol):
        df = options_data.get_chain_all(symbol)
        df["expiry_info"] = df["symbol"].apply(parse_occ)
        df["expiry_dt"] = df["expiry_info"].apply(
            lambda x: datetime(x["expiry"]["year"], x["expiry"]["month"], x["expiry"]["day"])
        )
        df["date"] = datetime.today()
        df["midprice"] = 0.50 * (df["ask"] + df["bid"])
        df["act_symbol"] = symbol
        df["ttm"] = (df["expiry_dt"] - df["date"]).dt.days + 1
        df["date"] = df["date"].dt.normalize()
        df.drop(
            [
                "symbol",
                "last",
                "volume",
                "change",
                "open",
                "high",
                "low",
                "close",
                "bid",
                "ask",
                "change_percentage",
                "last_volume",
                "trade_date",
                "prevclose",
                "bidsize",
                "bidexch",
                "bid_date",
                "asksize",
                "askexch",
                "ask_date",
            ],
            axis=1,
            inplace=True,
        )
        df.rename({"expiry_dt": "expiration", "option_type": "call_put"}, axis=1, inplace=True)
        df = df[["date", "expiration", "ttm", "midprice", "strike", "call_put", "act_symbol"]]
        return df

    #
    # Price Option Contract in the Chain
    #

    def price_option(row, expiry_date, settlement_date):
        pass

    #
    # Retrieve Closing Prices (Volatility), Recent Dividend and Fred Rate
    # These should be global variables because they will be common values to every contract in the options chain
    #

    q = quotesL if is_live else quotes
    df_ohlcv = q.get_historical_quotes(
        symbol=symbol, start_date=(datetime.today() - timedelta(weeks=2)).strftime("%Y-%m-%d")
    )
    df_dividends = dividend_table(
        symbol=symbol, start_date=(datetime.today() - timedelta(weeks=16)).strftime("%Y-%m-%d")
    )
    try:
        fred_data = pd.read_parquet("data/fred_data.parquet")
        fred_rate = fred_data.iloc[0]["fred_rate"]
    except Exception as e:
        fred_data = fred.get_series(
            series_id=series, observation_start=(datetime.today() - timedelta(weeks=5)).strftime("%Y-%m-%d")
        )
        fred_rate = float(fred_data.iloc[0])

    #
    # Iterate over Each Contract in Options Chain and Compute NPV, Greeks, IV
    #

    df_options = get_standard_options(symbol)

    return df_options
