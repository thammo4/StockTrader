#
# FILE: `StockTrader/scripts/price_active_options.py`
#
# This script performs pre-processing and pricing (NPV,Delta,Gamma,Theta,IV) for current options chain
#

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from StockTrader.settings import STOCK_TRADER_MARKET_DATA, logger
from StockTrader.tradier import quotes, quotesL
from scripts.fetch_active_options import fetch_active_options
from utils.dividend_table import dividend_table
from utils.vol_estimate import vol_estimate
from utils.AnalyzeOption import AnalyzeOption


def price_active_options(symbol, crr_steps=225, return_df=False, create_parquet=False, is_live=False):
    q = quotesL if is_live else quotes
    df_options = fetch_active_options(
        symbol=symbol, return_df=return_df, create_parquet=create_parquet, is_live=is_live
    )
    stock_price = q.get_quote_day(symbol=symbol, last_price=True)
    stock_dividend = dividend_table(symbol=symbol, current=True)
    stock_volatility = vol_estimate(symbol=symbol)
    try:
        # fred_data = pd.read_parquet("data/fred_data.parquet")
        fred_parquet = os.path.join(STOCK_TRADER_MARKET_DATA, "fred_data.parquet")
        fred_data = pd.read_parquet(fred_parquet)
        fred_rate = float(fred_data.iloc[-1]["fred_rate"])
    except Exception as e:
        from StockTrader.freddy import fred

        fred_data = fred.get_series(
            series_id="TB3MS", observation_start=(datetime.today() - timedelta(weeks=5)).strftime("%Y-%m-%d")
        )
        fred_rate = float(fred_data.iloc[-1])

    df_options["NPV"] = None
    df_options["Detla"] = None
    df_options["Gamma"] = None
    df_options["Theta"] = None

    ao = AnalyzeOption()

    for idx, row in df_options.iterrows():
        option_ql = ao.price_amr_bopm(
            S=stock_price,
            K=row.strike,
            q=stock_dividend,
            r=fred_rate,
            σ=stock_volatility,
            CP=row.call_put,
            T=row.expiration,
        )
        try:
            option_ql = ao.price_amr_bopm(
                S=stock_price,
                K=row["strike"],
                q=stock_dividend,
                r=fred_rate,
                σ=stock_volatility,
                CP=row["call_put"],
                T=row["expiration"],
            )
            df_options.loc[idx, ["NPV", "Delta", "Gamma", "Theta"]] = np.round(
                [option_ql.NPV(), option_ql.delta(), option_ql.gamma(), option_ql.theta()], 4
            )
        except Exception as e:
            print(f"{str(e)}")


if __name__ == "__main__":
    price_active_options("DD")
