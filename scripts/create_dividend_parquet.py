#
# FILE: `StockTrader/scripts/create_dividend_parquet.py`
#

import os
import requests
import pandas as pd
import numpy as np
from StockTrader.settings import STOCK_TRADER_MARKET_DATA, logger
from StockTrader.tradier import TRADIER_DIVIDEND_ENDPOINT, tradier_token_live


#
# Call Tradier API to Retrieve Dividend Data
#


def create_dividend_parquet(symbol, return_df=False):

    #
    # Call Tradier Fundamentals (Dividend) Endpoint
    #

    def http_request_dividends(symbol):
        try:
            r = requests.get(
                url=TRADIER_DIVIDEND_ENDPOINT,
                params={"symbols": symbol},
                headers={"Authorization": f"Bearer {tradier_token_live}", "Accept": "application/json"},
            )
            r.raise_for_status()

            if not r.json() or not isinstance(r.json(), list) or len(r.json()) == 0:
                logger.warning(f"API Response empty/not list, symbol={symbol} [create_dividend_parquet]")
                return {}
            return r.json()[0]
        except requests.RequestException as e:
            logger.error(f"API Request Failed: {e} [create_dividend_parquet]")
        except (IndexError, ValueError, TypeError) as e:
            logger.error(f"API wtf: {e} [create_dividend_parquet]")

        return {}

    #
    # Extract Cash Dividend Data from Tradier Response
    #

    def validate_extract_cash_dividends(div_dict):
        try:
            if "results" in div_dict and isinstance(div_dict["results"], list) and len(div_dict["results"]) > 0:
                div_data = div_dict["results"][0]

                if "tables" in div_data and "cash_dividends" in div_data["tables"]:
                    if div_data["tables"]["cash_dividends"] is not None:
                        return div_data["tables"]["cash_dividends"]

                    # Use second 'results' entry if first one is empty (which inexplicably happens, sometimes)
                    if len(div_dict["results"]) > 1:
                        return div_dict["results"][1]["tables"].get("cash_dividends", [])
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"API Parse Error: {e} [create_dividend_parquet]")

        return []

    #
    # Reformat Cash Dividend Data as DataFrame
    #

    def create_format_cash_dividends(div_list):
        try:
            if not div_list:
                return pd.DataFrame(columns=["cash_amount", "ex_date", "frequency", "symbol"])
            df = pd.json_normalize(div_list)
            df = pd.DataFrame(df, columns=["cash_amount", "ex_date", "frequency"])
            df["symbol"] = symbol
            df["ex_date"] = pd.to_datetime(df["ex_date"]).astype(np.int64)
            return df
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Dividend DF Formatting Error: {e} [create_dividend_parquet]")
        return pd.DataFrame(columns=["cash_amount", "ex_date", "frequency", "symbol"])

    #
    # Call Tradier Fundamentals Data Endpoint for Dividend Data, Extract/Format Response
    #

    logger.info(f"Creating dividend parquet: symbol={symbol} [create_dividend_parquet]")

    dividend_dict = http_request_dividends(symbol)
    cash_dividends = validate_extract_cash_dividends(dividend_dict)
    df_dividends = create_format_cash_dividends(cash_dividends)

    #
    # Fail Safe In Case \not\exists Dividend Data
    #

    if df_dividends.empty:
        logger.warning(f"No data, symbol={symbol} [create_dividend_parquet]")
        lacks_dividends_marker = os.path.join(STOCK_TRADER_MARKET_DATA, f"{symbol}_lacks_dividends.txt")
        with open(lacks_dividends_marker, "w") as f:
            f.write(f"{symbol} does not pay dividends\n")
        logger.info(f"Created lacks dividends marker file: {lacks_dividends_marker}")
        return None

    #
    # Create local parquet file
    #

    fpath_parquet = os.path.join(STOCK_TRADER_MARKET_DATA, f"{symbol}_dividend_data.parquet")
    try:
        df_dividends.to_parquet(fpath_parquet)
        logger.info(f"Created parquet file: {fpath_parquet} [create_dividend_parquet]")
    except Exception as e:
        logger.error(f"Parquet failed: {e} [create_dividend_parquet]")

    return df_dividends if return_df else None
