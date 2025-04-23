#
# FILE: `StockTrader/utils/dividend_table.py`
#

import requests
import pandas as pd
import numpy as np
from StockTrader.tradier import TRADIER_DIVIDEND_ENDPOINT, tradier_token_live
from StockTrader.settings import logger


#
# Call Tradier API to Retrieve Dividend Data
# This function is used to fetch dividend data for currently traded option contracts.
#


def dividend_table(symbol, start_date=None, end_date=None, current=False, ex_date_dtype="date"):

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
                logger.warning(f"API Response empty/invalid list symbol={symbol} [dividend_table]")
                return {}
            return r.json()[0]
        except requests.RequestException as e:
            logger.error(f"API Request failed symbol={symbol}: {str(e)} [dividend_table]")
        except (IndexError, ValueError, TypeError) as e:
            logger.error(f"API wtf symbol={symbol}: {str(e)} [dividend_table]")
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
                    if len(div_dict["results"]) > 1:
                        return div_dict["results"][1]["tables"].get("cash_dividends", [])
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"API Parse Error symbol={symbol}: {str(e)} [dividend_table]")
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
            df["ex_date"] = (
                pd.to_datetime(df["ex_date"]).dt.normalize()
                if ex_date_dtype == "date"
                else pd.to_datetime(df["ex_date"]).astype(np.int64)
            )
            return df
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Dividend DF Formatting Error: {str(e)} [dividend_table]")
        return pd.DataFrame(columns=["cash_amount", "ex_date", "frequency", "symbol"])

        #
        # Call Tradier Fundamentals Data Endpoint for Dividend Data, Extract/Format Response
        #

    dividend_dict = http_request_dividends(symbol)
    cash_dividends = validate_extract_cash_dividends(dividend_dict)
    df_dividends = create_format_cash_dividends(cash_dividends)

    if df_dividends.empty:
        print(f"No data symbol={symbol} [dividend_table]")
        return None

    if start_date:
        df_dividends = df_dividends.loc[df_dividends["ex_date"] > pd.Timestamp(start_date)]
    if end_date:
        df_dividends = df_dividends.loc[df_dividends["ex_date"] < pd.Timestamp(end_date)]

    if current:
        return float(df_dividends.iloc[0]["cash_amount"])

    return df_dividends
