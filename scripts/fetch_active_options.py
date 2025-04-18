#
# FILE: `StockTrader/scripts/fetch_active_options.py
#

import os
from datetime import datetime, timedelta
import pandas as pd
from utils.parse_occ import parse_occ
from StockTrader.tradier import options_data
from StockTrader.settings import STOCK_TRADER_MARKET_DATA, logger


#
# Retrieve and Standardize Schema to Match Dolt Options for Current Options Chain
#


def fetch_active_options(symbol, return_df=False, create_parquet=False, is_live=False):

    #
    # Reformat DataFrame Returned by Tradier to Match Schema of Historical Options from Dolt
    #

    def standardize_schema(df):
        df["act_symbol"] = symbol
        df["option_type"] = df["option_type"].str.capitalize()
        df["expiry_info"] = df["symbol"].apply(parse_occ)
        df["expiry_dt"] = df["expiry_info"].apply(
            lambda x: datetime(x["expiry"]["year"], x["expiry"]["month"], x["expiry"]["day"])
        )
        df["date"] = datetime.today()

        df["ttm"] = (df["expiry_dt"] - df["date"]).dt.days + 1
        df["midprice"] = 0.5 * (df["bid"] + df["ask"])

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
                "open_interest",
                "expiry_info",
            ],
            axis=1,
            inplace=True,
        )

        df.rename({"expiry_dt": "expiration", "option_type": "call_put"}, axis=1, inplace=True)
        df = df[["date", "expiration", "ttm", "midprice", "strike", "call_put", "act_symbol"]]

        return df

    try:
        df_options = options_data.get_chain_all(symbol)
        if df_options.empty:
            e_msg = f"No options data from Tradier symbol={symbol}"
            logger.error(e_msg)
            raise ValueError(e_msg)

        df_options = standardize_schema(df_options)
    except ValueError as e_val:
        logger.error(f"ValueError symbol={symbol}: {str(e_val)} [fetch_active_options]")
        raise
    except Exception as e:
        logger.error(f"Exception symbol={symbol}: {str(e)} [fetch_active_options]")
        raise

    if create_parquet:
        try:
            fname_parquet = f"{symbol}_{datetime.today().strftime('%d%B%Y')}.parquet"
            fpath_parquet = os.path.join(STOCK_TRADER_MARKET_DATA, "active", fname_parquet)

            df_options.to_parquet(fpath_parquet, index=False, engine="pyarrow")
            logger.info(f"Created parquet: {fpath_parquet} [fetch_active_options]")
        except Exception as e:
            logger.error(f"Exception symbol={symbol}: {str(e)} [fetch_active_options]")
            raise

    return df_options


#
# EXAMPLE - VMC
#

#
# Original DataFrame Returned By Tradier
#

# >>> options_data.get_chain_all("VMC")
#                 symbol    last  change  volume  open  high  low  close    bid     ask  strike  change_percentage  last_volume     trade_date  prevclose  bidsize bidexch       bid_date  asksize askexch       ask_date  open_interest option_type
# 0   VMC250516P00120000     NaN     NaN       0   NaN   NaN  NaN    NaN    0.0    1.95   120.0                NaN            0              0        NaN        0       A  1744919987000        5       I  1744919986000            0.0         put
# 1   VMC250516C00120000     NaN     NaN       0   NaN   NaN  NaN    NaN  120.1  125.00   120.0                NaN            0              0        NaN        5       A  1744919996000        5       S  1744919999000            0.0        call
# 2   VMC250516P00125000    0.35     0.0       0   NaN   NaN  NaN    NaN    0.0    1.35   125.0                0.0            1  1744205402728       0.35        0       Z  1744910987000       10       T  1744914716000            1.0         put
# 3   VMC250516C00125000     NaN     NaN       0   NaN   NaN  NaN    NaN  115.3  120.00   125.0                NaN            0              0        NaN        2       A  1744919996000        5       S  1744919999000            0.0        call
# 4   VMC250516P00130000    0.35     0.0       0   NaN   NaN  NaN    NaN    0.0    1.50   130.0                0.0            1  1744205402602       0.35        0       N  1744919986000        5       I  1744919986000            1.0         put
# ..                 ...     ...     ...     ...   ...   ...  ...    ...    ...     ...     ...                ...          ...            ...        ...      ...     ...            ...      ...     ...            ...            ...         ...
# 69  VMC251219C00380000     NaN     NaN       0   NaN   NaN  NaN    NaN    0.4    2.40   380.0                NaN            0              0        NaN        1       M  1744919402000       14       J  1744919189000            0.0        call
# 70  VMC251219P00390000     NaN     NaN       0   NaN   NaN  NaN    NaN  145.6  150.50   390.0                NaN            0              0        NaN        5       S  1744919996000        5       S  1744919993000            0.0         put
# 71  VMC251219C00390000    0.95     0.0       0   NaN   NaN  NaN    NaN    0.3    2.20   390.0                0.0            1  1743190896512       0.95        6       M  1744919596000       13       M  1744919549000            0.0        call
# 72  VMC251219P00400000  174.10     0.0       0   NaN   NaN  NaN    NaN  155.6  160.30   400.0                0.0            1  1744219174324     174.10        2       I  1744919993000        1       J  1744919994000            0.0         put
# 73  VMC251219C00400000     NaN     NaN       0   NaN   NaN  NaN    NaN    0.2    2.00   400.0                NaN            0              0        NaN        1       D  1744919402000        1       A  1744919403000            0.0        call
#
# [344 rows x 23 columns]


#
# Processing Performed by `fetch_active_options` on Tradier Returned DataFrame
#

# >>> import pandas as pd
# >>> from scripts.fetch_active_options import fetch_active_options
# >>> vmc = fetch_active_options("VMC", return_df=True, create_parquet=True)
# >>> vmc
#          date expiration  ttm  midprice  strike call_put act_symbol
# 0  2025-04-17 2025-05-16   29     0.975   120.0      Put        VMC
# 1  2025-04-17 2025-05-16   29   122.550   120.0     Call        VMC
# 2  2025-04-17 2025-05-16   29     0.675   125.0      Put        VMC
# 3  2025-04-17 2025-05-16   29   117.650   125.0     Call        VMC
# 4  2025-04-17 2025-05-16   29     0.750   130.0      Put        VMC
# ..        ...        ...  ...       ...     ...      ...        ...
# 69 2025-04-17 2025-12-19  246     1.400   380.0     Call        VMC
# 70 2025-04-17 2025-12-19  246   148.050   390.0      Put        VMC
# 71 2025-04-17 2025-12-19  246     1.250   390.0     Call        VMC
# 72 2025-04-17 2025-12-19  246   157.950   400.0      Put        VMC
# 73 2025-04-17 2025-12-19  246     1.100   400.0     Call        VMC
#
# [344 rows x 7 columns]
#
#
# >>> pd.read_parquet("data/active/VMC_17April2025.parquet")
#           date expiration  ttm  midprice  strike call_put act_symbol
# 0   2025-04-17 2025-05-16   29     0.975   120.0      Put        VMC
# 1   2025-04-17 2025-05-16   29   122.550   120.0     Call        VMC
# 2   2025-04-17 2025-05-16   29     0.675   125.0      Put        VMC
# 3   2025-04-17 2025-05-16   29   117.650   125.0     Call        VMC
# 4   2025-04-17 2025-05-16   29     0.750   130.0      Put        VMC
# ..         ...        ...  ...       ...     ...      ...        ...
# 339 2025-04-17 2025-12-19  246     1.400   380.0     Call        VMC
# 340 2025-04-17 2025-12-19  246   148.050   390.0      Put        VMC
# 341 2025-04-17 2025-12-19  246     1.250   390.0     Call        VMC
# 342 2025-04-17 2025-12-19  246   157.950   400.0      Put        VMC
# 343 2025-04-17 2025-12-19  246     1.100   400.0     Call        VMC
#
# [344 rows x 7 columns]
