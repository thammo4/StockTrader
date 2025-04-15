#
# FILE: `StockTrader/utils/parse_occ.py`
#

import re
import requests
import pandas as pd
import numpy as np
from StockTrader.tradier import TRADIER_DIVIDEND_ENDPOINT, tradier_token_live
from StockTrader.settings import logger


#
# Parse String OCC Symbol and Separate Into Constituent Components in Dictionary (underlying, expiry, type, strike)
# Ex:
# 	SPY271217P00915000 -> {'root': 'SPY', 'expiry': {'year': 2027, 'month': 12, 'day': 17}, 'option_type': 'put', 'strike': 915.0}
#


def parse_occ(occ_symbol):
    m = re.fullmatch(r"([A-Za-z0-9]{1,6})" r"(\d{6})" r"([CP])" r"(\d{8})", occ_symbol)
    if not m:
        raise ValueError(f"Invalid occ: {occ_symbol}")

    root, expiry, option_type, strike = m.groups()

    try:
        return {
            "root": root,
            "expiry": {"year": int(f"20{int(expiry[:2])}"), "month": int(expiry[2:4]), "day": int(expiry[4:6])},
            "option_type": "call" if option_type == "C" else "put",
            "strike": int(strike) / 1000.0,
        }
    except ValueError as e:
        raise ValueError(f"Bad parse occ={occ_symbol}: {str(e)}")
