#
# FILE: `StockTrader/src/StockTrader/portfolio/main.py`
#

import argparse
import time

from StockTrader.tradier import acct, quotes
from StockTrader.settings import logger
from StockTrader.portfolio.position_loader import PositionLoader
from StockTrader.portfolio.position_quotes import PositionQuotes
from StockTrader.portfolio.m2m import M2MCalc


def snapshot(position_loader, position_quotes):
    positions = position_loader.load_options_positions()
    if positions.empty:
        logger.info("No open positions")
        return None

    enriched = position_quotes.add_market_data(positions)

    m2m = M2MCalc.compute_upl(enriched)

    print(f"Marked Positions:\n{m2m}")

    print(f"Summary:\n{pd.Series(M2MCalc.portfolio_summary())}")
