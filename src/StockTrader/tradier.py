#
# FILE: `StockTrader/src/StockTrader/tradier.py`
# Instantiates classes from uvatradier Tradier API Client Library
#

import os
import dotenv
from uvatradier import Account, Quotes, EquityOrder, OptionsOrder, OptionsData, Stream
from StockTrader.settings import STOCK_TRADER_HOME

#
# Define API Keys Using Local .env File
#

env_path = os.path.join(STOCK_TRADER_HOME, ".env")
dotenv.load_dotenv(env_path)

tradier_acct = os.getenv("tradier_acct")
tradier_token = os.getenv("tradier_token")

tradier_acct_live = os.getenv("tradier_acct_live")
tradier_token_live = os.getenv("tradier_token_live")

acct = Account(tradier_acct, tradier_token)
quotes = Quotes(tradier_acct, tradier_token)
equity_order = EquityOrder(tradier_acct, tradier_token)
options_order = OptionsOrder(tradier_acct, tradier_token)
options_data = OptionsData(tradier_acct, tradier_token)

acctL = Account(tradier_acct_live, tradier_token_live, True)
quotesL = Quotes(tradier_acct_live, tradier_token_live, True)
equity_orderL = EquityOrder(tradier_acct_live, tradier_token_live, True)
options_orderL = OptionsOrder(tradier_acct_live, tradier_token_live, True)
options_dataL = OptionsData(tradier_acct_live, tradier_token_live, True)
streamL = Stream(tradier_acct_live, tradier_token_live, True)
