import os, dotenv;
import random;
import pandas as pd;
import numpy as np;
import yfinance as yf;
import QuantLib as ql;
import random;
from datetime import datetime, timedelta;
import matplotlib.pyplot as plt;
from scipy.optimize import brentq;

from fredapi import Fred;
from uvatradier import Account, Quotes, EquityOrder, OptionsOrder, OptionsData;
from nyse_sectors import nyse_sectors; # gives access to sector-sorted dictionary of NYSE symbols
from dow30 import DOW30;



import warnings;
warnings.filterwarnings('ignore');

dotenv.load_dotenv();

tradier_acct = os.getenv("tradier_acct");
tradier_token = os.getenv("tradier_token");

tradier_acct_live = os.getenv("tradier_acct_live");
tradier_token_live = os.getenv("tradier_token_live");

fred_api_key = os.getenv("fred_api_key");

acct = Account(tradier_acct, tradier_token);
quotes = Quotes(tradier_acct, tradier_token);
equity_order = EquityOrder(tradier_acct, tradier_token);
options_order = OptionsOrder(tradier_acct, tradier_token);
options_data = OptionsData(tradier_acct, tradier_token);

fred = Fred(api_key = fred_api_key);


today = datetime.today().strftime("%Y-%m-%d");

nyse_sector_names = list(nyse_sectors.keys());
