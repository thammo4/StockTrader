import os, dotenv;
import random;
import pandas as pd;
import numpy as np;
import yfinance as yf;
import random;
from datetime import datetime, timedelta;
import matplotlib.pyplot as plt;
from scipy.optimize import brentq;

from uvatradier import Account, Quotes, EquityOrder, OptionsOrder, OptionsData;
from nyse_sectors import nyse_sectors; # gives access to sector-sorted dictionary of NYSE symbols

import warnings;
warnings.filterwarnings('ignore');

dotenv.load_dotenv();

tradier_acct = os.getenv("tradier_acct");
tradier_token = os.getenv("tradier_token");

acct = Account(tradier_acct, tradier_token);
quotes = Quotes(tradier_acct, tradier_token);
equity_order = EquityOrder(tradier_acct, tradier_token);
options_order = OptionsOrder(tradier_acct, tradier_token);
options_data = OptionsData(tradier_acct, tradier_token);


today = datetime.today().strftime("%Y-%m-%d");

nyse_sector_names = list(nyse_sectors.keys());
