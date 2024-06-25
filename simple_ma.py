import os, dotenv;
import random;
import pandas as pd;
import numpy as np;

from uvatradier import Account, Quotes, EquityOrder;

import warnings;
warnings.filterwarnings('ignore');

dotenv.load_dotenv();

tradier_acct = os.getenv("tradier_acct");
tradier_token = os.getenv("tradier_token");

acct = Account(tradier_acct, tradier_token);
quotes = Quotes(tradier_acct, tradier_token);
equity_order = EquityOrder(tradier_acct, tradier_token);


#
# Define Dow30 Stock Ticker Symbols for Testing
#

DOW30 = ['AMZN', 'AXP', 'AMGN', 'AAPL', 'BA', 'CAT', 'CSCO', 'CVX', 'GS', 'HD', 'HON', 'IBM', 'INTC', 'JNJ', 'KO', 'JPM', 'MCD', 'MMM', 'MRK', 'MSFT', 'NKE', 'PG', 'TRV', 'UNH', 'CRM', 'VZ', 'V', 'WMT', 'DIS', 'DOW'];


#
# Simple Moving Average Strategy
#

def sma_strategy(symbol, w_short, w_long):
	total_days = int(w_long + .25*w_long);

	#
	# Determine starting/ending dates
	#

	end_date = str(pd.Timestamp.today().strftime("%Y-%m-%d"));
	start_date = str((pd.Timestamp.today()-pd.Timedelta(days=total_days)).strftime("%Y-%m-%d"));

	# print(end_date);
	# print(start_date);

	#
	# Retrieve past bar data
	#

	past_data = quotes.get_historical_quotes(symbol, start_date=start_date, end_date=end_date);
	# print(past_data);


	#
	# Compute Short, Long Moving Averages
	#

	past_data['sma_short'] = past_data['close'].rolling(window=w_short).mean();
	past_data['sma_long'] = past_data['close'].rolling(window=w_long).mean();


	#
	# Define SMA Xover Trading Signals
	#

	past_data['trade_signal'] = np.where(past_data['sma_short'] > past_data['sma_long'], 1, 0);
	past_data['position'] = past_data['trade_signal'].diff();


	#
	# Determine if we currently own stock
	#

	current_positions = acct.get_positions();
	stock_owned = current_positions[current_positions['symbol'] == symbol];

	#
	# Implement buy/sell signal logic
	#

	print(f'SMA for {symbol}');

	stock_price = quotes.get_quote_day(symbol, last_price=True);

	if past_data['position'].iloc[-1] == 1:
		equity_order.order(symbol=symbol, side='buy', quantity=10, order_type='limit', limit_price=stock_price);
		print(f'Bought {symbol} at {stock_price}');
	elif past_data['position'].iloc[-1] == -1 and not stock_owned.empty:
		quantity_owned = float(stock_owned['quantity']);
		equity_order(symbol=symbol, side='sell', quantity=float(stock_owned['quantity']), order_type='limit', limit_price=stock_price);
		print(f'Sold {symbol} at {stock_price}');
	else:
		print(f'SMA condintion unment for {symbol}');



#
# Run Example on Dow30 Sample
#

dow30_sample = random.sample(DOW30, 5);
[sma_strategy(x, 5, 30) for x in dow30_sample];