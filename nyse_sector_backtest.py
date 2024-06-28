import os, dotenv;
import random;
import pandas as pd;
import numpy as np;
import yfinance as yf;
import random;
from datetime import datetime, timedelta;
import matplotlib.pyplot as plt;

from uvatradier import Account, Quotes, EquityOrder;
from nyse_sectors import nyse_sectors; # gives access to sector-sorted dictionary of NYSE symbols

import warnings;
warnings.filterwarnings('ignore');

dotenv.load_dotenv();

tradier_acct = os.getenv("tradier_acct");
tradier_token = os.getenv("tradier_token");

acct = Account(tradier_acct, tradier_token);
quotes = Quotes(tradier_acct, tradier_token);
equity_order = EquityOrder(tradier_acct, tradier_token);

today = datetime.today().strftime("%Y-%m-%d");

nyse_sector_names = list(nyse_sectors.keys());

#
# Retrieve adjusted closing prices for given sector_symbols within the user-supplied date range
#

def get_backtest_data (start_date, end_date, window_size, sector_symbols):
	backtest_start_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=window_size)).strftime('%Y-%m-%d');

	try:
		data = yf.download(sector_symbols, start=backtest_start_date, end=end_date)['Adj Close'];
		return data;
	except Exception as e:
		print(f'Failed: {e}');
		return pd.DataFrame();


#
# Collect adjusted closing price data for several sectors
#

def gather_sector_data (start_date, end_date, window_size, sectors):
	all_sector_data = {};
	for sector, symbols in sectors.items():
		print(f'Retrieving data for: {sector}');
		sector_data = get_backtest_data(start_date, end_date, window_size, symbols);
		all_sector_data[sector] = sector_data;
	return all_sector_data;


#
# Compute intra-sector Z-scores of log-returns
#

def nyse_sector_Zreturns (sector_data):
	stocks_daily_returns = np.log(sector_data / sector_data.shift(1));

	stocks_agg_mean_returns = stocks_daily_returns.mean();
	stocks_agg_std_returns = stocks_daily_returns.std();

	sector_mean_return = stocks_agg_mean_returns.mean();
	sector_std_return = stocks_agg_std_returns.mean();
	sector_volatility = stocks_agg_std_returns.std();

	stocks_Zreturns = (stocks_agg_mean_returns - sector_mean_return) / sector_std_return;

	return stocks_Zreturns;


#
# Backtest Sector-based log-returns z-score strategy
#

def backtest_sector_strategy (start_date, end_date, window_size, backtest_data):
	results = {};
	current_date 	= pd.to_datetime(start_date);
	end_date 		= pd.to_datetime(end_date);

	while current_date <= end_date:
		backtest_end 	= current_date.strftime("%Y-%m-%d");
		backtest_start 	= (current_date-timedelta(days=window_size)).strftime("%Y-%m-%d");

		print(f"DATES: {backtest_start} - {backtest_end}");

		period_results = {sector: [] for sector in backtest_data.keys()};

		for sector, data in backtest_data.items():
			print(f"SECTOR: {sector}");

			sector_data = data[ (data.index >= backtest_start) & (data.index <= backtest_end)];
			sector_Zscores = nyse_sector_Zreturns(sector_data);

			for ticker, zscore in sector_Zscores.items():
				if zscore > .6745:
					period_results[sector].append(ticker);

			results[backtest_end] = period_results;
			print(f"PERIOD RESULTS:\n{period_results}");

		current_date += timedelta(days=1);
		print(f'RESULTS:\n{results}');
		print('\n-------------\n');

	return results;


#
# Helper function to directly update portfolio dictionary
#

def update_portfolio_value (portfolio, date, backtest_data):
	total_value = portfolio['cash'];
	for ticker, (shares, last_price) in portfolio['holdings'].items():
		if ticker in backtest_data and date in backtest_data[ticker].index:
			current_price = backtest_data[ticker].loc[date];
			portfolio['holdings'][ticker] = (shares, current_price);
			total_value += shares*current_price;
		else:
			total_value += shares*last_price;

	portfolio['total_value'] = total_value;


#
# Compute returns accumulated from log-returns z-score sector strategy
#

def backtest_sector_returns (backtest_results, backtest_data, cash_initial, window_size):
	portfolio = {
		'cash': cash_initial,
		'holdings': {},
		'total_value': cash_initial
	};
	portfolio_history = [];

	for date, sectors in backtest_results.items():
		print(f"(DATE, SECTOR): ({date}, {sectors})");
		date_obj = datetime.strptime(date, "%Y-%m-%d");
		end_date = (date_obj + timedelta(days=window_size)).strftime("%Y-%m-%d");


		# record portfolio value at day's start
		update_portfolio_value(portfolio, date, backtest_data);

		current_tickers = [ticker for sector_tickers in sectors.values() for ticker in sector_tickers];

		# remove holdings not in current tickers
		portfolio['holdings'] = {ticker: holdings for ticker, holdings in portfolio['holdings'].items() if ticker in current_tickers};

		#
		# Track performance from trades
		#
		for sector, tickers in sectors.items():
			print(f"TRYING (SECTOR, TICKERS): ({sector}, {tickers})")
			try:
				sector_data = backtest_data[sector][(backtest_data[sector].index >= date) & (backtest_data[sector].index <= end_date)]

				for ticker in tickers:
					if ticker in sector_data.columns:
						start_price = sector_data[ticker].iloc[0]
						max_spend_per_trade = 1000  # Maximum $1000 to spend per trade
						shares_to_buy = int(max_spend_per_trade / start_price)  # Calculate number of shares to buy for $1000

						amount_invested = shares_to_buy * start_price

						if amount_invested <= portfolio['cash'] and shares_to_buy > 0:  # Check sufficient cash and that we can buy at least one share
							if ticker in portfolio['holdings']:
								current_shares, _ = portfolio['holdings'][ticker]
								portfolio['holdings'][ticker] = (current_shares + shares_to_buy, start_price)
							else:
								portfolio['holdings'][ticker] = (shares_to_buy, start_price)
							portfolio['cash'] -= amount_invested
						else:
							print(f"Not enough cash to invest $1000: {ticker} [{sector}]")
			except Exception as e:
				print(f"ERROR\t{sector}: {e}")

		update_portfolio_value(portfolio, date, backtest_data);
		portfolio_history.append((end_date, portfolio['total_value']));
		print(f'PORTFOLIO\n{portfolio}');
		print('-------------------\n\n');

	return portfolio, portfolio_history;


backtest_start = "2023-01-01";
backtest_end = "2023-03-01";
cash_start = 100000;
w_size = 90;

btest_data 								= gather_sector_data(start_date=backtest_start, end_date=backtest_end, window_size=w_size, sectors=nyse_sectors);
btest_results 							= backtest_sector_strategy(start_date=backtest_start, end_date=backtest_end, window_size=w_size, backtest_data=btest_data);
portfolio_final, portfolio_history 		= backtest_sector_returns(backtest_results=btest_results, backtest_data=btest_data, cash_initial=cash_start, window_size=w_size);

daily_returns = [(portfolio_history[i+1][1] - portfolio_history[i][1])/portfolio_history[i][1] for i in range(len(portfolio_history)-1)];


#
# Analyze Trade Results
#

cumulative_returns = np.cumprod(1+np.array(daily_returns));

print(f"Initial Portfolio Value: ${cash_start:,.2f}");
print(f"Final Portfolio Value: ${portfolio_final['total_value']:,.2f}");
print(f"Total Return: {(portfolio_final['total_value']/cash_start - 1)*100:.2f}%");
print(f"Sharpe Ratio: {np.mean(daily_returns)/np.std(daily_returns)*np.sqrt(252):.2f}");

#
# Plot Cumulative Returns
#

# cumulative_returns = [1+ret for date in returns for sector in returns[date] for ret in returns[date][sector].values()];
# cumulative_returns = np.cumprod(cumulative_returns);

plt.figure(figsize=(12,6));
plt.plot(cumulative_returns);
plt.title(f'Cumulative Returns {backtest_start} - {backtest_end}');
plt.xlabel('Trades');
plt.ylabel('Cumulative Returns');
plt.show();