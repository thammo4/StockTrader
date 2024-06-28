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

nyse_sector_names.remove('Unknown');

nyse_sector_names = random.sample(nyse_sector_names, 4);


#
# Backtest Sector-based log-returns z-score strategy
#

def backtest_sector_strategy (start_date, end_date, window_size):
	from nyse_analysis import window_to_dates, get_sector_adjClose, nyse_sector_Zreturns;
	results = {};
	current_date = datetime.strptime(start_date, "%Y-%m-%d");
	end_date = datetime.strptime(end_date, "%Y-%m-%d");

	while current_date <= end_date:
		test_end = current_date.strftime("%Y-%m-%d");
		test_start = (current_date - timedelta(days=window_size)).strftime("%Y-%m-%d");
		print(f"Dates: {test_start} - {test_end}")

		period_results = {sector: [] for sector in nyse_sector_names};

		for nyse_sector in nyse_sector_names:
			print(f'SECTOR: {nyse_sector}');
			sector_data = get_sector_adjClose(sector=nyse_sector, start_date=test_start, end_date=test_end);
			sector_Zscores = nyse_sector_Zreturns(sector_data);

			for ticker, zscore in sector_Zscores.items():
				if zscore > .6745:
					period_results[nyse_sector].append(ticker);

			results[test_end] = period_results;
			current_date += timedelta(days=1);
			print(f"Period Results:\n{period_results}\n");
		print(f"RESULTS:\n{results}\n");
		print("\n");
		print("------------------");
		print("\n");

	return results;


#
# Helper function to directly update portfolio dictionary
#

def update_portfolio_value (portfolio, date):
	total_value = portfolio['cash'];
	for ticker, (shares, last_price) in portfolio['holdings'].items():
		total_value += shares*last_price;
	portfolio['total_value'] = total_value;

#
# Compute returns accumulated from log-returns z-score sector strategy
#

# def backtest_sector_returns (backtest_results, window_size):
# 	portfolio = {};
# 	returns = {};
# 	for date, sectors in backtest_results.items():
# 		date_obj = datetime.strptime(date, "%Y-%m-%d");
# 		end_date = (date_obj + timedelta(days=window_size)).strftime("%Y-%m-%d");

# 		for sector, tickers in sectors.items():
# 			for ticker in tickers:
# 				try:
# 					stock_data = yf.download(ticker, start=date, end=end_date);
# 					if not stock_data.empty:
# 						start_price = stock_data['Adj Close'].iloc[0];
# 						end_price = stock_data['Adj Close'].iloc[-1];
# 						returns.setdefault(date, {}).setdefault(sector, {})[ticker] = (end_price-start_price)/start_price;
# 				except Exception as e:
# 					print(f"Error computing return for {ticker}: {e}");

# 	return returns;

def backtest_sector_returns(backtest_results, cash_initial, window_size):
	from nyse_analysis import get_sector_adjClose;
	portfolio = {'cash':cash_initial, 'holdings':{}, 'total_value':cash_initial};
	portfolio_history = [];

	for date, sectors in backtest_results.items():
		print(f'(DATE, SECTOR): ({date},{sectors})');
		date_obj = datetime.strptime(date, "%Y-%m-%d");
		end_date = (date_obj + timedelta(days=window_size)).strftime("%Y-%m-%d");

		#
		# Record portfolio value at start of day
		#

		update_portfolio_value(portfolio, date);
		# portfolio_history.append((date, portfolio['total_value']));


		#
		# Track performance from trades
		#

		for sector, tickers in sectors.items():
			print(f'TRYING (SECTOR, TICKERS): ({sector}, {tickers})')
			try:
				sector_data = get_sector_adjClose(sector, start_date=date, end_date=end_date);

				for ticker in tickers:
					if ticker in sector_data.columns:
						start_price = sector_data[ticker].iloc[0];
						amount_invested = min(portfolio['cash'], 1000);
						if amount_invested > 0:
							num_shares = amount_invested / start_price;
							portfolio['holdings'][ticker] = (num_shares, start_price);
							portfolio['cash'] -= amount_invested;
						else:
							print(f"Nothing for {ticker} in {sector}");
			except Exception as e:
				print(f"ERROR: {sector} - {e}");

		update_portfolio_value(portfolio, date);

		portfolio_history.append((end_date, portfolio['total_value']));
		print(f'PORTFOLIO:\n{portfolio}');
		print('------');
		print('\n\n\n\n\n');

	return portfolio, portfolio_history;



backtest_start = "2023-01-01";
backtest_end = "2023-06-30";
cash_start = 100000;

# backtest_results = backtest_sector_strategy(backtest_start, backtest_end, window_size=90);
backtest_results = {'2023-01-01': {'Real Estate': [], 'Energy': ['NINE'], 'Healthcare': [], 'Industrials': []}, '2023-01-05': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': ['VATE']}, '2023-01-09': {'Real Estate': [], 'Energy': ['NINE'], 'Healthcare': [], 'Industrials': ['VATE']}, '2023-01-13': {'Real Estate': ['NYC'], 'Energy': ['NINE'], 'Healthcare': [], 'Industrials': []}, '2023-01-17': {'Real Estate': ['NYC'], 'Energy': ['NINE'], 'Healthcare': [], 'Industrials': ['VATE']}, '2023-01-21': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-01-25': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': ['VATE']}, '2023-01-29': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': ['VATE']}, '2023-02-02': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': ['VATE']}, '2023-02-06': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': ['VATE']}, '2023-02-10': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': ['VATE']}, '2023-02-14': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': ['VATE']}, '2023-02-18': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': ['VATE']}, '2023-02-22': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-02-26': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-03-02': {'Real Estate': ['NYC'], 'Energy': ['NGL'], 'Healthcare': [], 'Industrials': []}, '2023-03-06': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-03-10': {'Real Estate': ['NYC'], 'Energy': ['NGL'], 'Healthcare': [], 'Industrials': []}, '2023-03-14': {'Real Estate': ['NYC'], 'Energy': ['NGL'], 'Healthcare': [], 'Industrials': []}, '2023-03-18': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-03-22': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-03-26': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-03-30': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-04-03': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': ['CR']}, '2023-04-07': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': ['CR']}, '2023-04-11': {'Real Estate': ['NYC'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-04-15': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-04-19': {'Real Estate': ['PKST'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-04-23': {'Real Estate': ['PKST'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-04-27': {'Real Estate': ['PKST'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-05-01': {'Real Estate': ['PKST'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-05-05': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-05-09': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-05-13': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-05-17': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-05-21': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-05-25': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-05-29': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-06-02': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': ['ARLO']}, '2023-06-06': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-06-10': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-06-14': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-06-18': {'Real Estate': ['PKST'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-06-22': {'Real Estate': ['PKST'], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-06-26': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}, '2023-06-30': {'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []}};
portfolio_final, portfolio_history = backtest_sector_returns(backtest_results, cash_start, window_size=90);

daily_returns = [(portfolio_history[i+1][1] - portfolio_history[i][1])/portfolio_history[i][1] for i in range(len(portfolio_history)-1)];
cumulative_returns = np.cumprod(1+np.array(daily_returns));



#
# Analyze Trade Results
#

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



#
# OUTPUT
#

# (DATE, SECTOR): (2023-06-30,{'Real Estate': [], 'Energy': [], 'Healthcare': [], 'Industrials': []})
# TRYING (SECTOR, TICKERS): (Real Estate, [])
# [*********************100%%**********************]  186 of 186 completed

# 4 Failed downloads:
# ['AHR', 'SDHC', 'SILA', 'NLOP']: Exception("%ticker%: Data doesn't exist for startDate = 1688097600, endDate = 1695873600")
# TRYING (SECTOR, TICKERS): (Energy, [])
# [*********************100%%**********************]  178 of 178 completed

# 2 Failed downloads:
# ['MNR', 'DEC']: Exception("%ticker%: Data doesn't exist for startDate = 1688097600, endDate = 1695873600")
# TRYING (SECTOR, TICKERS): (Healthcare, [])
# [*********************100%%**********************]  118 of 118 completed

# 4 Failed downloads:
# ['SOLV', 'ANRO', 'AUNA', 'PACS']: Exception("%ticker%: Data doesn't exist for startDate = 1688097600, endDate = 1695873600")
# TRYING (SECTOR, TICKERS): (Industrials, [])
# [*********************100%%**********************]  309 of 309 completed

# 7 Failed downloads:
# ['ULS', 'CDLR', 'VLTO', 'HAFN', 'ECO', 'VSTS', 'LOAR']: Exception("%ticker%: Data doesn't exist for startDate = 1688097600, endDate = 1695873600")
# PORTFOLIO:
# {'cash': 51000, 'holdings': {'NINE': (66.62225081621432, 15.010000228881836), 'VATE': (336.7003334568775, 2.9700000286102295), 'NYC': (81.23476571182256, 12.3100004196167), 'NGL': (298.507471184445, 3.3499999046325684), 'CR': (12.764871671513355, 78.33999633789062), 'PKST': (28.96871340495873, 34.52000045776367), 'ARLO': (101.112231070361, 9.890000343322754)}, 'total_value': 58000.0}
# ------



# Initial Portfolio Value: $100,000.00
# Final Portfolio Value: $58,000.00
# Total Return: -42.00%
# Sharpe Ratio: -18.97