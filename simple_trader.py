# FILE: `StockTrader/simple_trader.py`
from config import * # imports all necessary modules and variables not explicitly defined

stock_universe = ['KMI', 'COIN', 'LMT', 'LLY', 'SHEL', 'C', 'WMB', 'IMO', 'LYB', 'MFA', 'MSM', 'MSCI', 'MDT', 'MCK', 'AES', 'MTN', 'VOC', 'URI', 'USAC', 'CEFD', 'USPH', 'TEN', 'TROX', 'TD', 'TTC', 'THO', 'BK', 'TT'];
pnl = 0.0;
drawdown = 0.0;
trade_count = 0;
trades = [];

def market_hours ():
	current_time = datetime.now();
	market_open = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0);
	market_close = datetime.now().replace(hour=16, minute=0, second=0, microsecond=0);

	if market_open <= current_time <= market_close:
		return True;
	else:
		return False;


def buy (symbol, stock_price):
	print(f'****** BUY [{datetime.now().strftime("%H:%M:%S")}]******');
	global pnl, drawdown, trade_count;
	qty_to_buy = random.randint(1,100);
	acct_balance = acct.get_account_balance().iloc[0]['total_cash'];
	if acct_balance > stock_price*qty_to_buy:
		print(f'Trying: Buy {qty_to_buy} {symbol} at ${stock_price}....Cash: ${acct_balance}');
		order = equity_order.order(symbol=symbol, side='buy', quantity=qty_to_buy, order_type='market', duration='day');
		outcome = list(order.keys())[0];
		if outcome == 'order':
			print(f'Buy Order Placed: {symbol}');
			trade_count += 1;
			pnl -= stock_price*qty_to_buy;
			drawdown += stock_price * qty_to_buy;
			# print(f'Drawdown: [{symbol}]: ${drawdown:.3f}');
			trades.append({'symbol':symbol, 'buy_price':stock_price, 'buy_shares':qty_to_buy, 'sell_price':None, 'sell_shares':None});
		elif outcome == 'errors':
			print(f'Buy Order ERROR: {symbol}');
	else:
		print(f'Insufficient Funds [{symbol}] \tCash: ${acct_balance}....Price: ${stock_price}');
		print('Selling for $$$$$');
		new_symbol = random.choice(list(acct.get_positions()['symbol']));
		new_price = quotes.get_quote_day(new_symbol, True);
		sell(
			symbol=new_symbol,
			stock_price=new_price
		);

def sell (symbol, stock_price):
	print(f'****** SELL [{datetime.now().strftime("%H:%M:%S")}] ******');
	global pnl, drawdown, trade_count;
	current_positions = acct.get_positions();
	portfolio_symbols = list(current_positions['symbol']);

	if symbol in portfolio_symbols:
		pos = current_positions.loc[current_positions['symbol'] == symbol].iloc[0];
		qty_owned = pos['quantity'];
		if qty_owned == 0:
			print(f'Zero quantity: {symbol}');
			return;

		qty_to_sell = random.randint(1, int(qty_owned));

		sale_proceeds = pos['quantity'] * stock_price;
		cost_basis = pos['cost_basis'];

		# pnl_individual = sale_proceeds - cost_basis;
		# pnl += pnl_individual;
		drawdown -= sale_proceeds;
		pnl += sale_proceeds - cost_basis;
		print(f'Trying: Sell {qty_to_sell} {symbol} for ${sale_proceeds:.3f}');
		order = equity_order.order(symbol=symbol, side='sell', quantity=qty_to_sell, order_type='market', duration='day');
		outcome = list(order.keys())[0];
		if outcome == 'order':
			print(f'Sell Order Placed: {symbol}');
			# print(f'PnL [{symbol}]: ${pnl:.3f}');
			trade_count +=1;
			for t in trades:
				if t['symbol'] == symbol and t['sell_price'] is None:
					t['sell_price'] = stock_price;
					t['sell_shares'] = qty_to_sell;
					break;
		elif outcome == 'errors':
			print(f'Sell Order ERROR: {symbol}');
	else:
		print(f'Stock not currently owned. Skipping. [{symbol}]');


def trade_algo():
	global pnl, drawdown, trade_count;
	iter = 0;
	while market_hours():
		buy_symbol = random.choice(stock_universe);
		stock_price = quotes.get_quote_day(buy_symbol).iloc[0]['last'];

		buy(buy_symbol, stock_price);
		# print(97*'-', '\n');
		print('-------')
		# time.sleep(.00075);

		sell_symbol = random.choice(stock_universe);
		stock_price = quotes.get_quote_day(sell_symbol).iloc[0]['last'];

		sell(sell_symbol, stock_price);
		# print(97*'-', '\n');
		print('-------')

		print(f"****** REPORT [{datetime.now().strftime('%H:%M:%S')}] ******");
		# print(f"Symbol: {symbol}");
		# print(f"Buy Symbol: {buy_symbol} ..... Sell Symbol: {sell_symbol}")
		print(f"(Buy, Sell) = ({buy_symbol}, {sell_symbol})");
		if iter % 15 == 0:
			print(f"Drawdown: ${drawdown:.3f}");
			print(f"PnL: ${pnl:.3f}");
			print(f"Trade Count: {trade_count}");
			print(f"Transactions:");
			for t in trades:
				print(f"Symbol: {t['symbol']} | Bought: ${t['buy_price']} ({t['buy_shares']}) | Sold: ${t['sell_price']} ({t['sell_shares']})");
			print(" ############## END OF BUY-SELL SEQUENCE ############## ");
			print(97*'-', '\n\n\n');
			time.sleep(1.5);
		iter += 1;
		# print(" ############## END OF BUY-SELL SEQUENCE ############## ");

	print(f"!!!!!!!!! REPORT - END OF DAY !!!!!!!!!");
	print(f"Total PnL: ${pnl:.3f}");
	print(f"Total Trades: {trade_count}");
	print(f"Trades:");
	for t in trades:
		print(f"Symbol: {t['symbol']} | Bought: ${t['buy_price']} ({t['buy_shares']}) | Sold: ${t['sell_price']} ({t['sell_shares']})");
	print('\n -------------- END OF TRADING --------------\n\n\n\n');


if __name__ == '__main__':
	print(f"Market Open? -> {market_hours()}");
	print('\n\n\n');
	trade_algo();
	if market_hours():
		trade_algo();
	else:
		print("Market Closed.");





#
# SAMPLE OUTPUT
#

# ****** BUY [11:35:30]******
# Trying: Buy 3 LMT at $553.81....Cash: $339791.36
# Buy Order Placed: LMT
# -------
# ****** SELL [11:35:33] ******
# Trying: Sell 3.0 SHEL for $215.475
# Sell Order Placed: SHEL
# -------
# ****** REPORT [11:35:33] ******
# (Buy, Sell) = (LMT, SHEL)
# Drawdown: $1445.955
# PnL: $-0.045
# Trade Count: 2
# Transactions:
# Symbol: LMT | Bought: $553.81 (3) | Sold: $None (None)
#  ############## END OF BUY-SELL SEQUENCE ############## 
# ------------------------------------------------------------------------------------------------- 



# ****** BUY [11:35:36]******
# Trying: Buy 2 LMT at $553.81....Cash: $339791.36
# Buy Order Placed: LMT
# -------
# ****** SELL [11:35:39] ******
# Stock not currently owned. Skipping. [SHEL]
# -------
# ****** REPORT [11:35:39] ******
# (Buy, Sell) = (LMT, SHEL)
# Drawdown: $2553.575
# PnL: $-0.045
# Trade Count: 3
# Transactions:
# Symbol: LMT | Bought: $553.81 (3) | Sold: $None (None)
# Symbol: LMT | Bought: $553.81 (2) | Sold: $None (None)
#  ############## END OF BUY-SELL SEQUENCE ############## 
# ------------------------------------------------------------------------------------------------- 



# ****** BUY [11:35:41]******
# Trying: Buy 1 COIN at $196.37....Cash: $337235.45
# Buy Order Placed: COIN
# -------
# ****** SELL [11:35:44] ******
# Stock not currently owned. Skipping. [WMB]
# -------
# ****** REPORT [11:35:44] ******
# (Buy, Sell) = (COIN, WMB)
# Drawdown: $2749.945
# PnL: $-0.045
# Trade Count: 4
# Transactions:
# Symbol: LMT | Bought: $553.81 (3) | Sold: $None (None)
# Symbol: LMT | Bought: $553.81 (2) | Sold: $None (None)
# Symbol: COIN | Bought: $196.37 (1) | Sold: $None (None)
#  ############## END OF BUY-SELL SEQUENCE ############## 
# ------------------------------------------------------------------------------------------------- 



# ****** BUY [11:35:47]******
# Trying: Buy 3 C at $61.29....Cash: $337235.45
# Buy Order Placed: C
# -------
# ****** SELL [11:35:50] ******
# Stock not currently owned. Skipping. [WMB]
# -------
# ****** REPORT [11:35:50] ******
# (Buy, Sell) = (C, WMB)
# Drawdown: $2933.815
# PnL: $-0.045
# Trade Count: 5
# Transactions:
# Symbol: LMT | Bought: $553.81 (3) | Sold: $None (None)
# Symbol: LMT | Bought: $553.81 (2) | Sold: $None (None)
# Symbol: COIN | Bought: $196.37 (1) | Sold: $None (None)
# Symbol: C | Bought: $61.29 (3) | Sold: $None (None)
#  ############## END OF BUY-SELL SEQUENCE ############## 
# ------------------------------------------------------------------------------------------------- 



