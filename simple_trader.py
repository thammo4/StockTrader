# FILE: `StockTrader/simple_trader.py`
from config import * # imports all necessary modules and variables not explicitly defined

stock_universe = ['NVR', 'COIN', 'LMT', 'LLY', 'ERIC', 'C', 'ZBH', 'AJG', 'SPOT', 'IMO'];
pnl = 0.0;
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
	global pnl, trade_count;
	acct_balance = acct.get_account_balance().iloc[0]['total_cash'];

	if acct_balance > stock_price:
		print(f'Trying: Buy {symbol} at ${stock_price}....Cash: ${acct_balance}');
		order = equity_order.order(symbol=symbol, side='buy', quantity=1, order_type='market', duration='day');
		outcome = list(order.keys())[0];
		if outcome == 'order':
			print(f'Buy Order Placed: {symbol}');
			trade_count += 1;
			trades.append({'symbol':symbol, 'buy_price':stock_price, 'sell_price':None});
		elif outcome == 'errors':
			print(f'Buy Order ERROR: {symbol}');
	else:
		print(f'Insufficient Funds [{symbol}] \tCash: ${acct_balance}....Price: ${stock_price}');

def sell (symbol, stock_price):
	global pnl, trade_count;
	current_positions = acct.get_positions();
	portfolio_symbols = list(current_positions['symbol']);

	if symbol in portfolio_symbols:
		pos = current_positions.loc[current_positions['symbol'] == symbol].iloc[0];
		qty_owned = pos['quantity'];
		sale_proceeds = pos['quantity'] * stock_price;
		cost_basis = pos['cost_basis'];

		pnl_individual = sale_proceeds - cost_basis;
		pnl += pnl_individual;
		print(f'Trying: Sell {qty_owned} {symbol} for ${sale_proceeds:.3f}');
		order = equity_order.order(symbol=symbol, side='sell', quantity=qty_owned, order_type='market', duration='day');
		outcome = list(order.keys())[0];
		if outcome == 'order':
			print(f'Sell Order Placed: {symbol}');
			print(f'PnL [{symbol}]: ${pnl:.3f}');
			trade_count +=1;
			for t in trades:
				if t['symbol'] == symbol and t['sell_price'] is None:
					t['sell_price'] = stock_price;
					break;
		elif outcome == 'errors':
			print(f'Sell Order ERROR: {symbol}');
	else:
		print(f'Stock not currently owned. Skipping. [{symbol}]');


def trade_algo():
	global pnl, trade_count;
	while market_hours():
		symbol = random.choice(stock_universe);
		stock_price = quotes.get_quote_day(symbol).iloc[0]['last'];

		buy(symbol, stock_price);
		print(97*'-', '\n');
		time.sleep(300);

		sell(symbol, stock_price);
		print(97*'-', '\n');

		print(f"****** REPORT ******");
		print(f"Symbol: {symbol}");
		print(f"PnL: ${pnl:.3f}");
		print(f"Trade Count: {trade_count}");
		print(97*'-', '\n\n\n');
		time.sleep(300);

	print(f"!!!!!!!!! REPORT - END OF DAY !!!!!!!!!");
	print(f"Total PnL: ${pnl:.3f}");
	print(f"Total Trades: {trade_count}");
	print(f"Trades:");
	for t in trades:
		print(f"Symbol: {t['symbol']} | Bought: ${t['buy_price']} | Sold: ${t['sell_price']}");
	print('\n -------------- END OF TRADING --------------\n');


if __name__ == '__main__':
	print(f"Market Open? -> {market_hours()}");
	trade_algo();
	if market_hours():
		trade_algo();
	else:
		print("Market Closed.");






