import pandas as pd;
import heapq


#
# Simple Example of Call Auction Algo Used by Exchanges to Discover/Set Open/Close Prices
#

class CallAuction:
	def __init__ (self):
		self.buy_orders = [];
		self.sell_orders = [];
		self.aggregate_demand = [];
		self.aggregate_supply = [];

		self.clearing_price = None;
		self.clearing_quantity = None;

	def add_order (self, order_type, price, quantity):
		order = {'price':price, 'quantity':quantity};

		if order_type == 'buy':
			self.buy_orders.append(order);
		elif order_type == 'sell':
			self.sell_orders.append(order);
		else:
			raise ValueError('Buy/sell order types only');

	def clear_auction (self):
		#
		# Sort the Orders by Price
		# 	• Descending Buy Orders: High Price -> Low Price
		# 	• Ascending Sell Orders: Low Price -> High Price
		#

		self.buy_orders = sorted(self.buy_orders, reverse=True);
		self.sell_orders = sorted(self.sell_orders, reverse=False);

		#
		# Compute Aggregate Supply/Demand
		#

		self.aggregate_demand = sum(k[1] for k in self.buy_orders);
		self.aggregate_supply = sum(k[1] for k in self.sell_orders);

		#
		# Determine the Clearing Price p^{*}
		# 	p^{*} : Volume(p^{*}) = max(Volume) = argmax(min(D(p), S(p))); argmax taken over p = 1,...,n
		#

		i, j = [0,0];
		matched_quantity = 0;
		matched_price = None;

		while i < len(self.buy_orders) and j < len(self.sell_orders):
			b_order, s_order = self.buy_orders[i], self.sell_orders[j];

			if buy_order['price'] >= s_order['price']:
				trade_quantity = min(b_order['quantity'], s_order['quantity']);

				matched_quantity 	+= trade_quantity;
				matched_price 		= s_order['price'];

				b_order['quantity'] -= trade_quantity;
				s_order['quantity'] -= trade_quantity;

				if b_order['quantity'] == 0:
					i += 1;

				if s_order['quantity'] == 0:
					j += 1;

			else:
				break;

		return matched_price, matched_quantity;








#
# Continuous Double Auction - Buyers/Sellers continuously update bids/asks until: min(ask) ≤ max(bid)
# 	• buy orders = Max-heap
# 	• min orders = Min-heap
#

class ContinuousDoubleAuction:
	def __init__ (self):
		self.buy_orders = [];
		self.sell_orders = [];

	def add_order (self, order_type, price, quantity):
		order = {'price':price, 'quantity':quantity};

		if order_type == 'buy':
			heapq.heappush(self.buy_orders, (-price, quantity));
		elif order_type == 'sell':
			heapq.heappush(self.sell_orders, (price, quantity));
		else:
			raise ValueError('Buy/sell orders only');

		self.match_orders();

	def match_orders (self):
		while self.buy_orders and self.sell_orders:
			best_buy = self.buy_orders[0];
			best_sell = self.sell_order[0];

			if -best_buy[0] >= best_sell[0]:
				trade_price = best_sell[0];
				trade_quantity = min(best_buy[1], best_sell[1]);

				print(f"Trade Execution: (price, quantity): ({trade_price}, {trade_quantity})");
				if best_buy[1] > trade_quantity:
					heapq.heapreplace(self.buy_orders, (-best_buy[0], best_buy[1]-trade_quantity));
				else:
					heapq.heappop(self.buy_orders);

				if best_sell[1] > trade_quantity:
					heapq.heapreplace(self.sell_orders, (best_sell[0], best_sell[1] - trade_quantity));
				else:
					heapq.heappop(self.sell_orders);

			else:
				break;



#
# Instantiate Call Auction / Continuous Double Auction
#

auction_call = CallAuction();
auction_cont_double = ContinuousDoubleAuction();


#
# Add Buy Orders to Both Auctions
#

[auction_call.add_order('buy', p, q) for p, q in [(100,10), (105,5), (98,15)]];
[auction_cont_double.add_order('buy', p, q) for p,q in [(100,10), (105,5), (98,15)]];


#
# Add Sell Orders to Both Auctions
#

[auction_call.add_order('sell', p, q) for p,q in [(102,8), (100,12), (99,10)]];
[auction_cont_double.add_order('sell', p, q) for p, q in [(102,8), (100,12), (99,10)]];



#
# Clear the Call Auction
#

matched_price, matched_quantity = auction_call.clear_auction();

print(f'Call Auction Clearing Results: (price, quantity) = ({matched_price}, {matched_quantity})');
print(125*'-');



# 






