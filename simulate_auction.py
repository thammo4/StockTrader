# FILE: `StockTrader/simulate_auction.py`
# imports necessary packages and defines a few variables
from typing import Callable, List, Tuple;
from collections import namedtuple;
from abc import ABC, abstractmethod;
import heapq;

# from config import *


Order = namedtuple('Order', ['price', 'quantity', 'time', 'id']);



class MarketAuction (ABC):
	# def __init__ (self, trade_callback):
	def __init__ (self, trade_callback: Callable[[int, float, int, int], None]):
		self.buy_orders = [];
		self.sell_orders = [];
		# self.order_count = [];
		self.order_count = 0;
		self.trade_callback = trade_callback;

	def add_order (self, order_type, price, quantity, time):
		self.order_count += 1;
		order = Order(price, quantity, time, self.order_count);
		if order_type == 'buy':
			self._add_buy_order(order);
		elif order_type == 'sell':
			self._add_sell_order(order);
		else:
			raise ValueError('Buy/sell orders only');
		self.match_orders();

		return self.order_count;


	@abstractmethod
	def _add_buy_order (self, order):
		pass;


	@abstractmethod
	def _add_sell_order (self, order):
		pass;


	@abstractmethod
	def match_orders (self):
		pass;

	def cancel_order (self, order_id, order_type):
		target_list = self.buy_orders if order_type == 'buy' else self.sell_orders;
		# for i, (_, _, order) in enumerate (target_list):
		for i, order in enumerate(target_list):
			if order.id == order_id:
				del target_list[i];
				heapq.heapify(target_list);
				return True;
			return False;


class CallAuction (MarketAuction):
	def __init__ (self, trade_callback):
		super().__init__(trade_callback);
		self.aggregate_demand = [];
		self.aggregate_supply = [];
		self.clearing_price = None;
		self.clearing_quantity = None;

	def _add_buy_order (self, order):
		self.buy_orders.append(order);

	def _add_sell_order(self, order):
		self.sell_orders.append(order);

	def match_orders (self):
		self.buy_orders = sorted(self.buy_orders, reverse=True);
		self.sell_orders = sorted(self.sell_orders, reverse=False);

		self.aggregate_demand = self._compute_aggregate(self.buy_orders, reverse=True);
		self.aggregate_supply = self._compute_aggregate(self.sell_orders);

		volume_trading_max = 0;
		clearing_prices = [];

		for price in set(order.price for order in self.buy_orders+self.sell_orders):
			volume = min(
				self._get_quantity_at_price(self.aggregate_demand, price),
				self._get_quantity_at_price(self.aggregate_supply, price)
			);
			if volume > volume_trading_max:
				volume_trading_max = volume;
				clearing_prices = [price];
			elif volume == volume_trading_max:
				clearing_prices.append(price);

		self.clearing_quantity = volume_trading_max;
		self.clearing_price = self._break_price_tie(clearing_prices);
		self._execute_trades();

	def _compute_aggregate (self, orders, reverse=False):
		aggregate=[];
		quantity_total = 0;
		for order in (orders if not reverse else reversed(orders)):
			quantity_total += order.quantity;
			aggregate.append((order.price, quantity_total));
		return aggregate;

	def _get_quantity_at_price (self, aggregate, price):
		for p, q in aggregate:
			if (p >= price and not aggregate[-1][0] > aggregate[0][0]) or (p <= price and aggregate[-1][0] > aggregate[0][0]):
				return q;
		return 0;

	def _break_price_tie (self, prices):
		if len(prices) == 1:
			return prices[0];

		mid_price = sum(prices) / len(prices);

		order_imbalance = [ (abs(self._get_quantity_at_price(self.aggregate_demand, p)-self._get_quantity_at_price(self.aggregate_supply, p)), p) for p in prices];
		order_imbalance_min = min(order_imbalance)[0];
		price_imbalance_min = [p for imb, p in order_imbalance if imb == order_imbalance_min];

		if len(price_imbalance_min) == 1:
			return price_imbalance_min[0];

		demand = self._get_quantity_at_price(self.aggregate_demand, mid_price);
		supply = self._get_quantity_at_price(self.aggregate_supply, mid_price);

		if demand == supply:
			return mid_price;
		else:
			return max(price_imbalance_min) if demand > supply else min(price_imbalance_min);

	def _execute_trades(self):
		if not self.clearing_price or not self.clearing_quantity:
			return;

		quantity_executed = 0;
		for b_order in self.buy_orders:
			if b_order.price < self.clearing_price:
				break;
			for s_order in self.sell_orders:
				if s_order.price > self.clearing_price:
					break;
				trade_quantity = min(b_order.quantity,s_order.quantity, self.clearing_quantity-quantity_executed);
				if trade_quantity > 0:
					self.trade_callback(self.clearing_price, trade_quantity, b_order.id, s_order.id);
					quantity_executed += trade_quantity;
					b_order = b_order._replace(quantity=b_order.quantity-trade_quantity);
					s_order = s_order._replace(quantity=s_order.quantity - trade_quantity);
				if quantity_executed == self.clearing_quantity:
					return;





#
# NOTE RE: Continuous Double Auction Data Structures
# • Buy list -> Max Priority Queue
# • Sell List -> Min Priority Queue
class ContinuousDoubleAuction (MarketAuction):
	def __init__ (self, trade_callback: Callable[[int, float, int, int, int], None]):
		'''
			This class will instantiate an object representing a Continuous Double Auction.
			This is a market mechanism in which participants are either buyers or sellers.
			The buyers and sellers continuously submit orders to the exchange.
			When a buyer's bid price exceeds a seller's ask price, a transaction occurs.
		'''
		super().__init__(trade_callback);

		# self.buy_orders = List[Tuple[float, int, Order]] = [];
		# self.sell_orders = List[Tuple[float, int, Order]] = [];
		self.buy_orders: List[Tuple[float, int, Order]] = [];
		self.sell_orders: List[Tuple[float, int, Order]] = [];
		self.trade_count: int = 0;



		# self.trade_callback = trade_callback;

		# self.order_count: int = 0;
		# self.trade_count: int = 0;

	def add_order (self, side: str, price: float, quantity: int, ts: int) -> int:
		self.order_count += 1;
		order = Order(self.order_count, price, quantity, ts);
		if side == 'buy':
			heapq.heappush(self.buy_orders, (-price, -ts, order));
		elif side == 'sell':
			heapq.heappush(self.sell_orders, (price, ts, order));
		else:
			raise ValueError('Side must be buy/sell');
		# heapq.heappush(self.buy_orders, (-price,-ts,order)) if side == 'buy' else heapq.heappush(self.sell_orders, (price,ts,order));

		self.match_orders();
		return self.order_count;

	def cancel_order (self, order_id: int, side: str) -> bool:
		order_book = self.buy_orders if side == 'buy' else self.sell_orders;
		for i, (_,_,order) in enumerate(order_book):
			if order.id == order_id:
				order_book[i] = order_book[-1];
				order_book.pop();

				if i < len(order_book):
					heapq._siftup(order_book, i);
					heapq._siftdown(order_book, 0, i);
				return True;
		return False;


	def match_orders (self) -> None:
		while self.buy_orders and self.sell_orders:
			bid_highest = self.buy_orders[0][2];
			ask_lowest = self.sell_orders[0][2];


			#
			# Trade only occurs when someones buy/sell order crosses bid-ask spread
			#


			if bid_highest.price < ask_lowest.price:
				break;

			# trade_price = bid_highest.price;
			trade_price = ask_lowest.price;
			trade_quantity = min(bid_highest.quantity, ask_lowest.quantity);

			self.trade_count += 1;
			self.trade_callback(self.trade_count, trade_price, trade_quantity, bid_highest.id, ask_lowest.id);


			#
			# Update/Remove orders
			#

			if bid_highest.quantity > trade_quantity:
				new_bid = Order(bid_highest.price, bid_highest.quantity-trade_quantity, bid_highest.time, bid_highest.id);
				heapq.heapreplace(self.buy_orders, (-new_bid.price, -new_bid.time, new_bid));
			else:
				heapq.heappop(self.buy_orders);

			if ask_lowest.quantity > trade_quantity:
				new_ask = Order(ask_lowest.price, ask_lowest.quantity-trade_quantity, ask_lowest.time, ask_lowest.id);
				heapq.heapreplace(self.sell_orders, (new_ask.price, new_ask.time, new_ask));
			else:
				heapq.heappop(self.sell_orders);


	def get_order_book (self) -> Tuple[List[Tuple[float, int]], List[Tuple[float,int]]]:
		bid_book = [(-price, order.quantity) for price, _, order in self.buy_orders];
		sell_book = [(price, order.quantity) for price, _, order in self.sell_orders];
		return (sorted(bid_book, reverse=True), sorted(sell_book, reverse=False));

B = 6 * ['buy'];
P = [100, 100, 99, 101, 101, 102];
Q = [10, 5, 5, 10, 5, 10];
T = [x for x in range(1,7)];

S = 6 * ['sell'];
P = [103, 103, 111, 104, 101, 112];
Q = [10, 5, 5, 10, 5, 10];
T = [x for x in range(1,7)];

buy_orders = [(b, p, q, t) for b,p,q,t in zip(B, P,Q,T)];
sell_orders = [(s, p, q, t) for s,p,q,t in zip(S, P,Q,T)];



def auction_trade_callback (price, quantity, buy_order_id, sell_order_id):
	print(f"ORDER EXECUTED\nPrice: {price}\nQuantity: {quantity}\nBuy Order ID: {buy_order_id}\nSell Order ID: {sell_order_id}");




#
# Instantiate Call Auction Object
#

call_auction = CallAuction(auction_trade_callback);


#
# 'Send' Buy/Sell Orders to the Call Auction
#

[call_auction.add_order(b, p,q,t) for b, p,q,t in buy_orders];
[call_auction.add_order(s, p,q,t) for s,p,q,t in sell_orders];








# >>> call_auction
# <simulate_auction.CallAuction object at 0x12fa13860>
# >>> [('buy', p, q, r) for p,q,r in [(100,10,1), (100,5,2), (99,5,3), (101,10,4), (101,5,5), (102,10,6)]]
# [('buy', 100, 10, 1), ('buy', 100, 5, 2), ('buy', 99, 5, 3), ('buy', 101, 10, 4), ('buy', 101, 5, 5), ('buy', 102, 10, 6)]
# >>> call_auction.buy_orders
# []
























