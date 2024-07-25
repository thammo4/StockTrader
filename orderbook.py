from config import * # import needed package


class EHKModel:
	def __init__ (self, n_states=3, n_features=10):
		self.n_states = n_states;
		self.n_features = n_features;

		self.hmm = hmm.GaussianHMM(n_components=n_states, covariance_type='full');

		self.kf_models = [KalmanFilter(dim_x=n_features, dim_z=n_features) for _ in range(n_states)];
		for kf in self.kf_models:
			kf.x = np.zeros(n_features);
			kf.P *= 1000.0;
			kf.R = np.eye(n_features)*0.1;
			kf.Q = np.eye(n_features)*0.01;

			self.state_seq = [];
			self.observations = [];
			self.is_fit = False;
			self.min_obvs = max(100, 10*n_states);

	def em (self):
		if len(self.observations) > self.n_states:
			obvs_array = np.array(self.observations);
			try:
				print(f"FITTIN HMM {len(obvs_array)} obvs");
				self.hmm.fit(obvs_array);
				posteriors = self.hmm.predict_proba(obvs_array);
				print("HMM FIT GOOD");

				for i, kf in enumerate(self.kf_models):
					obvs_weighted = obvs_array * posteriors[:, i:i+1];
					kf.batch_filter(obvs_weighted);

				self.is_fit = True;
				print(f'FIT MODEL {len(self.observations)}');
			except Exception as e:
				print(f'FIT ERROR {str(e)}');
		else:
			print('LACKS OBVS, COUNT: {len(self.observations)}\nNEED COUNT: {self.n_states}');

	def update (self, observation):
		observation = np.array(observation).reshape(1,-1);
		self.observations.append(observation[0]);
		print(f'STATE SEQ UPDATE: {self.state_seq}');

		if len(self.observations) >= self.min_obvs:
			if not self.is_fit:
				self.em();
				print(f'TOTAL OBVS: {len(self.observations)}');

			if self.is_fit:
				state = self.hmm.predict(observation);
				self.state_seq.append(state[0]);
				self.kf_models[state[0]].update(observation[0]);

			if len(self.observations) % 100 == 0:
				self.em();
				print(f'TOTAL OBVS: {len(self.observations)}');

	def estimate_liq (self):
		print(f'ESTIMATE LIQ: is_fit:{self.is_fit}\tstate_seq:{self.state_seq}');
		if not self.is_fit:
			return np.zeros(self.n_features);
		if not self.state_seq:
			return self.kf_models[0].x

		current_state = self.state_seq[-1];
		return self.kf_models[current_state].x;

	def get_hidden_state (self):
		if not self.state_seq:
			return None;
		return self.state_seq[-1];

	def get_state_probs (self):
		if not self.observations:
			return None;
		return self.hmm.predict_proba(self.observations[-1].reshape(1,-1))[0];


class OrderBook:
	def __init__ (self):
		self.bids = defaultdict(lambda: defaultdict(int));
		self.asks = defaultdict(lambda: defaultdict(int));
		self.last_update = None;

	def update (self, quote):
		bid_price, bid_size = float(quote['bid']), int(quote['bidsz']);
		ask_price, ask_size = float(quote['ask']), int(quote['asksz']);

		self.bids[bid_price][quote['bidexch']] = bid_size;
		self.asks[ask_price][quote['askexch']] = ask_size;

		# self.last_update = max(int(quote['biddate']), int(quote['askdate']));
		self.last_update = max(
			datetime.fromtimestamp(int(quote['biddate'])/1000).strftime('%Y-%m-%d %H:%M:%S'),
			datetime.fromtimestamp(int(quote['askdate'])/1000).strftime('%Y-%m-%d %H:%M:%S')
		);

	def get_book_state (self):
		return {
			'bids': dict(self.bids),
			'asks': dict(self.asks),
			'last_update': self.last_update
		};



def extract_features (book_state):
	bids, asks = book_state['bids'], book_state['asks'];

	bid_prices, ask_prices = sorted(bids.keys(), reverse=True), sorted(asks.keys());

	features = {
		'best_bid': bid_prices[0] if bid_prices else None,
		'best_ask': ask_prices[0] if ask_prices else None,
		'bid_ask_spread': ask_prices[0] - bid_prices[0] if bid_prices and ask_prices else None,
		'total_bid_size': sum(sum(sizes.values()) for sizes in bids.values()),
		'total_ask_size': sum(sum(sizes.values()) for sizes in asks.values()),
		'bid_depth': len(bid_prices),
		'ask_depth': len(ask_prices),
		'bid_size_imbalance': sum(sum(sizes.values()) for sizes in bids.values()) / len(bid_prices) if bid_prices else None,
		'ask_size_imbalance': sum(sum(sizes.values()) for sizes in asks.values()) / len(ask_prices) if ask_prices else None
		# 'since_update': (int(datetime.now().strftime('%s'))-book_state['last_update'])
	};

	return features;


def ehk_init (n_states=3, n_features=10):
	return EHKModel(n_states, n_features);


with open("./nvda_quotes724.json", "r") as f:
	quote_events = json.load(f);



# >>> book.get_book_state()
# {'bids': {114.71: defaultdict(<class 'int'>, {'Z': 1, 'N': 7, 'Q': 1, 'H': 7, 'K': 9, 'P': 6, 'M': 3, 'J': 1}), 114.73: defaultdict(<class 'int'>, {'N': 8, 'Z': 9, 'K': 13, 'Q': 11, 'P': 8, 'H': 5, 'U': 3}), 114.72: defaultdict(<class 'int'>, {'N': 6, 'K': 7, 'Q': 9, 'P': 2, 'Z': 2, 'H': 6}), 114.75: defaultdict(<class 'int'>, {'H': 4, 'N': 7, 'P': 8, 'Q': 2, 'K': 6, 'Z': 1}), 114.74: defaultdict(<class 'int'>, {'H': 7, 'P': 5, 'N': 7, 'K': 7, 'U': 8, 'Q': 2, 'M': 3, 'Z': 2}), 114.76: defaultdict(<class 'int'>, {'H': 1, 'Q': 5, 'K': 7, 'N': 6, 'P': 7, 'Z': 2}), 114.77: defaultdict(<class 'int'>, {'Z': 2, 'H': 3, 'K': 8, 'N': 6, 'Q': 9, 'P': 1, 'U': 20}), 114.78: defaultdict(<class 'int'>, {'Q': 5, 'U': 1, 'N': 1, 'H': 5, 'P': 2, 'Z': 8, 'K': 8}), 114.7: defaultdict(<class 'int'>, {'Q': 3, 'K': 7, 'N': 25, 'P': 2, 'M': 3, 'H': 4, 'Z': 2, 'U': 1}), 114.69: defaultdict(<class 'int'>, {'Q': 8, 'A': 2, 'N': 7, 'H': 3, 'P': 1, 'K': 8, 'Z': 3}), 114.68: defaultdict(<class 'int'>, {'N': 7, 'Q': 9, 'Z': 9, 'H': 7, 'K': 15, 'P': 2, 'U': 6}), 114.67: defaultdict(<class 'int'>, {'Q': 3, 'Z': 18, 'N': 7, 'K': 8, 'H': 2, 'P': 1}), 114.66: defaultdict(<class 'int'>, {'Q': 1, 'H': 1, 'N': 9, 'K': 7, 'U': 6, 'Z': 2, 'J': 4, 'P': 7}), 114.65: defaultdict(<class 'int'>, {'K': 6, 'U': 1, 'Z': 2, 'Q': 9, 'N': 8, 'P': 1}), 114.79: defaultdict(<class 'int'>, {'Q': 4, 'N': 1, 'Z': 4, 'H': 6, 'K': 8}), 114.8: defaultdict(<class 'int'>, {'Q': 6, 'K': 7}), 114.81: defaultdict(<class 'int'>, {'Q': 3, 'H': 6, 'K': 9}), 114.64: defaultdict(<class 'int'>, {'Q': 6, 'K': 8, 'N': 1, 'H': 4, 'P': 1}), 114.63: defaultdict(<class 'int'>, {'K': 6, 'H': 5, 'Q': 13, 'M': 3, 'P': 13, 'N': 7, 'Z': 1, 'U': 1}), 114.61: defaultdict(<class 'int'>, {'U': 4, 'P': 1, 'K': 7, 'Q': 2, 'H': 5, 'N': 1, 'Z': 2, 'V': 1}), 114.59: defaultdict(<class 'int'>, {'Z': 2, 'N': 7, 'K': 6, 'Q': 9, 'P': 1, 'H': 5, 'U': 5}), 114.6: defaultdict(<class 'int'>, {'N': 6, 'K': 1, 'Z': 1, 'Q': 4, 'H': 6, 'M': 3, 'P': 1}), 114.58: defaultdict(<class 'int'>, {'N': 6, 'Q': 1, 'K': 18}), 114.57: defaultdict(<class 'int'>, {'Q': 9, 'N': 6, 'P': 8, 'K': 8}), 114.56: defaultdict(<class 'int'>, {'P': 10, 'N': 14}), 114.62: defaultdict(<class 'int'>, {'Q': 1, 'P': 2, 'H': 6, 'N': 6, 'K': 6, 'Z': 1})}, 'asks': {114.73: defaultdict(<class 'int'>, {'P': 8, 'M': 3, 'Q': 6, 'Z': 32, 'K': 6, 'N': 6, 'U': 1, 'H': 10}), 114.75: defaultdict(<class 'int'>, {'K': 6, 'P': 1, 'Q': 7, 'N': 6, 'M': 3, 'Z': 1, 'U': 53, 'H': 1}), 114.74: defaultdict(<class 'int'>, {'Q': 2, 'P': 1, 'K': 2, 'N': 6, 'H': 1, 'M': 3, 'Z': 1}), 114.77: defaultdict(<class 'int'>, {'K': 3, 'Q': 1, 'N': 1, 'X': 1, 'P': 1, 'Z': 1, 'U': 3, 'H': 1}), 114.76: defaultdict(<class 'int'>, {'Q': 35, 'H': 1, 'Z': 2, 'K': 6, 'P': 1, 'N': 7}), 114.79: defaultdict(<class 'int'>, {'P': 6, 'U': 12, 'Q': 9, 'K': 14, 'Z': 2, 'H': 1, 'N': 6}), 114.78: defaultdict(<class 'int'>, {'K': 6, 'Q': 7, 'P': 1, 'N': 7, 'U': 1, 'H': 1, 'Z': 2}), 114.8: defaultdict(<class 'int'>, {'Q': 6, 'K': 74, 'P': 2}), 114.71: defaultdict(<class 'int'>, {'Q': 2, 'H': 1, 'K': 7, 'Z': 1, 'U': 1, 'N': 6, 'X': 7, 'P': 1}), 114.72: defaultdict(<class 'int'>, {'Q': 12, 'U': 1, 'K': 7, 'N': 6, 'P': 1, 'M': 3, 'Z': 6, 'H': 1}), 114.7: defaultdict(<class 'int'>, {'Q': 8, 'K': 7, 'Z': 1, 'P': 2, 'N': 6, 'H': 1, 'M': 3}), 114.69: defaultdict(<class 'int'>, {'Q': 10, 'Z': 2, 'K': 8, 'P': 1, 'N': 7, 'H': 1}), 114.68: defaultdict(<class 'int'>, {'Q': 7, 'K': 1, 'N': 7, 'P': 10}), 114.67: defaultdict(<class 'int'>, {'P': 2, 'H': 10, 'X': 7, 'Q': 1, 'K': 2, 'Z': 1, 'N': 8}), 114.66: defaultdict(<class 'int'>, {'Q': 2, 'N': 6, 'K': 1, 'Z': 1, 'M': 3, 'P': 10, 'X': 6}), 114.81: defaultdict(<class 'int'>, {'K': 6, 'Q': 1, 'N': 6, 'H': 1, 'P': 10}), 114.82: defaultdict(<class 'int'>, {'Q': 15, 'Z': 8}), 114.83: defaultdict(<class 'int'>, {'K': 17}), 114.65: defaultdict(<class 'int'>, {'K': 7, 'Q': 2, 'N': 6, 'Z': 3, 'P': 8}), 114.64: defaultdict(<class 'int'>, {'K': 1, 'Q': 1, 'P': 3}), 114.62: defaultdict(<class 'int'>, {'Q': 2, 'K': 13, 'Z': 1, 'N': 7, 'H': 1}), 114.6: defaultdict(<class 'int'>, {'K': 1, 'Q': 1, 'H': 1, 'Z': 1, 'N': 6, 'U': 1}), 114.61: defaultdict(<class 'int'>, {'K': 8, 'Q': 3, 'N': 7, 'P': 9, 'Z': 1}), 114.63: defaultdict(<class 'int'>, {'N': 6, 'K': 6, 'P': 5, 'Q': 9, 'Z': 1, 'X': 2}), 114.59: defaultdict(<class 'int'>, {'K': 7, 'Q': 4}), 114.58: defaultdict(<class 'int'>, {'Q': 1, 'Z': 1})}, 'last_update': '2024-07-24 14:54:46'}
book = OrderBook();
model = ehk_init(n_states=3, n_features=9);

#
# Testing `for quote in quote_events` loop with first element of quote_events
#

# {'type': 'quote', 'symbol': 'NVDA', 'bid': 114.71, 'bidsz': 12, 'bidexch': 'Z', 'biddate': '1721847016000', 'ask': 114.73, 'asksz': 7, 'askexch': 'P', 'askdate': '1721847017000'}
# q_event = quote_events[0];


# >>> book.get_book_state()
# {'bids': {114.71: defaultdict(<class 'int'>, {'Z': 12})}, 'asks': {114.73: defaultdict(<class 'int'>, {'P': 7})}, 'last_update': '2024-07-24 14:50:17'}
# book.update(q_event);

# >>> book_features
# {'best_bid': 114.71, 'best_ask': 114.73, 'bid_ask_spread': 0.020000000000010232, 'total_bid_size': 12, 'total_ask_size': 7, 'bid_depth': 1, 'ask_depth': 1, 'bid_size_imbalance': 12.0, 'ask_size_imbalance': 7.0}
# book_features = extract_features(book.get_book_state());

# model.update(list(book_features.values()));



for i, q_event in enumerate(quote_events[1:]):
	book.update(q_event);
	book_features = extract_features(book.get_book_state());
	model.update(list(book_features.values()));




#
# OUTPUT
#


# >>> model.state_seq
# [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
# >>> model
# <orderbook.EHKModel object at 0x15fa5fe00>
# >>> model.
# model.em()               model.get_hidden_state() model.hmm                model.kf_models          model.n_features         model.observations       model.update(           
# model.estimate_liq()     model.get_state_probs()  model.is_fit             model.min_obvs           model.n_states           model.state_seq         
# >>> model.kf_models
# [KalmanFilter object
# dim_x = 9
# dim_z = 9
# dim_u = 0
# x = [0. 0. 0. 0. 0. 0. 0. 0. 0.]
# P = [[1172.    0.    0.    0.    0.    0.    0.    0.    0.]
#      [   0. 1172.    0.    0.    0.    0.    0.    0.    0.]
#      [   0.    0. 1172.    0.    0.    0.    0.    0.    0.]
#      [   0.    0.    0. 1172.    0.    0.    0.    0.    0.]
#      [   0.    0.    0.    0. 1172.    0.    0.    0.    0.]
#      [   0.    0.    0.    0.    0. 1172.    0.    0.    0.]
#      [   0.    0.    0.    0.    0.    0. 1172.    0.    0.]
#      [   0.    0.    0.    0.    0.    0.    0. 1172.    0.]
#      [   0.    0.    0.    0.    0.    0.    0.    0. 1172.]]
# x_prior = [0. 0. 0. 0. 0. 0. 0. 0. 0.]
# P_prior = [[1172.    0.    0.    0.    0.    0.    0.    0.    0.]
#            [   0. 1172.    0.    0.    0.    0.    0.    0.    0.]
#            [   0.    0. 1172.    0.    0.    0.    0.    0.    0.]
#            [   0.    0.    0. 1172.    0.    0.    0.    0.    0.]
#            [   0.    0.    0.    0. 1172.    0.    0.    0.    0.]
#            [   0.    0.    0.    0.    0. 1172.    0.    0.    0.]
#            [   0.    0.    0.    0.    0.    0. 1172.    0.    0.]
#            [   0.    0.    0.    0.    0.    0.    0. 1172.    0.]
#            [   0.    0.    0.    0.    0.    0.    0.    0. 1172.]]
# x_post = [0. 0. 0. 0. 0. 0. 0. 0. 0.]
# P_post = [[1172.    0.    0.    0.    0.    0.    0.    0.    0.]
#           [   0. 1172.    0.    0.    0.    0.    0.    0.    0.]
#           [   0.    0. 1172.    0.    0.    0.    0.    0.    0.]
#           [   0.    0.    0. 1172.    0.    0.    0.    0.    0.]
#           [   0.    0.    0.    0. 1172.    0.    0.    0.    0.]
#           [   0.    0.    0.    0.    0. 1172.    0.    0.    0.]
#           [   0.    0.    0.    0.    0.    0. 1172.    0.    0.]
#           [   0.    0.    0.    0.    0.    0.    0. 1172.    0.]
#           [   0.    0.    0.    0.    0.    0.    0.    0. 1172.]]
# F = [[1. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 1. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 1. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 1. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 1. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 1. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 1. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 1. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 1.]]
# Q = [[0.01 0.   0.   0.   0.   0.   0.   0.   0.  ]
#      [0.   0.01 0.   0.   0.   0.   0.   0.   0.  ]
#      [0.   0.   0.01 0.   0.   0.   0.   0.   0.  ]
#      [0.   0.   0.   0.01 0.   0.   0.   0.   0.  ]
#      [0.   0.   0.   0.   0.01 0.   0.   0.   0.  ]
#      [0.   0.   0.   0.   0.   0.01 0.   0.   0.  ]
#      [0.   0.   0.   0.   0.   0.   0.01 0.   0.  ]
#      [0.   0.   0.   0.   0.   0.   0.   0.01 0.  ]
#      [0.   0.   0.   0.   0.   0.   0.   0.   0.01]]
# R = [[0.1 0.  0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.1 0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.1 0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.1 0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.1 0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.1 0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.1 0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.1 0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.  0.1]]
# H = [[0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]]
# K = [[0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]]
# y = [ 1.14810000e+02  1.14580000e+02 -2.30000000e-01  8.76000000e+02
#       7.83000000e+02  2.60000000e+01  2.60000000e+01  3.36923077e+01
#       3.01153846e+01]
# S = [[0.1 0.  0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.1 0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.1 0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.1 0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.1 0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.1 0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.1 0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.1 0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.  0.1]]
# SI = [[10.  0.  0.  0.  0.  0.  0.  0.  0.]
#       [ 0. 10.  0.  0.  0.  0.  0.  0.  0.]
#       [ 0.  0. 10.  0.  0.  0.  0.  0.  0.]
#       [ 0.  0.  0. 10.  0.  0.  0.  0.  0.]
#       [ 0.  0.  0.  0. 10.  0.  0.  0.  0.]
#       [ 0.  0.  0.  0.  0. 10.  0.  0.  0.]
#       [ 0.  0.  0.  0.  0.  0. 10.  0.  0.]
#       [ 0.  0.  0.  0.  0.  0.  0. 10.  0.]
#       [ 0.  0.  0.  0.  0.  0.  0.  0. 10.]]
# M = [[0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]]
# B = None
# z = [ 1.14810000e+02  1.14580000e+02 -2.30000000e-01  8.76000000e+02
#       7.83000000e+02  2.60000000e+01  2.60000000e+01  3.36923077e+01
#       3.01153846e+01]
# log-likelihood = -7050843.275754711
# likelihood = 2.2250738585072014e-308
# mahalanobis = 3755.2217955643655
# alpha = 1.0
# inv = <function inv at 0x1036920c0>, KalmanFilter object
# dim_x = 9
# dim_z = 9
# dim_u = 0
# x = [0. 0. 0. 0. 0. 0. 0. 0. 0.]
# P = [[1172.    0.    0.    0.    0.    0.    0.    0.    0.]
#      [   0. 1172.    0.    0.    0.    0.    0.    0.    0.]
#      [   0.    0. 1172.    0.    0.    0.    0.    0.    0.]
#      [   0.    0.    0. 1172.    0.    0.    0.    0.    0.]
#      [   0.    0.    0.    0. 1172.    0.    0.    0.    0.]
#      [   0.    0.    0.    0.    0. 1172.    0.    0.    0.]
#      [   0.    0.    0.    0.    0.    0. 1172.    0.    0.]
#      [   0.    0.    0.    0.    0.    0.    0. 1172.    0.]
#      [   0.    0.    0.    0.    0.    0.    0.    0. 1172.]]
# x_prior = [0. 0. 0. 0. 0. 0. 0. 0. 0.]
# P_prior = [[1172.    0.    0.    0.    0.    0.    0.    0.    0.]
#            [   0. 1172.    0.    0.    0.    0.    0.    0.    0.]
#            [   0.    0. 1172.    0.    0.    0.    0.    0.    0.]
#            [   0.    0.    0. 1172.    0.    0.    0.    0.    0.]
#            [   0.    0.    0.    0. 1172.    0.    0.    0.    0.]
#            [   0.    0.    0.    0.    0. 1172.    0.    0.    0.]
#            [   0.    0.    0.    0.    0.    0. 1172.    0.    0.]
#            [   0.    0.    0.    0.    0.    0.    0. 1172.    0.]
#            [   0.    0.    0.    0.    0.    0.    0.    0. 1172.]]
# x_post = [0. 0. 0. 0. 0. 0. 0. 0. 0.]
# P_post = [[1172.    0.    0.    0.    0.    0.    0.    0.    0.]
#           [   0. 1172.    0.    0.    0.    0.    0.    0.    0.]
#           [   0.    0. 1172.    0.    0.    0.    0.    0.    0.]
#           [   0.    0.    0. 1172.    0.    0.    0.    0.    0.]
#           [   0.    0.    0.    0. 1172.    0.    0.    0.    0.]
#           [   0.    0.    0.    0.    0. 1172.    0.    0.    0.]
#           [   0.    0.    0.    0.    0.    0. 1172.    0.    0.]
#           [   0.    0.    0.    0.    0.    0.    0. 1172.    0.]
#           [   0.    0.    0.    0.    0.    0.    0.    0. 1172.]]
# F = [[1. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 1. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 1. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 1. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 1. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 1. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 1. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 1. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 1.]]
# Q = [[0.01 0.   0.   0.   0.   0.   0.   0.   0.  ]
#      [0.   0.01 0.   0.   0.   0.   0.   0.   0.  ]
#      [0.   0.   0.01 0.   0.   0.   0.   0.   0.  ]
#      [0.   0.   0.   0.01 0.   0.   0.   0.   0.  ]
#      [0.   0.   0.   0.   0.01 0.   0.   0.   0.  ]
#      [0.   0.   0.   0.   0.   0.01 0.   0.   0.  ]
#      [0.   0.   0.   0.   0.   0.   0.01 0.   0.  ]
#      [0.   0.   0.   0.   0.   0.   0.   0.01 0.  ]
#      [0.   0.   0.   0.   0.   0.   0.   0.   0.01]]
# R = [[0.1 0.  0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.1 0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.1 0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.1 0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.1 0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.1 0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.1 0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.1 0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.  0.1]]
# H = [[0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]]
# K = [[0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]]
# y = [ 1.14810000e+02  1.14580000e+02 -2.30000000e-01  8.42000000e+02
#       7.92000000e+02  2.60000000e+01  2.60000000e+01  3.23846154e+01
#       3.04615385e+01]
# S = [[0.1 0.  0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.1 0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.1 0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.1 0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.1 0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.1 0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.1 0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.1 0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.  0.1]]
# SI = [[10.  0.  0.  0.  0.  0.  0.  0.  0.]
#       [ 0. 10.  0.  0.  0.  0.  0.  0.  0.]
#       [ 0.  0. 10.  0.  0.  0.  0.  0.  0.]
#       [ 0.  0.  0. 10.  0.  0.  0.  0.  0.]
#       [ 0.  0.  0.  0. 10.  0.  0.  0.  0.]
#       [ 0.  0.  0.  0.  0. 10.  0.  0.  0.]
#       [ 0.  0.  0.  0.  0.  0. 10.  0.  0.]
#       [ 0.  0.  0.  0.  0.  0.  0. 10.  0.]
#       [ 0.  0.  0.  0.  0.  0.  0.  0. 10.]]
# M = [[0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]]
# B = None
# z = [ 1.14810000e+02  1.14580000e+02 -2.30000000e-01  8.42000000e+02
#       7.92000000e+02  2.60000000e+01  2.60000000e+01  3.23846154e+01
#       3.04615385e+01]
# log-likelihood = -6829331.079009148
# likelihood = 2.2250738585072014e-308
# mahalanobis = 3695.7632960446117
# alpha = 1.0
# inv = <function inv at 0x1036920c0>, KalmanFilter object
# dim_x = 9
# dim_z = 9
# dim_u = 0
# x = [0. 0. 0. 0. 0. 0. 0. 0. 0.]
# P = [[1172.    0.    0.    0.    0.    0.    0.    0.    0.]
#      [   0. 1172.    0.    0.    0.    0.    0.    0.    0.]
#      [   0.    0. 1172.    0.    0.    0.    0.    0.    0.]
#      [   0.    0.    0. 1172.    0.    0.    0.    0.    0.]
#      [   0.    0.    0.    0. 1172.    0.    0.    0.    0.]
#      [   0.    0.    0.    0.    0. 1172.    0.    0.    0.]
#      [   0.    0.    0.    0.    0.    0. 1172.    0.    0.]
#      [   0.    0.    0.    0.    0.    0.    0. 1172.    0.]
#      [   0.    0.    0.    0.    0.    0.    0.    0. 1172.]]
# x_prior = [0. 0. 0. 0. 0. 0. 0. 0. 0.]
# P_prior = [[1172.    0.    0.    0.    0.    0.    0.    0.    0.]
#            [   0. 1172.    0.    0.    0.    0.    0.    0.    0.]
#            [   0.    0. 1172.    0.    0.    0.    0.    0.    0.]
#            [   0.    0.    0. 1172.    0.    0.    0.    0.    0.]
#            [   0.    0.    0.    0. 1172.    0.    0.    0.    0.]
#            [   0.    0.    0.    0.    0. 1172.    0.    0.    0.]
#            [   0.    0.    0.    0.    0.    0. 1172.    0.    0.]
#            [   0.    0.    0.    0.    0.    0.    0. 1172.    0.]
#            [   0.    0.    0.    0.    0.    0.    0.    0. 1172.]]
# x_post = [0. 0. 0. 0. 0. 0. 0. 0. 0.]
# P_post = [[1172.    0.    0.    0.    0.    0.    0.    0.    0.]
#           [   0. 1172.    0.    0.    0.    0.    0.    0.    0.]
#           [   0.    0. 1172.    0.    0.    0.    0.    0.    0.]
#           [   0.    0.    0. 1172.    0.    0.    0.    0.    0.]
#           [   0.    0.    0.    0. 1172.    0.    0.    0.    0.]
#           [   0.    0.    0.    0.    0. 1172.    0.    0.    0.]
#           [   0.    0.    0.    0.    0.    0. 1172.    0.    0.]
#           [   0.    0.    0.    0.    0.    0.    0. 1172.    0.]
#           [   0.    0.    0.    0.    0.    0.    0.    0. 1172.]]
# F = [[1. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 1. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 1. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 1. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 1. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 1. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 1. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 1. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 1.]]
# Q = [[0.01 0.   0.   0.   0.   0.   0.   0.   0.  ]
#      [0.   0.01 0.   0.   0.   0.   0.   0.   0.  ]
#      [0.   0.   0.01 0.   0.   0.   0.   0.   0.  ]
#      [0.   0.   0.   0.01 0.   0.   0.   0.   0.  ]
#      [0.   0.   0.   0.   0.01 0.   0.   0.   0.  ]
#      [0.   0.   0.   0.   0.   0.01 0.   0.   0.  ]
#      [0.   0.   0.   0.   0.   0.   0.01 0.   0.  ]
#      [0.   0.   0.   0.   0.   0.   0.   0.01 0.  ]
#      [0.   0.   0.   0.   0.   0.   0.   0.   0.01]]
# R = [[0.1 0.  0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.1 0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.1 0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.1 0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.1 0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.1 0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.1 0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.1 0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.  0.1]]
# H = [[0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]]
# K = [[0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]]
# y = [ 0.  0. -0.  0.  0.  0.  0.  0.  0.]
# S = [[0.1 0.  0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.1 0.  0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.1 0.  0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.1 0.  0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.1 0.  0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.1 0.  0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.1 0.  0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.1 0. ]
#      [0.  0.  0.  0.  0.  0.  0.  0.  0.1]]
# SI = [[10.  0.  0.  0.  0.  0.  0.  0.  0.]
#       [ 0. 10.  0.  0.  0.  0.  0.  0.  0.]
#       [ 0.  0. 10.  0.  0.  0.  0.  0.  0.]
#       [ 0.  0.  0. 10.  0.  0.  0.  0.  0.]
#       [ 0.  0.  0.  0. 10.  0.  0.  0.  0.]
#       [ 0.  0.  0.  0.  0. 10.  0.  0.  0.]
#       [ 0.  0.  0.  0.  0.  0. 10.  0.  0.]
#       [ 0.  0.  0.  0.  0.  0.  0. 10.  0.]
#       [ 0.  0.  0.  0.  0.  0.  0.  0. 10.]]
# M = [[0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]
#      [0. 0. 0. 0. 0. 0. 0. 0. 0.]]
# B = None
# z = [ 0.  0. -0.  0.  0.  0.  0.  0.  0.]
# log-likelihood = 2.0911861196311516
# likelihood = 8.094510530398724
# mahalanobis = 0.0
# alpha = 1.0
# inv = <function inv at 0x1036920c0>]
# >>> model.
# model.em()               model.get_hidden_state() model.hmm                model.kf_models          model.n_features         model.observations       model.update(           
# model.estimate_liq()     model.get_state_probs()  model.is_fit             model.min_obvs           model.n_states           model.state_seq         
# >>> model.estimate_liq()
# array([0., 0., 0., 0., 0., 0., 0., 0., 0.])









# class OrderBook:
# 	def __init__ (self):
# 		# self.bids = defaultdict(lambda: defaultdict(int));
# 		# self.asks = defaultdict(lambda: defaultdict(int));
# 		self.bids = [];
# 		self.asks = [];
# 		self.bid_map = defaultdict(dict);
# 		self.ask_map = defaultdict(dict);
# 		self.last_trade = None;

# 	def _add_bid (self, price, size, exchange):
# 		if exchange in self.bid_map[price]:
# 			self._remove_bid(price, exchange);
# 		heapq.heappush(self.bids, (-price, size, exchange));
# 		self.bid_map[price][exchange] = size;

# 	def _add_ask (self, price, size, exchange):
# 		if exchange in self.ask_map[price]:
# 			del self.bid_map[price][exchange];
# 			if not self.bid_map[price]:
# 				del self.bid_map[price];
# 			self.bids = [b for b in self.bids if b[2] != exchange or -b[0] != price];
# 			heapq.heapify(self.bids);

# 	def _remove_bid (self, price, exchange):
# 		if exchange in self.bid_map[price]:
# 			del self.bid_map[price][exchange];
# 			if not self.bid_map[price]:
# 				del self.bid_map[price];
# 			self.bids = [b for b in self.bids if b[2] != exchange or -b[0] != price];
# 			heapq.heapify(self.bids);

# 	def _remove_ask(self, price, exchange):
# 		if exchange in self.ask_map[price]:
# 			del self.ask_map[price][exchange];
# 			if not self.ask_map[price]:
# 				del self.ask_map[price];
# 			self.asks = [a for a in self.asks if a[2] != exchange or a[0] != price];
# 			heapq.heapify(self.asks);

# 	def update (self, data):
# 		event = json.loads(data);
# 		if event['type'] == 'quote':
# 			self.update_quote(event);
# 		elif event['type'] in ['trade', 'tradex']:
# 			self.update_trade(event);
# 		elif event['type'] == 'timesale':
# 			self.update_timesale(event);

# 	def update_quote (self, quote):
# 		self._add_bid(float(quote['bid']), int(quote['bidsz']), quote['bidexch']);
# 		self._add_ask(float(quote['ask']), int(quote['asksz']), quote['askexch']);

# 	def update_trade (self, trade):
# 		self.last_trade = {
# 			'price': float(trade['price']),
# 			'size': int(trade['size']),
# 			'exchange': trade['exch']
# 		};

# 	def update_timesale (self, timesale):
# 		if not timesale['cancel'] and not timesale['correction']:
# 			self._add_bid(float(timesale['bid']), int(timesale['size']), timesale['exch']);
# 			self._add_ask(float(timesale['ask']), int(timesale['size']), timesale['exch']);

# 			self.last_trade = {
# 				'price': float(timesale['last']),
# 				'size': int(timesale['size']),
# 				'exchange': timesale['exch']
# 			};

# 	def get_best_bid (self):
# 		return (-self.bids[0][0], self.bids[0][1], self.bids[0][2]) if self.bids else None;

# 	def get_best_ask (self):
# 		return self.asks[0] if self.asks else None;

# 	def get_book_for_exchange (self, exchange):
# 		bids = sorted(
# 			[(price,size) for price, exch_sizes in self.bid_map.items() for exch,size in exch_sizes.items() if exch==exchange],
# 			reverse=True
# 		);
# 		asks = sorted(
# 			[(price,size) for price, exch_sizes in self.ask_map.items() for exch,size in exch_sizes.items() if exch == exchange]
# 		);

# 		return {
# 			'bids': bids,
# 			'asks': asks,
# 			'last_trade': self.last_trade
# 		};


















