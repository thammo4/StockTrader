# FILE: `StockTrader/StrategyVRP.py`
from config import *  # imports all necessary modules and variables
from BinomialOptions import BinomialOptions
import logging;


class StrategyVRP (BinomialOptions):
	def __init__(self, symbol, past_bars=252, option_expiry_days=30, option_expiry_date=None, itm_percent=.025):
		super().__init__(symbol, past_bars, option_expiry_days, option_expiry_date, itm_percent);
		self.setup_logging();
		self.process_data();
		self.set_itm_options();

	def setup_logging(self):
		logging.basicConfig(
			filename=f'{self.symbol}_VRPStrategy.log',
			level=logging.INFO,
			format='%(asctime)s - %(levelname)s - %(message)s'
		);

	def process_data (self):
		self.cwt_vol();
		self.categorize_vol();
		self.garch_forecast = self.garch_vol_forecast();
		self.options_chain.dropna(subset='IV', inplace=True);
		self.compute_vrp();

	def cwt_vol (self, wavelet_type='morl'):
		scales = np.arange(1, 128);
		coefs, freqs = pywt.cwt(self.bars['vol_historical'], scales, wavelet_type);
		wavelet_features = pd.DataFrame({
			'cwt_power_mean': np.mean(np.abs(coefs), axis=0),
			'cwt_power_std': np.std(np.abs(coefs), axis=0)
		}, index=self.bars.index);
		self.bars = pd.concat([self.bars, wavelet_features], axis=1);

	def categorize_vol (self, window=None):
		if window is None:
			window = self.days_to_expiry;

		rolling_mean = self.bars['cwt_power_mean'].rolling(window=window).mean();
		rolling_std = self.bars['cwt_power_mean'].rolling(window=window).std();

		self.bars['vol_category'] = 'Medium';
		self.bars.loc[self.bars['cwt_power_mean'] > (rolling_mean + rolling_std), 'vol_category'] = 'High';
		self.bars.loc[self.bars['cwt_power_mean'] < (rolling_mean - rolling_std), 'vol_category'] = 'Low';

	def garch_vol_forecast (self, P=1, Q=1):
		garch_model = arch_model(self.bars['vol_historical'], vol='Garch', p=P, q=Q);
		garch_fit = garch_model.fit(disp='off');
		forecast = garch_fit.forecast(horizon=self.days_to_expiry, start=self.bars.index[0]);
		return np.sqrt(forecast.variance.iloc[-1]);

	def compute_vrp (self):
		logging.info(f"Mean GARCH Vol: {self.garch_forecast.mean()}");
		self.options_chain['VRP'] = self.options_chain['IV'] - self.garch_forecast.mean();


	def trade_signal (self, vrp_threshold=0.05):
		max_vrp_option = self.near_itm.loc[self.near_itm['VRP'].idxmax()];

		if max_vrp_option['VRP'] > vrp_threshold:
			# signal = {
			self.signal = {
				'symbol': max_vrp_option['symbol'],
				'action': 'sell_to_open',
				'quantity': 1,
				'vrp': max_vrp_option['VRP'],
				'strike': max_vrp_option['strike'],
				'option_type': max_vrp_option['option_type']
			};
			logging.info(f"TRADESIG: {self.signal}");
			logging.info("Ok to exec order.");
			# return signal;
		else:
			self.signal = None;
			logging.info(f"No trade signal.");
			# return None;
		return self.signal;

	# def trade_contract (self, contract, options_order):
	def trade_contract (self):
		if self.signal is None:
			logging.info('No trade signal');
			return None;

		try:
			# result = options_order.options_order(
			# 	occ_symbol = contract['symbol'],
			# 	order_type = 'market',
			# 	side = contract['action'],
			# 	quantity = contract['quantity'],
			# 	duration = 'day'
			# );
			result = options_order.options_order(
				occ_symbol = self.signal['symbol'],
				order_type = 'market',
				side = self.signal['action'],
				quantity = self.signal['quantity'],
				duration = 'day'
			);
			logging.info(f"Executed Trade: {contract}");
			logging.info(f"Trade result: {result}");
			self.signal = None;
			return result;
		except Exception as e:
			logging.error(f"TRADEEXEC ERROR: {e}");
			return None;

	def track_strategy (self, account):
		try:
			balance = account.get_account_balance();
			positions = account.get_positions(options=True);
			gainloss = account.get_gainloss();

			logging.info(f"Account Balance Info");
			logging.info(f"Equity: ${balance['total_equity'].values[0]:.2f}");
			logging.info(f"Option Long: ${balance['option_long_value'].values[0]:.2f}");
			logging.info(f"Option Short: ${balance['option_short_value'].values[0]:.2f}");
			logging.info(f"Total Cash: ${balance['total_cash'].values[0]:.2f}");

			if not positions.empty:
				logging.info("Current Option Positions");
				for _, position in positions.iterrows():
					logging.info(f"Symbol: {position['symbol']}, Quantity: {position['quantity']}, Cost Basis: ${position['cost_basis']:.2f}");
			else:
				logging.info("No positions");

			if not gainloss.empty:
				gainloss_recent = gainloss.sort_values('close_date', ascending=False).head(7);
				logging.info('Recent Closed Positions');
				for _, trade in gainloss_recent.iterrows():
					logging.info(f"Symbol: {trade['symbol']}, G/L: ${trade['gain_loss']:.2f}, Percent: {trade['gain_loss_percent']:.2f}%");
			else:
				logging.info('No recent closed positions');

			return {
				'balance': balance,
				'positions': positions,
				'recent_gainloss': gainloss.sort_values('close_date', ascending=False).head(7) if not gainloss.empty else None
			};
		except Exception as e:
			logging.error(f"PERFORMTRACK ERROR: {e}");
			return None;



#
# Example Usage - Wells Fargo (WFC)
#

# >>> wfc.near_itm
#                 symbol  strike   bid   ask    mp  bidsize  asksize  open_interest  volume option_type     NPV   Delta   Gamma   Theta      IV       VRP
# 28  WFC240920P00055000    55.0  1.13  1.15  1.14       10       14          10798    1030         put  1.5592 -0.4440  0.0893 -7.3181  0.1839  0.088287
# 29  WFC240920C00055000    55.0  1.75  1.77  1.76       12        5          10182    3092        call  2.0209  0.5563  0.0882 -8.4664  0.2056  0.109987

wfc = StrategyVRP(symbol='WFC', past_bars=365);


# >>> wfc.trade_signal(vrp_threshold=.1)
# {'symbol': 'WFC240920C00055000', 'action': 'sell_to_open', 'quantity': 1, 'vrp': 0.10998659929203047, 'strike': 55.0, 'option_type': 'call'}
wfc.trade_signal(vrp_threshold=.10);
wfc.trade_contract();

# Confirm trade went through
# >>> acct.get_orders()
#          id    type symbol          side  quantity   status duration  ...  last_fill_price  last_fill_quantity  remaining_quantity               create_date          transaction_date   class       option_symbol
# 0  13681745  market    WFC  sell_to_open       1.0  pending      day  ...              0.0                 0.0                 1.0  2024-08-19T01:52:53.997Z  2024-08-19T01:52:54.127Z  option  WFC240920C00055000
acct.get_orders();