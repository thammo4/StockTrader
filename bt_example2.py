from config import *

class LogReturns (bt.Indicator):
	lines = ('log_returns',);

	def __init__ (self):
		self.addminperiod(2);

	def next (self):
		self.lines.log_returns[0] = np.log(self.data[0] / self.data[-1]);

class TTestLogReturns (bt.Strategy):
	params = (
		('w_short', 5),
		('w_long', 50),
		('alpha_buy', .075),
		('alpha_sell', .125),
		('persist', 5)
	);

	def __init__ (self):
		self.log_returns = LogReturns(self.data.close);
		self.mu_short = bt.indicators.SMA(self.log_returns, period=self.p.w_short);
		self.mu_long = bt.indicators.SMA(self.log_returns, period=self.p.w_long);
		self.sigma_long = bt.indicators.StdDev(self.log_returns, period=self.p.w_long);

		# self.signal = 0;
		# self.signal_count = 0;
		self.signal = 0;
		self.signal_count = 0;
		self.average_buy_price = 0;
		self.total_buy_size = 0;

	def next (self):
		if len(self.log_returns) < self.p.w_long:
			return;

		t_stat = (self.mu_short[0] - self.mu_long[0]) / (self.sigma_long[0] / np.sqrt(self.p.w_long));
		p_value = 2 * (1-stats.t.cdf(abs(t_stat), self.p.w_long - 1));

		if p_value < self.p.alpha_buy and t_stat > 0:
			self.signal = 1;
			self.signal_count = self.p.persist;
		elif p_value < self.p.alpha_sell and t_stat < 0:
			self.signal = -1;
			self.signal_count = self.p.persist;
		elif self.signal_count > 0:
			self.signal_count -= 1;
		else:
			self.signal = 0;

		if self.signal == 1 and self.broker.getcash() > 100*self.data.close[0]:
			self.buy();
		elif self.signal == -1 and self.position and self.data.close[0] > self.average_buy_price:
			self.close();

	def buy (self, **kwargs):
		order = super().buy(**kwargs);
		if order.status == order.Accepted:
			self.total_buy_size += order.size;
			self.average_buy_price = ((self.average_buy_price *(self.total_buy_size-order.size)) + (order.executed_price*order.size)) / self.total_buy_size;
		return order;

	def log (self, txt, dt=None):
		dt = dt or self.datas[0].datetime.date(0);
		print(f"{dt.isoformat()} {txt}");

	def notify_order (self, order):
		if order.status in [order.Submitted, order.Accepted]:
			return;

		if order.status in [order.Completed]:
			if order.isbuy():
				self.log(f"BUY EXEC, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}");
			else:
				self.log(f"SELL EXEC, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm: {order.executed.comm:.2f}");
		elif order.status in [order.Canceled, order.Margin, order.Rejected]:
			self.log('Order Canceled/Margin/Rejected');


cerebro = bt.Cerebro();
cerebro.addstrategy(TTestLogReturns);

# stt = yf.download('STT', '2021-01-01', '2023-01-01');
# stt['LogClose'] = np.log(stt['Close']);
# stt['LogReturn'] = stt['LogClose'].diff();
# stt.dropna(inplace=True);
# data = bt.feeds.PandasData(dataname=stt);
# cerebro.adddata(data);

symbols = ['STT', 'MSCI'];
for x in symbols:
	data = yf.download(x, '2021-01-01', '2023-06-01');
	data['LogClose'] = np.log(data['Close']);
	data['LogReturn'] = data['LogClose'].diff();
	data.dropna(inplace=True);
	datafeed = bt.feeds.PandasData(dataname=data);
	cerebro.adddata(datafeed);

cerebro.broker.setcash(100000.0);
cerebro.addsizer(bt.sizers.PercentSizer, percents=9.5);
# cerebro.addsizer(bt.sizers.FixedSize, stake=100);
# cerebro.broker.setcommission(commission=.001);

# cerebro.addanalyzer(bt.analyzers.Cash, _name='cash');
cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe');
cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown');
cerebro.addanalyzer(bt.analyzers.Returns, _name='returns');

print(f"Portfolio Initial: {cerebro.broker.getvalue():.2f}");
results = cerebro.run();
print(f"Portfolio Final: {cerebro.broker.getvalue():2f}");

strat = results[0];
print(f"Sharpe: {strat.analyzers.sharpe.get_analysis()['sharperatio']:.3f}");
print(f"MDD: {strat.analyzers.drawdown.get_analysis()['max']['drawdown']:.3f}");
print(f"Total Return: {strat.analyzers.returns.get_analysis()['rtot']:.2f}%");

# print('STRAT');
# print(strat);
# print(dir(strat));
# print("\n");
# print('RESULTS');
# print(results);
# print(dir(results));

cerebro.plot(style='candlestick');



#
# OUTPUT
#


# Portfolio Final: 102824.968954
# Sharpe: -0.019
# MDD: 16.779
# Total Return: 0.03%