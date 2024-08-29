from config import *

data = yf.download('STT', start='2021-01-01', end='2022-01-01');

#
# Define trading strategy as subclass of bt.Strategy
#

class MovingAverageCrossStrategy(bt.Strategy):
	params = (('short_period', 40), ('long_period', 100));

	def __init__ (self):
		self.short_mavg = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.short_period);
		self.long_mavg = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.long_period);

	def next (self):
		if self.short_mavg[0] > self.long_mavg[0]: # and self.position.size == 0:
			self.buy();
		elif self.short_mavg[0] < self.long_mavg[0] and self.position.size > 0:
			self.sell();

#
# Setup Cerebro Engine backtrader environment
#

cerebro = bt.Cerebro();
cerebro.addstrategy(MovingAverageCrossStrategy);

data_feed = bt.feeds.PandasData(dataname=data);

cerebro.adddata(data_feed);

cerebro.broker.set_cash(100000);

print(f'Portfolio Start: {cerebro.broker.getvalue():.2f}');








#
# Run Backtest
#

cerebro.run();
print(f"Porfolio End: {cerebro.broker.getvalue():.2f}");


cerebro.plot();