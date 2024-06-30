import numpy as np;
import pandas as pd;
import matplotlib.pyplot as plt;
import yfinance as yf;

def log_returns (tickers, start_date=None, end_date=None):
	adj_close_data = yf.download(tickers, start=start_date, end=end_date)['Adj Close'];
	log_returns = np.log(adj_close_data / adj_close_data.shift(1));
	return log_returns.dropna();


def fourier_power (log_returns):
	n = len(log_returns);
	freqs = np.fft.fftfreq(n);
	mask = freqs > 0;
	fft_values = np.fft.fft(log_returns);

	# Compute power as |FourierXform|^2
	power = np.abs(fft_values)**2;
	return freqs[mask], power[mask];


def simulate_power_spectrum (n, length):
	simulated_statistics = [];
	for _ in range(n):
		gauss_noise = np.random.normal(0,1,length);
		gauss_noise_fft = np.fft.fft(gauss_noise);
		gauss_noise_power = np.abs(gauss_noise_fft)**2;
		simulated_statistics.append(np.max(gauss_noise_power));

	return simulated_statistics;



#
# Run example with ConocoPhillips
#

cop_log_returns = log_returns(tickers='COP', start_date='2024-06-01', end_date='2024-06-28');
cop_fourier_power = fourier_power(cop_log_returns);
cop_simulation = simulate_power_spectrum(n=10000, length=len(cop_log_returns));

#
# Compute p-value
#

cop_test_stat = np.max(cop_fourier_power[1]);
p_value = np.mean([stat >= cop_test_stat for stat in cop_simulation]);


print(f"COP Power p-value: {p_value}.");

