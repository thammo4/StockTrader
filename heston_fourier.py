from config import * # imports all of the needed packages

def heston_characteristic (u, T, S0, V0, kappa, theta, sigma, rho, r):
	try:
		def complex_sqrt (x):
			return cmath.sqrt(x);

		a = kappa * theta;
		b = kappa;
		d = complex_sqrt((rho*sigma*u*1j -b)**2 - sigma**2 * (u*1j - u**2));
		g = (b - rho*sigma*u*1j + d) / (b - rho*sigma*u*1j - d);
		C = r*u*1j*T + a/sigma**2*(
			(b - rho*sigma*u*1j + d)*T - 2*np.log((1 - g*np.exp(d*T)) / (1-g))
		);
		D = (b - rho*sigma*u*1j + d)/sigma**2*((1-np.exp(d*T))/(1-g*np.exp(d*T)));

		char_func = np.exp(C + D*V0 + 1j*u*np.log(S0));
		return char_func;
	except Exception as e:
		print(f"ERROR [heston_characteristic]: {e}");
		return np.nan;


don# scipy.integrate.quad(func, a, b, args=(), full_output=0, epsabs=1.49e-08, epsrel=1.49e-08, limit=50, points=None, weight=None, wvar=None, wopts=None, maxp1=50, limlst=50, complex_func=False)
def heston_call_price (S0, K, T, r, V0, kappa, theta, sigma, rho):
	def integrand (u):
		chacteristic = heston_characteristic(u - 1j*0.5, T, S0, V0, kappa, theta, sigma, rho, r);
		return np.real(np.exp(-1j*u*np.log(K)) * chacteristic/(1j*u));

	integral, error = integrate.quad(func=integrand, a=0, b=100, limit=1000, epsabs=1e-8, epsrel=1e-8);
	price = S0 - np.sqrt(S0*K)*np.exp(-r*T) * integral / np.pi;

	return price;




underlying, strike, time_to_expiry = 100, 100, 1;
risk_free, V0, kappa, theta, sigma, rho = 0.03, 0.04, 2, 0.04, 0.3, -0.7;

call_option_price = heston_call_price (underlying, strike, time_to_expiry, risk_free, V0, kappa, theta, sigma, rho);


#
# OUTPUT
#

# >>> call_option_price
# -90.89672793593977