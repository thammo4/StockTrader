# FILE: `StockTrader/MRK_vol_surface.py`
from BinomialOptions import *


def plot_vol_surface (X,Y,Z):
	figgy = plt.figure();
	ax = figgy.add_subplot(111, projection='3d');
	ax.plot_surface(X,Y,Z, cmap='viridis');

	ax.set_xlabel('Strike');
	ax.set_ylabel('TTM (Yrs)');
	ax.set_zlabel('IV');
	plt.title('MRK Volatility Surface [2024-08-02, 2026-12-18]');
	plt.show();

vol_surface_options = [];

# MRK Options Expiries
# ['2024-08-02', '2024-08-09', '2024-08-16', '2024-08-23', '2024-08-30', '2024-09-06', '2024-09-20', '2024-10-18', '2025-01-17', '2025-06-20', '2025-12-19', '2026-01-16', '2026-12-18']
mrk_expiries = options_data.get_expiry_dates('MRK');
print(f'MRK Options Expiries\n{mrk_expiries}');

for expiry in mrk_expiries:
	mrk = BinomialOptions(symbol='MRK', option_expiry_days=(datetime.strptime(expiry, '%Y-%m-%d')-datetime.today()).days);
	mrk.append_greeks_NPV();
	mrk.options_chain.sort_values(by=['option_type', 'strike'], ascending=[True, True], inplace=True);
	mrk.options_chain['Expiry'] = expiry;
	mrk.options_chain['TTM'] = (datetime.strptime(expiry, '%Y-%m-%d')-datetime.today()).days;

	vol_surface_options.append(mrk.options_chain);

df_vol_surface = pd.concat(vol_surface_options, ignore_index=True);
# >>> df_vol_surface
#                  symbol  strike    bid    ask     mp  bidsize  asksize  open_interest  volume option_type      NPV   Delta  Gamma   Theta      IV      Expiry  TTM
# 0    MRK240802C00070000    70.0  53.65  57.45  55.55       53       51              1       0        call  55.2616  0.9999  0.000 -0.5863  5.7739  2024-08-02    3
# 1    MRK240802C00075000    75.0  48.65  52.50  50.58       53       50              0       0        call  50.2623  0.9999  0.000 -0.8482  5.2547  2024-08-02    3
# 2    MRK240802C00080000    80.0  43.70  47.30  45.50       51       51              0       0        call  45.2630  0.9999  0.000 -1.1102  4.4446  2024-08-02    3
# 3    MRK240802C00085000    85.0  38.70  42.30  40.50       51       52              0       0        call  40.2638  0.9999  0.000 -1.3722  3.9145  2024-08-02    3
# 4    MRK240802C00090000    90.0  33.70  37.30  35.50       52       52              0       0        call  35.2645  0.9999  0.000 -1.6341  3.4154  2024-08-02    3
# ..                  ...     ...    ...    ...    ...      ...      ...            ...     ...         ...      ...     ...    ...     ...     ...         ...  ...
# 949  MRK261218P00175000   175.0  47.00  52.00  49.50       67       69              0       0         put  49.7400 -1.0000  0.002  5.5920     NaN  2026-12-18  871
# 950  MRK261218P00180000   180.0  52.55  57.00  54.78       55       52              0       0         put  54.7400 -1.0000  0.000  6.3506  0.2186  2026-12-18  871
# 951  MRK261218P00185000   185.0  57.50  61.90  59.70       52       51              0       0         put  59.7400 -1.0000  0.000  6.6126     NaN  2026-12-18  871
# 952  MRK261218P00190000   190.0  62.05  67.00  64.53       50       56              0       0         put  64.7400 -1.0000  0.000  6.8746     NaN  2026-12-18  871
# 953  MRK261218P00195000   195.0  67.60  72.00  69.80       51       51              0       0         put  69.7400 -1.0000  0.000  7.1366  0.2556  2026-12-18  871
#
# [954 rows x 17 columns]

strikes = sorted(df_vol_surface['strike'].unique());
ttms = sorted(df_vol_surface['TTM'].unique());

X, Y = np.meshgrid(strikes, ttms);
Z = np.zeros_like(X);

for i, K in enumerate(strikes):
	for j, T in enumerate(ttms):
		iv_values = df_vol_surface.loc[(df_vol_surface['strike'] == K) & (df_vol_surface['TTM'] == T)]['IV']
		if not iv_values.empty:
			Z[j,i] = iv_values.mean();