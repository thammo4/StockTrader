#
# FILE: `StockTrader/scripts/prep_bopm_data.py`
#

import os
import pandas as pd
import numpy as np
import QuantLib as ql
from scipy.optimize import root_scalar
from StockTrader.settings import STOCK_TRADER_MARKET_DATA, logger


#
# Helper Function to Log OCC Symbol-Related Activity
#

def occ_symbol(row):
	expr_str = row["expiration"].strftime("%y%m%d")
	option_type = "C" if row["call_put"].lower() == "call" else "P"
	strike = f"{int(row['strike']*1000):08d}"
	return f"{row['act_symbol']}{expr_str}{option_type}{strike}"


#
# Price American-Style Options with QuantLib
#

def price_bopm_data (symbol, crr_steps=225, return_df=False):
	"""
	Price American-style option contracts using QuantLib Cox-Ross-Rubinstein BOPM (BinomialVanillaEngine).
	Computes NPV and Greeks (Δ, Γ, Θ) for each contract (row) and appends values existing BOPM input dataset in place.
	"""

	#
	# Validate Input Data
	#

	fpath_parquet = os.path.join(STOCK_TRADER_MARKET_DATA, f"{symbol}_bopm_data.parquet")
	if not os.path.exists(fpath_parquet):
		logger.error(f"Missing bopm parquet symbol={symbol}. Expect: {fpath_parquet} [price_bopm_data]")
		return None

	#
	# Read BOPM Input DataFrame from Prepared Parquet
	#

	try:
		df = pd.read_parquet(fpath_parquet)
		if df.empty:
			logger.warning(f"Empty bopm parquet symbol={symbol}. Path: {fpath_parquet} [price_bopm_data]")
			return None
	except Exception as e:
		logger.error(f"Failed read bopm parquet symbol={symbol}: {str(e)} [price_bopm_data]")
		return None

	#
	# Initialize Cols for Computed Values
	#

	df["NPV"] = None
	df["Delta"] = None
	df["Gamma"] = None
	df["Theta"] = None
	# df["IV"] = None


	#
	# Configure Common Date Settings
	#

	calendar = ql.UnitedStates(ql.UnitedStates.NYSE)
	day_count = ql.Actual365Fixed()
	settlement_days = 2

	#
	# Compute NPV, Greeks for Each Contract
	#

	logger.info(f"Computing option prices symbol={symbol} [price_bopm_data]")

	for idx, row in df.iterrows():
		try:
			# logger.info(f"->Pricing: {occ_symbol(row)}")
			eval_date = ql.Date(row["date"].day, row["date"].month, row["date"].year)
			expiry_date = ql.Date(row["expiration"].day, row["expiration"].month, row["expiration"].year)
			settlement_date = calendar.advance(eval_date, ql.Period(settlement_days, ql.Days))

			ql.Settings.instance().evaluationDate = eval_date

			spotH = ql.QuoteHandle(ql.SimpleQuote(row["close"]))
			risk_freeTS = ql.YieldTermStructureHandle(
				ql.FlatForward(settlement_date, row["fred_rate"], day_count)
			)
			div_yieldTS = ql.YieldTermStructureHandle(
				ql.FlatForward(settlement_date, row["div_amt"]/row["close"], day_count)
			)
			volTS = ql.BlackVolTermStructureHandle(
				ql.BlackConstantVol(settlement_date, calendar, row["vol_estimate"], day_count)
			)

			bsm_process = ql.BlackScholesMertonProcess(spotH, div_yieldTS, risk_freeTS, volTS)

			option_type = ql.Option.Call if row["call_put"].lower() == "call" else ql.Option.Put
			payoff = ql.PlainVanillaPayoff(option_type, row["strike"])
			amr_exercise = ql.AmericanExercise(settlement_date, expiry_date)
			
			option_contract = ql.VanillaOption(payoff, amr_exercise)
			binom_engine = ql.BinomialVanillaEngine(bsm_process, "crr", crr_steps)
			option_contract.setPricingEngine(binom_engine)

			logger.info(f"Priced: {occ_symbol(row)} npv={option_contract.NPV():.2f}, Δ={option_contract.delta():.2f}, Γ={option_contract.gamma():.2f}, Θ={option_contract.theta():.2f}")

			df.loc[idx, ["NPV", "Delta", "Gamma", "Theta"]] = np.round([option_contract.NPV(), option_contract.delta(), option_contract.gamma(), option_contract.theta()], 4)


		except Exception as e:
			logger.warning(f"Failed to price {occ_symbol(row)}: {str(e)}")

	fpath_parquet = os.path.join(STOCK_TRADER_MARKET_DATA, f"{symbol}_bopm_priced.parquet")
	df.to_parquet(fpath_parquet, index=False, engine="pyarrow")

	logger.info(f"Created: {fpath_parquet}")

	if return_df:
		return df