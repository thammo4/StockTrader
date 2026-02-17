#
# FILE: `StockTrader/src/StockTrader/pricing/qlib/models/crr_bopm_amr_divs.py`
#


import time
import math

from StockTrader.pricing.base import BasePricingModel
from StockTrader.pricing.registry import register_model
from StockTrader.pricing.types import OptionRow, PricingResult
from StockTrader.pricing.errors import NPVCalculationError
from StockTrader.pricing.qlib.builders import build_option_with_engine
from StockTrader.pricing.qlib.implied_vol import BrentIVConfig, try_solve_implied_vol_brent

@register_model
class CRRBinomialAmericanDividends (BasePricingModel):
	name = "crr_bopm_amr_divs"
	description = "CRR Binomial Options Pricing Model for American-Style Options on Dividend Paying Underlying"

	def configure (self, **kwargs) -> None:
		self.n_steps: int = kwargs.get("n_steps", 225)
		self.iv_config: BrentIVConfig=kwargs.get("iv_config", BrentIVConfig())

	def _build (self, option:OptionRow):
		opt, _ = build_option_with_engine(
			option_type=option.option_type,
			S=option.S,
			K=option.K,
			r=option.r,
			q=option.q,
			σ=option.σ,
			market_date=option.market_date,
			expiry_date=option.expiry_date,
			n_steps=option.n_steps
		)
		return opt

	def price (self, option:OptionRow) -> PricingResult:
		pass