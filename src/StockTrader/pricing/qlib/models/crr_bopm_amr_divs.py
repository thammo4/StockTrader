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
class CRRBinomialAmericanDividends(BasePricingModel):
    name = "crr_bopm_amr_divs"
    description = "CRR Binomial Options Pricing Model for American-Style Options on Dividend Paying Underlying"

    def configure(self, **kwargs) -> None:
        self.n_steps: int = kwargs.get("n_steps", 225)
        self.iv_config: BrentIVConfig = kwargs.get("iv_config", BrentIVConfig())

    def _build(self, option: OptionRow):
        return build_option_with_engine(
            option_type=option.option_type,
            S=option.S,
            K=option.K,
            r=option.r,
            q=option.q,
            σ=option.σ,
            market_date=option.market_date,
            expiry_date=option.expiry_date,
            n_steps=self.n_steps,
        )

    def price(self, option: OptionRow) -> PricingResult:
        pricing_result = PricingResult(occ=option.occ, model_name=self.name, n_steps=self.n_steps)

        t0 = time.time()

        #
        # NPV
        #

        try:
            ql_option, market_quote_handles = self._build(option)
            npv = ql_option.NPV()
            if not math.isfinite(npv):
                raise NPVCalculationError(message=f"Inf npv: {npv}", occ=option.occ)
            pricing_result.npv = npv
        except Exception as e:
            pricing_result.npv_err = f"{type(e).__name__}: {str(e)[:100]}"
            pricing_result.compute_ms = (time.time() - t0) * 1000
            return pricing_result

        #
        # Greeks - Δ, Γ, Θ
        #

        try:
            pricing_result.Δ = ql_option.delta()
            pricing_result.Γ = ql_option.gamma()
            pricing_result.Θ = ql_option.theta()
        except Exception as e:
            pricing_result.greek_err = f"{type(e).__name__}: {str(e)[:100]}"

        #
        # Implied Volatility
        #

        def f_reprice(σ: float) -> float:
            market_quote_handles.volQ.setValue(σ)
            return ql_option.NPV()

        σ_iv, σ_iv_err, _ = try_solve_implied_vol_brent(
            f_reprice, option.p_m, cfg=self.iv_config, occ=option.occ, σ_guess=option.σ
        )
        pricing_result.σ_iv = σ_iv
        pricing_result.σ_iv_err = σ_iv_err

        market_quote_handles.volQ.setValue(option.σ)

        pricing_result.compute_ms = (time.time() - t0) * 1000

        return pricing_result
