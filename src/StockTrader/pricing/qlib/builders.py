#
# FILE: `StockTrader/src/StockTrader/pricing/qlib/builders.py`
#

"""
QuantLib Object Builders

Provides standardized interface to implement QuantLib objects commonly employed during options pricing.
"""
import QuantLib as ql

from dataclasses import dataclass
from datetime import date
from typing import Tuple

from StockTrader.pricing.qlib.context import get_context


@dataclass
class MarketQuoteHandles:
    spotQ: ql.SimpleQuote
    volQ: ql.SimpleQuote


def build_amr_exercise(eval_date: ql.Date, expiry_date: ql.Date) -> ql.AmericanExercise:
    return ql.AmericanExercise(eval_date, expiry_date)


def build_bsm_process(
    spotH: ql.QuoteHandle,
    rateH: ql.YieldTermStructureHandle,
    divH: ql.YieldTermStructureHandle,
    volH: ql.BlackVolTermStructureHandle,
) -> ql.BlackScholesMertonProcess:
    return ql.BlackScholesMertonProcess(spotH, divH, rateH, volH)


def build_crr_binom_engine(process: ql.BlackScholesMertonProcess, n_steps: int) -> ql.BinomialVanillaEngine:
    return ql.BinomialVanillaEngine(process, "crr", n_steps)


def build_market_data_handles(eval_date: ql.Date, S: float, r: float, q: float, σ: float) -> Tuple[
    ql.QuoteHandle,
    ql.YieldTermStructureHandle,
    ql.YieldTermStructureHandle,
    ql.BlackVolTermStructureHandle,
    MarketQuoteHandles,
]:
    ctx = get_context()

    spotQ = ql.SimpleQuote(S)
    volQ = ql.SimpleQuote(σ)

    spotH = ql.QuoteHandle(spotQ)

    rateH = ql.YieldTermStructureHandle(ql.FlatForward(eval_date, r, ctx.day_counter))
    divH = ql.YieldTermStructureHandle(ql.FlatForward(eval_date, q, ctx.day_counter))

    volTS = ql.BlackConstantVol(eval_date, ctx.calendar, ql.QuoteHandle(volQ), ctx.day_counter)
    volH = ql.BlackVolTermStructureHandle(volTS)

    return spotH, rateH, divH, volH, MarketQuoteHandles(spotQ=spotQ, volQ=volQ)


def build_option_with_engine(
    option_type: str,
    S: float,
    K: float,
    r: float,
    q: float,
    σ: float,
    market_date: date,
    expiry_date: date,
    n_steps: int = 225,
) -> Tuple[
    ql.VanillaOption,
    # ql.BlackScholesMertonProcess,
    MarketQuoteHandles,
]:
    ctx = get_context()

    eval_date = ctx.set_eval_date(market_date)
    expr_date = ctx.to_ql_date(expiry_date)

    payoff = build_payoff(option_type, K)
    exercise = build_amr_exercise(eval_date, expr_date)
    option = build_vanilla_option(payoff, exercise)

    spotH, rateH, divH, volH, market_quote_handles = build_market_data_handles(eval_date, S, r, q, σ)

    bsm_process = build_bsm_process(spotH, rateH, divH, volH)

    engine = build_crr_binom_engine(bsm_process, n_steps)
    option.setPricingEngine(engine)

    # return option, bsm_process, market_quote_handles
    return option, market_quote_handles


def build_payoff(option_type: str, K: float) -> ql.PlainVanillaPayoff:
    ql_type = ql.Option.Call if option_type.lower() == "call" else ql.Option.Put
    return ql.PlainVanillaPayoff(ql_type, K)


def build_vanilla_option(payoff: ql.PlainVanillaPayoff, exercise: ql.Exercise) -> ql.VanillaOption:
    return ql.VanillaOption(payoff, exercise)
