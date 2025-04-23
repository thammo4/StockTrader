import QuantLib as ql
from datetime import datetime


class AnalyzeOption:
    def __init__(
        self, calendar=ql.UnitedStates(ql.UnitedStates.NYSE), day_count=ql.Actual365Fixed(), settlement_days=2
    ):
        self.calendar = calendar
        self.day_count = day_count
        self.settlement_days = settlement_days

    def price_amr_bopm(self, S, K, q, r, σ, CP, T, t=datetime.today(), crr_steps=225):
        eval_date = ql.Date(t.day, t.month, t.year)
        expiry_date = ql.Date(T.day, T.month, T.year)
        settlement_date = self.calendar.advance(eval_date, ql.Period(self.settlement_days, ql.Days))

        ql.Settings.instance().evaluationDate = eval_date

        spotH = ql.QuoteHandle(ql.SimpleQuote(S))
        risk_freeTS = ql.YieldTermStructureHandle(ql.FlatForward(settlement_date, r, self.day_count))
        div_yieldTS = ql.YieldTermStructureHandle(ql.FlatForward(settlement_date, q / S, self.day_count))
        σTS = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(settlement_date, self.calendar, σ, self.day_count))

        bsm_process = ql.BlackScholesMertonProcess(spotH, div_yieldTS, risk_freeTS, σTS)
        option_type = ql.Option.Call if CP.lower() == "call" else ql.Option.Put
        payoff = ql.PlainVanillaPayoff(option_type, K)

        amr_exercise = ql.AmericanExercise(settlement_date, expiry_date)

        option_contract = ql.VanillaOption(payoff, amr_exercise)
        binom_engine = ql.BinomialVanillaEngine(bsm_process, "crr", crr_steps)
        option_contract.setPricingEngine(binom_engine)

        return option_contract
