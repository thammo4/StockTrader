#
# FILE: `StockTrader/src/StockTrader/pricing/qlib/implied_vol.py`
#

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple
import math

from scipy.optimize import brentq

from StockTrader.pricing.errors import (
	InputValidationError,
	IVBracketError,
	IVConvergeError,
	QLibError
)

# @dataclass(frozen=true)
@dataclass(frozen=True)
class BrentIVConfig:
	σ_lower: float = 1e-6
	σ_upper: float = 1.0
	σ_upper_max: float = 10.0

	expand_factor: float = 1.6
	expand_max: int = 25

	xtol: float = 1e-10
	rtol: float = 1e-8
	maxiter: int = 100

	objective_atol: float = 1e-6


# def _require_scipy() -> None:
# 	if brentq is None:
# 		raise ImportError("need scipy", "install scipy in pricing worker env with pip")


def _is_finite (x: float) -> bool:
	return x is not None and isinstance(x, (int,float)) and math.isfinite(float(x))

def _eval_objective (f_price: Callable[[float], float], σ: float, price_target: float, occ: Optional[str]) -> float:
	try:
		px = f_price(float(σ))
	except RuntimeError as e:
		raise QLibError.from_runtime_error(e, occ=occ)
	except Exception as e:
		raise QLibError(message=f"f_price failed, {type(e).__name__}: {str(e)}", occ=occ)

	if not _is_finite(px):
		raise QLibError(message=f"Non-finite model price at σ={σ}: {px}", occ=occ)

	return float(px) - float(price_target)


def _initial_bracket (cfg: BrentIVConfig, σ_guess: Optional[float]) -> Tuple[float, float]:
	lower = max(float(cfg.σ_lower), 1e-12)
	upper = float(cfg.σ_upper)

	if σ_guess is not None and _is_finite(σ_guess) and float(σ_guess) > 0.0:
		# upper = max(upper, float(σ_upper)*2.0)
		upper = max(upper, float(σ_guess)*2.0)
		lower = max(lower, float(σ_guess)/10.0)

	upper = min(upper, float(cfg.σ_upper_max))
	if upper <= lower:
		upper = min(float(cfg.σ_upper_max), lower * 1.01)

	return lower, upper

def bracket_root (f_price: Callable[[float], float], price_target: float, *, cfg: BrentIVConfig, occ: Optional[str] = None, σ_guess: Optional[float] = None) -> Tuple[float, float, float, float]:
	σ_lower, σ_upper = _initial_bracket(cfg, σ_guess)

	f_lower = _eval_objective(f_price, σ_lower, price_target, occ=occ)
	if abs(f_lower) <= cfg.objective_atol:
		return σ_lower, σ_lower, f_lower, f_lower

	f_upper = _eval_objective(f_price, σ_upper, price_target, occ=occ)
	if abs(f_upper) <= cfg.objective_atol:
		return σ_upper, σ_upper, f_upper, f_upper

	if f_lower*f_upper < 0.0:
		return σ_lower, σ_upper, f_lower, f_upper

	expand_steps = 0
	while expand_steps < cfg.expand_max and σ_upper < cfg.σ_upper_max and f_lower*f_upper>0.0:
		expand_steps += 1
		σ_upper = min(cfg.σ_upper_max, σ_upper*cfg.expand_factor)

		f_upper = _eval_objective(f_price, σ_upper, price_target, occ=occ)
		if abs(f_upper) <= cfg.objective_atol:
			return σ_upper, σ_upper, f_upper, f_upper

		if f_lower*f_upper<0.0:
			return σ_lower, σ_upper, f_lower, f_upper

	raise IVBracketError(
		message="IV bracketing failed: could not find σ: sgn(f_lower)*sgn(f_upper) nonnegative",
		occ=occ,
		details={
			"price_target": price_target,
			"σ_lower": σ_lower,
			"σ_upper": σ_upper,
			"f_lower": f_lower,
			"f_upper": f_upper,
			"σ_upper_max": cfg.σ_upper_max,
			"expand_factor": cfg.expand_factor,
			"expand_max": cfg.expand_max
		}
	)


def solve_implied_vol_brent(f_price: Callable[[float], float], price_target: float, *, cfg: Optional[BrentIVConfig] = None, occ: Optional[str] = None, σ_guess: Optional[float] = None) -> float:
	# _require_scipy()
	cfg = cfg or BrentIVConfig()

	if not _is_finite(price_target) or float(price_target) < 0.0:
		raise InputValidationError(
			message="Invalid price_target for implied vol solver",
			occ=occ,
			details={"price_target":price_target}
		)

	σ_lower, σ_upper, f_lower, f_upper = bracket_root(
		f_price,
		float(price_target),
		cfg=cfg,
		occ=occ,
		σ_guess=σ_guess
	)

	if σ_lower == σ_upper:
		root = float(σ_lower)
		if root <= 0.0 or not _is_finite(root):
			raise IVConvergeError(message="Endpoint root non-finite or non-positive", occ=occ, details={"root":root})
		return root

	try:
		root = brentq(
			lambda s: _eval_objective(f_price, s, float(price_target), occ=occ),
			a=float(σ_lower),
			b=float(σ_upper),
			xtol=float(cfg.xtol),
			rtol=float(cfg.rtol),
			maxiter=int(cfg.maxiter)
		)
	except ValueError as e:
		raise IVBracketError(
			message=f"brentq rejected bracket: {str(e)}",
			occ=occ,
			details={"σ_lower":σ_lower, "σ_upper":σ_upper, "f_lower":f_lower, "f_upper":f_upper, "price_target":price_target}
		)
	except RuntimeError as e:
		raise IVConvergeError(
			message=f"brentq converge fail: {str(e)}",
			occ=occ,
			details={"σ_lower":σ_lower, "σ_upper":σ_upper, "f_lower":f_lower, "f_upper":f_upper, "price_target":price_target}
		)

	if not _is_finite(root) or float(root) <= 0.0:
		raise IVConvergeError(
			message="Implied volatility root non-finite or non-positive",
			occ=occ,
			details={"root":root, "σ_lower":σ_lower, "σ_upper":σ_upper, "price_target":price_target}
		)

	return float(root)


def try_solve_implied_vol_brent(f_price: Callable[[float], float], price_target: float, *, cfg: Optional[BrentIVConfig] = None, occ: Optional[str] = None, σ_guess: Optional[float] = None) -> Tuple[Optional[float], Optional[str], Dict[str,Any]]:
	try:
		σ = solve_implied_vol_brent(
			f_price,
			price_target,
			cfg=cfg,
			occ=occ,
			σ_guess=σ_guess
		)
		return σ, None, {"price_target":price_target}
	except Exception as e:
		return None, f"{type(e).__name__}: {str(e)}", getattr(e, "details", {}) or {"price_target":price_target}
