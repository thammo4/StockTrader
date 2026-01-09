#
# FILE: `StockTrader/src/StockTrader/pricing/errors.py`
#

"""
Custom exception handling for pricing engine.
"""

from typing import Optional, Dict, Any

class PricingError(Exception):
	"""
	Base exception for pricing-related errors.
	"""

	error_code: str = "PRICING_ERROR"

	def __init__ (self, message: str, occ: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
		self.message = message
		self.occ = occ
		self.details = details or {}

		super().__init__(self._format_message())

	def _format_message (self) -> str:
		base = f"[{self.error_code}] {self.message}"
		if self.occ:
			base = f"{base} (occ={self.occ})"
		return base

	def to_dict (self) -> Dict[str, Any]:
		return {
			"error_code": self.error_code,
			"message": self.message,
			"occ": self.occ,
			"details": self.details
		}

class ModelNotFoundError (PricingError):
	"""Raised when stupid enough to request nonexistent pricing model"""
	error_code = "MODEL_NOT_FOUND_ERROR"

class ModelConfigurationError (PricingError):
	"""Raised when careless enough to let model contain invalid config"""
	error_code = "MODEL_CONFIG_ERROR"

class InputValidationError (PricingError):
	"""
	Raised when input parameters fail validation.
	Recoverable - e.g. batch processor to log and move on.
	"""
	error_code = "INPUT_VALIDATION_ERROR"

class NPVCalculationError (PricingError):
	"""
	Raised when QuantLib fails to compute NPV.
	Common Issues:
		- Numerical unstable
		- QuantLib internal errror
		- Invalid date config
	"""
	error_code = "NPV_CALCULATION_ERROR"

class GreeksCalculationError (PricingError):
	"""Raised when greek computation fails (e.g. Δ, Γ, Θ, ρ, ν)"""
	error_code = "GREEKS_CALCULATION_ERROR"

class IVBracketError (PricingError):
	"""
	Raised when root finder cannot bracket IV solution.
	Often indicates market price outside bounds within which volatility solution exists.
	"""
	error_code = "IV_BRACKET_ERROR"

class IVConvergeError (PricingError):
	"""
	Raised when root finder exceeds max iterations.
	Can indicate flat objective function or numerical issues near solution boundary.
	"""
	error_code = "IV_CONVERGE_ERROR"

class QLibError (PricingError):
	"""
	Wrapper for unexpected quantlib runtime errors.
	Seeking to log for further investigation.
	"""
	error_code = "QLIB_ERROR"

	@classmethod
	def from_runtime_error (cls, e: RuntimeError, occ: Optional[str] = None) -> "QLibError":
		"""Factory method to wrap quantlib rt error"""
		return cls(
			message=str(e),
			occ=occ,
			details={"original_type":type(e).__name__}
		)

