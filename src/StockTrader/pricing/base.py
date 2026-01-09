#
# FILE: `StockTrader/src/StockTrader/pricing/base.py`
#

"""
Abstract Base Class interface for Pricing Models.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from StockTrader.pricing.types import OptionRow, PricingResult


class BasePricingModel(ABC):
    """
    Abstract interface for options pricing models.
    Required:
            - name: Unique identifier for registry lookup.
            - price(): Core pricer.
            - validate_inputs(): Parameter validator.
    """

    def __init__(self, **kwargs):
        """
        Initialize model.

        Subclass to call super().__init__(**kwargs) -> apply specific config.
        """

        self._config = kwargs
        self.configure(**kwargs)

    def configure(self, **kwargs) -> None:
        """
        Apply model-specific config.
        """

        pass

    @abstractmethod
    def price(self, option: OptionRow, compute_greeks: bool = True, compute_iv: bool = True) -> PricingResult:
        """
        Compute theoretical option price (and Greeks, IV).

        Args:
                - option: Immutable container with pricing inputs.
                - compute_greeks: compute delta, gamma, theta, vega, rho.
                - compute_iv: solve for implied volatility.

        Returns:
                - PricingResult with computed values and error information.
        """

        pass

    @abstractmethod
    def validate_inputs(self, option: OptionRow) -> Optional[str]:
        """Validate input ranges"""
        pass

    def get_config(self) -> Dict[str, Any]:
        """Return current config for logging/debugging."""
        return {
            "name": self.name,
            "supports_greeks": self.supports_greeks,
            "supports_iv": self.supports_iv,
            **self._config,
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"
