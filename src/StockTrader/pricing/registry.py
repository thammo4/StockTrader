#
# FILE: `StockTrader/src/StockTrader/pricing/registry.py`
#

"""
Pricing model registry with decorator based object registration.

Factory Pattern.

Oks models to self-register by decorating class definitions for ez new model addition.

Example:
        # Model File
        @register_model
        class NewModelMe (BasePricingModel):
                name = "new_model_me"

        # instantiate
        m = get_model("new_model_me", n_steps=225)
"""


from typing import Any, Dict, List, Type

from StockTrader.pricing.base import BasePricingModel
from StockTrader.pricing.errors import ModelNotFoundError, ModelConfigurationError
from StockTrader.settings import logger


#
# Global Registry Map: (Model Name) <--> Class
#

_MODEL_REGISTRY: Dict[str, Type[BasePricingModel]] = {}


def register_model(cls: Type[BasePricingModel]) -> Type[BasePricingModel]:
    """
    Register-a-Pricing-Model Decorator.

    Registry Key = Model 'name' class attribute.

    Example:
            @register_model
            class BigBadPricingModel(BasePricingModel):
                    name = "big_bad_pricing_model"
    """
    if not hasattr(cls, "name") or not cls.name:
        raise ModelConfigurationError(f"Model class {cls.__name__} requires 'name' attribute")

    model_name = cls.name

    if model_name in _MODEL_REGISTRY:
        logger.warning(f"Model '{model_name}' already registered by {_MODEL_REGISTRY[model_name].__name__}")
        logger.warning(f"overwriting with {cls.__name__}")

    _MODEL_REGISTRY[model_name] = cls
    logger.info(f"Registered pricing model: {model_name} -> {cls.__name__}")

    return cls


def get_model(name: str, **kwargs) -> BasePricingModel:
    """Factory function to instantiate existing registered pricing model"""
    if name not in _MODEL_REGISTRY:
        model_list = list(_MODEL_REGISTRY.keys())
        raise ModelNotFoundError(f"Model '{name}' not found. Models: {model_list}")

    model_class = _MODEL_REGISTRY[name]

    try:
        return model_class(**kwargs)
    except Exception as e:
        raise ModelConfigurationError(f"Failed model instantiate: '{name}': {e}")


def list_models() -> List[Dict[str, Any]]:
    """List registered models + model metadata"""
    models = []
    for name, cls in _MODEL_REGISTRY.items():
        models.append({"name":name, "class":cls.__name__})
    return models


def is_registered(name: str) -> bool:
    """Check if model already registered."""
    return name in _MODEL_REGISTRY
