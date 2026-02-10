#
# FILE: `StockTrader/src/StockTrader/pricing/types.py`
#

"""
Implements dataclass containers for pricing inputs/outputs.
Defines schema of input data (OptionRow), output data (PricingResult) and reporting/process data (BatchResult)
to provide standardized interface.
"""


#
# Note - dataclasses
#
# Provides decorator + functions to user-defined classes.
# Imports used:
#   dataclass method - adds special methods to user-def class
#   field def - class var with type annotation
#   asdict method - maps each dataclass object into a key-value dict of its fields


from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
from datetime import date

import math


#
# Batch Processing Input
#

@dataclass(frozen=True)
class OptionRow:
    """
    Immutable container for unit option contract pricing model input.
    frozen=true -> no changes during processing.
    """

    #
    # Contract Identifiers
    #

    market_date: date
    symbol: str
    occ: str
    option_type: str
    expiry_date: date

    S: float
    K: float
    r: float
    q: float
    σ: float
    T: float

    p_m: float
    p_a: float
    p_b: float

    p_i: Optional[float] = None
    p_tm: Optional[float] = None
    p_tb: Optional[float] = None
    p_ta: Optional[float] = None

    mnys: Optional[float] = None
    mnys_cat: Optional[str] = None
    volume: Optional[int] = None
    oi: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary"""
        return {k: v for k, v in asdict(self).items()}

    @classmethod
    def from_series(cls, row: Any) -> "OptionRow":
        """Construct OptionRow from pandas series"""

        return cls(
            market_date=row["market_date"],
            symbol=str(row["symbol"]),
            occ=str(row["occ"]),
            option_type=str(row["option_type"]).lower(),
            expiry_date=row["expiry_date"],
            S=float(row["S"]),
            K=float(row["K"]),
            r=float(row["r"]),
            q=float(row.get("q", 0.0)),
            σ=float(row["σ"]),
            T=float(row["T"]),
            p_m=float(row["p_m"]),
            p_b=float(row["p_b"]),
            p_a=float(row["p_a"]),
            p_i=row.get("p_i"),
            p_tm=row.get("p_tm"),
            p_tb=row.get("p_tb"),
            p_ta=row.get("p_ta"),
            mnys=row.get("mnys"),
            mnys_cat=row.get("mnys_cat"),
            volume=row.get("volume"),
            oi=row.get("oi"),
        )


#
# Batch Processing Output
#

@dataclass
class PricingResult:
    """
    Container for a contract's pricing outputs.
    Mutable dataclass which accumulates results from n-phased computation (NPV, Greeks, IV).
    Tracks errors.
    """

    occ: str

    npv: Optional[float] = None
    Δ: Optional[float] = None
    Γ: Optional[float] = None
    Θ: Optional[float] = None
    ν: Optional[float] = None
    ρ: Optional[float] = None
    σ_iv: Optional[float] = None

    npv_err: Optional[str] = None
    greek_err: Optional[str] = None
    σ_iv_err: Optional[str] = None

    model_name: Optional[str] = None
    n_steps: Optional[int] = None
    compute_ms: Optional[float] = None

    @property
    def is_valid(self) -> bool:
        """Returns True if NPV compute ok"""
        if self.npv is None:
            return False
        if self.npv_err is not None:
            return False
        return math.isfinite(self.npv)

    @property
    def has_delta(self) -> bool:
        """Returns true if delta compute ok"""
        return self.Δ is not None

    @property
    def has_iv(self) -> bool:
        """Returns true if σ_iv solved for"""
        return self.σ_iv is not None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize dictionary to construct dataframe"""
        return asdict(self)

    def to_output_dict(self) -> Dict[str, Any]:
        """Serialize dictionary with non-None output fields"""
        output_fields = [
            "occ",
            "npv",
            "Δ",
            "Γ",
            "Θ",
            "ν",
            "ρ",
            "σ_iv",
            "npv_err",
            "greek_err",
            "σ_iv_err",
            "model_name",
            "n_steps",
            "compute_ms",
        ]
        d = asdict(self)
        return {k: d.get(k) for k in output_fields}


#
# Batch Processing Summary
#

@dataclass
class BatchResult:
    """Aggregated reesults from processing options batch for logging/monitoring/manifest."""

    job_id: str
    batch_id: str
    shard: int
    market_date: date

    n_total: int = 0
    n_success: int = 0
    n_failed: int = 0
    n_iv_solved: int = 0
    n_iv_failed: int = 0
    elapsed_sec: float = 0.0
    model_name: str = ""

    #
    # Note - dataclasses field(default_factory)
    #
    # Specifies default value for mutable field (e.g. list, dict)
    # Ensures every instance keeps its own mutable type.
    # This means each BatchResult instance will have its own result set list (opposed to a shared/common one amongst all BatchResults)
    #

    results: List[PricingResult] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Proportion of successfully priced contracts"""
        return self.n_success / self.n_total if self.n_total > 0 else 0.0

    @property
    def iv_solve_rate(self) -> float:
        """Proportion of successfully priced contracts with solved implied vol."""
        return self.n_iv_solved / self.n_success if self.n_success > 0 else 0.0

    def to_manifest_entry(self) -> Dict[str, Any]:
        """Generate manifest entry for batch result."""
        return {
            "job_id": self.job_id,
            "batch_id": self.batch_id,
            "market_date": self.market_date,
            "shard": self.shard,
            "n_total": self.n_total,
            "n_success": self.n_success,
            "n_failed": self.n_failed,
            "n_iv_solved": self.n_iv_solved,
            "success_rate": round(self.success_rate, 4),
            "iv_solve_rate": round(self.iv_solve_rate, 4),
            "elapsed_sec": round(self.elapsed_sec, 2),
            "model_name": self.model_name,
        }
