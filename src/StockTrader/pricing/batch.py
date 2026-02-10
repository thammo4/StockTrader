#
# FILE: `StockTrader/src/StockTrader/pricing/batch.py`
#

"""
Model independent batch processing logic.

Functionality:
        - DataFrame Iteration
        - Error Aggregation
        - Result Compilation

Designed to integrate with Redis job schema for distributed compute.
"""

import time
import pandas as pd
from typing import Callable, List, Optional
from datetime import date

from StockTrader.pricing.types import BatchResult, OptionRow, PricingResult
from StockTrader.pricing.base import BasePricingModel
from StockTrader.pricing.registry import get_model

from StockTrader.settings import logger


def price_df(
    df: pd.DataFrame,
    model: BasePricingModel,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    **model_kwargs,
) -> pd.DataFrame:

    logger.info(f"Starting batch pricing: n={len(df)} contracts")
    # logger.info(f"model={model.name}, greeks={compute_greeks}, iv={compute_iv}")
    logger.info(f"model={model.name}")

    results: List[dict] = []
    n_total = len(df)

    #
    # Note - including additional i-counter to accommodate progress_callback in the event of non-numeric index
    #

    for i, (idx, row) in enumerate(df.iterrows()):
        if progress_callback and i % 100 == 0:
            progress_callback(idx, n_total)

        try:
            option_row = OptionRow.from_series(row)
            result = model.price(option_row)
            results.append(result.to_dict())

        except Exception as e:
            occ = row.get("occ", f"row_{idx}")
            logger.error(f"Unexpected pricing error, occ={occ}: {e}")
            results.append(
                PricingResult(occ=str(occ), npv_err=f"UNEXPECTED: {type(e).__name__}: {str(e)[:100]}").to_dict()
            )

    df_results = pd.DataFrame(results)

    df_output = df.merge(df_results, on="occ", how="left", suffixes=("", "_result"))

    n_success = df_results["npv"].notna().sum()
    n_iv = df_results["σ_iv"].notna().sum() if "σ_iv" in df_results else 0

    logger.info(f"Batch done: {n_success}/{n_total} priced ok.")
    logger.info(f"{n_iv} σ_iv solved.")

    return df_output


def process_job_shard(
    df: pd.DataFrame,
    job_id: str,
    batch_id: str,
    market_date: date,
    shard: int,
    model_name: str,
    **model_kwargs,
) -> BatchResult:

    start_time = time.time()

    model = get_model(model_name, **model_kwargs)

    logger.info(f"Processing job={job_id}, batch={batch_id}, market_date={market_date}, shard={shard}, n={len(df)}")

    batch_result = BatchResult(
        job_id=job_id, batch_id=batch_id, market_date=market_date, shard=shard, n_total=len(df), model_name=model_name
    )

    for idx, row in df.iterrows():
        try:
            option_row = OptionRow.from_series(row)
            result = model.price(option_row)

            if result.is_valid:
                batch_result.n_success += 1
                if result.has_iv:
                    batch_result.n_iv_solved += 1
                else:
                    batch_result.n_iv_failed += 1
            else:
                batch_result.n_failed += 1

            batch_result.results.append(result)

        except Exception as e:
            occ = row.get("occ", f"row_{idx}")
            logger.error(f"Error processing occ={occ}: {e}")
            batch_result.n_failed += 1
            batch_result.results.append(PricingResult(occ=str(occ), npv_err=f"UNEXPECTED: {str(e)[:100]}"))

    batch_result.elapsed_sec = time.time() - start_time

    logger.info("Done.")
    logger.info(
        f"job={job_id}, success={batch_result.n_success}, total={batch_result.n_total}, iv_success={batch_result.n_iv_solved}, runtime={batch_result.elapsed_sec:.2f}s"
    )

    return batch_result


def results_to_df(results: List[PricingResult]) -> pd.DataFrame:
    """Convert list of PricingResult objs to DataFrame"""
    return pd.DataFrame([r.to_output_dict() for r in results])
