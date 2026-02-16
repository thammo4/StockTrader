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
from typing import List
from datetime import date

from StockTrader.pricing.types import BatchResult, OptionRow, PricingResult
from StockTrader.pricing.registry import get_model

from StockTrader.settings import logger


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
            pricing_result = model.price(option_row)

            if pricing_result.is_valid:
                batch_result.n_success += 1
                if pricing_result.has_iv:
                    batch_result.n_iv_solved += 1
                else:
                    batch_result.n_iv_failed += 1
            else:
                batch_result.n_failed += 1
                batch_result.n_iv_failed += 1  # cant compute implied volatility without first having model price

            batch_result.results.append(pricing_result)

        except Exception as e:
            occ = row.get("occ", f"row_{idx}")
            logger.error(f"Error processing occ={occ}: {e}")
            batch_result.n_failed += 1
            batch_result.n_iv_failed += 1
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
