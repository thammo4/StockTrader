#
# FILE: `StockTrader/src/StockTrader/execution/main2.py`
#

import argparse
from datetime import date
from utils.minio_store import MinioStore

from StockTrader.settings import logger
from StockTrader.tradier import acct, acctL, options_order, options_orderL

from StockTrader.execution.orchestrator import OrderPipe
from StockTrader.execution.executor import TraderAdapter, SimpleOrderExecutor
from StockTrader.execution.builders.template_builder import TemplateOrderBuilder
from StockTrader.execution.loaders.candidate_loader import CandidateLoader
from StockTrader.execution.persisters.result_persister import MinioResultPersister

from StockTrader.execution.filters.filtered_loader import FilteredCandidateLoader

CANDIDATES_BUCKET = "trading-candidates"
ORDERS_BUCKET = "trading-orders"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mart", required=True)
    parser.add_argument("--date", default=None)
    parser.add_argument("--template", required=True, help="Path to YAML order template for this strategy")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--live", action="store_true")

    parser.add_argument(
        "--max-position-frac",
        type = float,
        default = 0.05,
        help = "max per-position margin proportion of option buying power"
    )
    parser.add_argument(
        "--max-portfolio-margin-frac",
        type = float,
        default = 0.80,
        help = "max agg-portfolio margin proportion of total equity"
    )
    parser.add_argument(
        "--min-credit-frac",
        type = float,
        default = 0.005,
        help = "min agg candidate credit proportion of total equity"
    )
    parser.add_argument(
        "--ok-duplicate-symbols",
        action = "store_true",
        help = "ok to have > 1 same underlying"
    )
    parser.add_argument(
        "--no-filter",
        action = "store_true",
        help = "get rich quick / yolo"
    )


    args = parser.parse_args()

    run_date = args.date or date.today().isoformat()
    trader = options_orderL if args.live else options_order
    account_client = acctL if args.live else acct

    store = MinioStore()
    loader_raw = CandidateLoader(store=store, bucket=CANDIDATES_BUCKET)

    if args.no_filter:
        loader = loader_raw
    else:
        loader = FilteredCandidateLoader(
            inner_loader = loader_raw,
            account_client = account_client,
            max_position_frac = args.max_position_frac,
            max_portfolio_margin_frac = args.max_portfolio_margin_frac,
            min_credit_frac = args.min_credit_frac,
            ok_duplicate_symbols = args.ok_duplicate_symbols
        )

    builder = TemplateOrderBuilder(template_path=args.template)
    adapter = TraderAdapter(options_client=trader, equities_client=None)
    executor = SimpleOrderExecutor(trader_adapter=adapter)
    persister = MinioResultPersister(store=store, bucket=ORDERS_BUCKET)

    pipe = OrderPipe(loader=loader, builder=builder, executor=executor, persister=persister)

    #
    # Try/Except Block to Catch Empty DataFrame When No Candidates Pass Filter
    #

    try:
        fpath_output = pipe.run(dry_run=args.dry_run, mart=args.mart, date_str=run_date)
        logger.info(f"Order results persisted to: {fpath_output}")
    except ValueError as e:
        logger.info(f"No orders to execute: {str(e)} [main2]")


if __name__ == "__main__":
    main()
