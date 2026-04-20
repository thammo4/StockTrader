#
# FILE: `StockTrader/src/StockTrader/execution/main2.py`
#

import argparse
from datetime import date
from utils.minio_store import MinioStore

from StockTrader.settings import logger
from StockTrader.tradier import options_order, options_orderL

from StockTrader.execution.orchestrator import OrderPipe
from StockTrader.execution.executor import TraderAdapter, SimpleOrderExecutor
from StockTrader.execution.builders.template_builder import TemplateOrderBuilder
from StockTrader.execution.loaders.candidate_loader import CandidateLoader
from StockTrader.execution.persisters.result_persister import MinioResultPersister

CANDIDATES_BUCKET = "trading-candidates"
ORDERS_BUCKET = "trading-orders"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mart", required=True)
    parser.add_argument("--date", default=None)
    parser.add_argument("--template", required=True, help="Path to YAML order template for this strategy")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()

    run_date = args.date or date.today().isoformat()
    trader = options_orderL if args.live else options_order

    store = MinioStore()
    loader = CandidateLoader(store=store, bucket=CANDIDATES_BUCKET)
    builder = TemplateOrderBuilder(template_path=args.template)
    adapter = TraderAdapter(options_client=trader, equities_client=None)
    executor = SimpleOrderExecutor(trader_adapter=adapter)
    persister = MinioResultPersister(store=store, bucket=ORDERS_BUCKET)

    pipe = OrderPipe(loader=loader, builder=builder, executor=executor, persister=persister)

    fpath_output = pipe.run(dry_run=args.dry_run, mart=args.mart, date_str=run_date)

    logger.info(f"Order results persisted to: {fpath_output}")


if __name__ == "__main__":
    main()
