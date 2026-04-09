#
# FILE: `StockTrader/src/StockTrader/execution/main.py`
#


import argparse
from datetime import date
from StockTrader.tradier import options_order, options_orderL
from StockTrader.execution.order_processor import OrderProcessor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mart", required=True)
    parser.add_argument("--date", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--live", action="store_true")

    args = parser.parse_args()
    run_date = args.date or date.today().isoformat()
    # trader = options_order
    trader = options_orderL if args.live else options_order
    processor = OrderProcessor()
    processor.process(args.mart, run_date, trader, args.dry_run)


if __name__ == "__main__":
    main()
