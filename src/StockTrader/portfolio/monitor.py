#
# FILE: `StockTrader/src/StockTrader/portfolio/monitor.py`
#

import time
import argparse
from datetime import datetime

from StockTrader.settings import logger
from StockTrader.tradier import acct, acctL, quotes, quotesL

from StockTrader.portfolio.position_loader import PositionLoader
from StockTrader.portfolio.position_quotes import PositionQuotes
from StockTrader.portfolio.m2m import M2MCalc
from utils.minio_store import MinioStore

SNAPSHOT_BUCKET = "portfolio-snapshots"
SUMMARY_BUCKET = "portfolio-summaries"


# def run_monitoring ():
def run_monitoring(account_client, quotes_client, minio_store: MinioStore):

    l = PositionLoader(account_client)
    q = PositionQuotes(quotes_client)

    df_positions = l.load_options_positions()
    if df_positions.empty:
        logger.info("No open positions, skip snapshot [monitor]")
        return

    df_enriched = q.add_market_data(df_positions)
    df_upl = M2MCalc.compute_upl(df_enriched)

    portfolio_summary = M2MCalc.portfolio_summary(df_upl)

    logger.info(f"Portfolio summary: {portfolio_summary} [monitor]")

    now = datetime.now()
    date_str = now.strftime("%Y%m%d")
    time_str = now.strftime("%H%M%S")
    snapshot_id = f"{date_str}/{time_str}.parquet"
    summary_id = f"{date_str}/{time_str}.json"

    # Implement df_upl parquet write to s3 here

    s3_addr_snapshot = minio_store.write_parquet(
        bucket=SNAPSHOT_BUCKET, obj_name=snapshot_id, df=df_upl, ensure_bucket=True
    )
    # logger.info(f"Snapshot: s3://{SNAPSHOT_BUCKET}/{snapshot_id}")
    logger.info(f"Snapshot: {s3_addr_snapshot} [monitor]")

    s3_addr_summary = minio_store.write_json(
        bucket=SUMMARY_BUCKET, obj_name=summary_id, data=portfolio_summary, ensure_bucket=True
    )

    logger.info(f"Summary: {s3_addr_summary} [monitor]")

    # Implement portfolio_summary text file write to s3 here

    logger.info(f"Snapshot persisted to: {SNAPSHOT_BUCKET} [monitor]")
    logger.info(f"Summary persisted to: {SUMMARY_BUCKET} [monitor]")


def main():
    parser = argparse.ArgumentParser(description="Portfolio Monitor")
    parser.add_argument("--live", action="store_true", help="Use live account (default: paper)")
    parser.add_argument("--interval", type=int, default=300, help="Inter-snapshot sleep seconds")

    args = parser.parse_args()

    account_client = acct if not args.live else acctL
    quotes_client = quotes if not args.live else quotesL
    minio_store = MinioStore()

    logger.info(f"Starting portfolio monitor [monitor]")
    while True:
        run_monitoring(account_client, quotes_client, minio_store)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
