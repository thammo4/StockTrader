#
# FILE: `StockTrader/src/StockTrader/portfolio/monitor.py`
#

from datetime import datetime
from zoneinfo import ZoneInfo

from StockTrader.settings import logger
from StockTrader.portfolio.position_loader import PositionLoader
from StockTrader.portfolio.position_quotes import PositionQuotes
from StockTrader.portfolio.m2m import M2MCalc

from utils.minio_store import MinioStore

SNAPSHOT_BUCKET = "portfolio-snapshots"
SUMMARY_BUCKET = "portfolio-summaries"

MARKET_TZ = ZoneInfo("America/New_York")


def run_monitoring(account_client, quotes_client, minio_store: MinioStore):

    l = PositionLoader(account_client)
    q = PositionQuotes(quotes_client)

    #
    # Load Current Set of Positions from Tradier Account
    #

    df_positions = l.load_options_positions()
    if df_positions.empty:
        logger.info("No open positions, skip snapshot [monitor]")
        return

    #
    # Mark Positions + Compute Performance Metrics
    #

    df_enriched = q.add_market_data(df_positions)
    df_upl = M2MCalc.compute_upl(df_enriched)
    portfolio_summary = M2MCalc.portfolio_summary(df_upl)
    logger.info(f"Portfolio summary: {portfolio_summary} [monitor]")

    now = datetime.now()
    date_str = now.strftime("%Y%m%d")
    time_str = now.strftime("%H%M%S")
    snapshot_id = f"{date_str}/{time_str}.parquet"
    summary_id = f"{date_str}/{time_str}.json"

    #
    # Write Snapshot Data to S3 Parquet
    #

    s3_addr_snapshot = minio_store.write_parquet(
        bucket=SNAPSHOT_BUCKET, obj_name=snapshot_id, df=df_upl, ensure_bucket=True
    )
    logger.info(f"Snapshot: {s3_addr_snapshot} [monitor]")

    #
    # Write Summary Data to S3 JSON
    #

    s3_addr_summary = minio_store.write_json(
        bucket=SUMMARY_BUCKET, obj_name=summary_id, data=portfolio_summary, ensure_bucket=True
    )
    logger.info(f"Summary: {s3_addr_summary} [monitor]")

    logger.info(f"Snapshot persisted to: {SNAPSHOT_BUCKET} [monitor]")
    logger.info(f"Summary persisted to: {SUMMARY_BUCKET} [monitor]")
