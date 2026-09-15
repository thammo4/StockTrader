#
# FILE: `StockTrader/scripts/close_premium_capture.py`
#

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from airflow.exceptions import AirflowSkipException

from StockTrader.settings import logger
from StockTrader.tradier import acct, acctL, options_order, options_orderL
from StockTrader.execution.premium_capture import run_premium_capture_close
from StockTrader.execution.strategy_position_registry import PRICE_POINTS

from utils.minio_store import MinioStore


MARKET_TZ = ZoneInfo("America/New_York")

#
# Submission Window + Max Snapshot Age
#
# Window excludes the first and last five minutes of the regular session.
# Max age < 5-min monitor cadence so a late or retried run skips rather than
# acting on a superseded snapshot.
#

SESSION_OPEN = time(9, 35)
SESSION_CLOSE = time(15, 55)
MAX_SNAPSHOT_AGE = timedelta(minutes=4)


def close_premium_capture(
     snapshot_id: str,
     strategy: str = "vrp",
     capture_threshold: float = 0.85,
     price_point: str = "mid",
     live: bool = False,
     dry_run: bool = True,
     minio_endpoint: str = None,
     minio_access_key: str = None,
     minio_secret_key: str = None
):

     #
     # Require Portfolio Snapshot from Upstream Monitor
     #

     if not snapshot_id:
          raise AirflowSkipException("No portfolio snapshot provided. [close_premium_capture]")

     if price_point not in PRICE_POINTS:
          raise ValueError("Bad price point [close_premium_capture]")

     if not 0 < capture_threshold < 1:
          raise ValueError(f"capture_threshold={capture_threshold} not in (0,1) [close_premium_capture]")

     #
     # Regular-Session Gate
     #

     now = datetime.now(MARKET_TZ)

     if not SESSION_OPEN <= now.time() <= SESSION_CLOSE:
          raise AirflowSkipException(
               f"Outside submission window: now={now:%H:%M:%S} ET [close_premium_capture]"
          )

     #
     # Snapshot Staleness Gate (Snapshot Keys Are ET: YYYYMMDD/HHMMSS.parquet)
     #

     snapshot_ts = datetime.strptime(snapshot_id, "%Y%m%d/%H%M%S.parquet").replace(tzinfo=MARKET_TZ)
     snapshot_age = now - snapshot_ts

     if snapshot_age > MAX_SNAPSHOT_AGE:
          raise AirflowSkipException(
               f"Stale snapshot: snapshot={snapshot_id}, age={snapshot_age} [close_premium_capture]"
          )

     #
     # Define Order + Account Clients (Same Environment)
     #

     options_client = options_orderL if live else options_order
     account_client = acctL if live else acct


     #
     # Define MinIO Client
     #

     m = MinioStore(endpoint=minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key)

     #
     # Run Premium-Capture Pipeline
     #

     logger.info(
          "Starting premium capture close: "
          f"snapshot={snapshot_id}, "
          f"strategy={strategy}, "
          f"threshold={capture_threshold:.2%}, "
          f"price={price_point}, "
          f"live={live}, "
          f"dry={dry_run} "
          f"[close_premium_capture]"
     )

     result = run_premium_capture_close(
          snapshot_id = snapshot_id,
          strategy = strategy,
          options_client = options_client,
          account_client = account_client,
          minio_store = m,
          capture_threshold = capture_threshold,
          price_point = price_point,
          dry_run = dry_run
     )

     logger.info(
          "Premium capture close complete: "
          f"snapshot={snapshot_id}, "
          f"result={result} "
          f"[close_premium_capture]"
     )

     return result
