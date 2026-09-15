#
# FILE: `StockTrader/scripts/close_premium_capture.py`
#

from airflow.exceptions import AirflowSkipException

from StockTrader.settings import logger
from StockTrader.tradier import options_order, options_orderL
from StockTrader.execution.premium_capture import run_premium_capture_close

from utils.minio_store import MinioStore

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

	if price_point not in {"ask", "bid", "mid", "close"}:
		raise ValueError("Bad price point [close_premium_capture]")

	if not 0 < capture_threshold <= 1:
		raise ValueError(f"capture_threshold={capture_threshold} not in (0,1] [close_premium_capture]")

	#
	# Define Order Client
	#

	options_client = options_orderL if live else options_order


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
