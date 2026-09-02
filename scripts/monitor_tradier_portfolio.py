#
# FILE: `StockTrader/scripts/monitor_tradier_portfolio.py`
#

from StockTrader.portfolio.monitor import run_monitoring
from utils.minio_store import MinioStore


def monitor_tradier_portfolio(
    acct_client, quotes_client, minio_endpoint: str = None, minio_access_key: str = None, minio_secret_key: str = None
):
    m = MinioStore(endpoint=minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key)

    run_monitoring(acct_client, quotes_client, m)
