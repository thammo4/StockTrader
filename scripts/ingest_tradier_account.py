#
# FILE: `StockTrader/scripts/ingest_tradier_account.py`
#

import os
import pandas as pd
from StockTrader.tradier import acct, acctL
from StockTrader.settings import STOCK_TRADER_DWH, logger, today
from StockTrader.portfolio.equity_snapshot import EquitySnapshot
from utils.write_atomic import write_parquet_atomic

#
# Retrieve + Store today's account balance snapshot
#


def ingest_tradier_account(live: bool = False, subdir: str = "account_af"):
    env = "live" if live else "paper"
    account_client = acctL if live else acct

    equity_snapshot = EquitySnapshot(account_client)
    df = equity_snapshot.load_equity_snapshot()

    if df.empty:
        raise RuntimeError(f"Empty equity snapshot, env={env} [ingest_tradier_account]")

    df["account_env"] = env
    df["created_date"] = today

    dir_landing = os.path.join(STOCK_TRADER_DWH, subdir)
    os.makedirs(dir_landing, exist_ok=True)
    fpath_parquet = os.path.join(dir_landing, f"{env}.parquet")

    if os.path.exists(fpath_parquet):
        df_existing = pd.read_parquet(fpath_parquet)
        df = pd.concat([df_existing, df])

    df = df.sort_values("snapshot_ts").drop_duplicates(subset=["account_env", "market_date"], keep="last")

    write_parquet_atomic(df, fpath_parquet)
    logger.info(f"Loaded equity snapshot: env={env},  n_records={len(df)} [ingest_tradier_account]")
