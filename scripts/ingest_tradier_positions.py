#
# FILE: `StockTrader/scripts/ingest_tradier_positions.py`
#

import os
import pandas as pd
from datetime import datetime
from StockTrader.tradier import acct, acctL
from StockTrader.settings import STOCK_TRADER_DWH, logger, today
from StockTrader.portfolio.position_loader import PositionLoader
from utils.write_atomic import write_parquet_atomic


#
# Retrieve + Store today's EOD open options positions snapshot
#


def ingest_tradier_positions(live: bool = False, subdir: str = "positions_af"):
    env = "live" if live else "paper"
    account_client = acctL if live else acct

    l = PositionLoader(account_client)
    df = l.load_options_positions()

    if df.empty:
        logger.info(f"No open positions, env={env} [ingest_tradier_positions]")
        return

    df = df.drop(columns=["acq_time"], errors="ignore")

    df["market_date"] = pd.to_datetime(today).date().isoformat()
    df["snapshot_ts"] = datetime.now().isoformat(timespec="seconds")
    df["account_env"] = env
    df["created_date"] = today

    dir_landing = os.path.join(STOCK_TRADER_DWH, subdir)
    os.makedirs(dir_landing, exist_ok=True)
    fpath_parquet = os.path.join(dir_landing, f"{env}.parquet")

    if os.path.exists(fpath_parquet):
        df_existing = pd.read_parquet(fpath_parquet)
        df = pd.concat([df_existing, df])

    df = df.sort_values(by="snapshot_ts").drop_duplicates(subset=["account_env", "occ", "market_date"], keep="last")

    write_parquet_atomic(df, fpath_parquet)
    logger.info(f"Loaded positions snapshot: env={env}, n_records={len(df)} [ingest_tradier_positions]")
