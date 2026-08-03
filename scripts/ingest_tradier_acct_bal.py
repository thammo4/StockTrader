#
# FILE: `StockTrader/scripts/ingest_tradier_acct_bal.py`
#

import os
import pandas as pd
from datetime import datetime
from StockTrader.tradier import acct, acctL
from StockTrader.settings import STOCK_TRADER_DWH, logger, today
from utils.write_atomic import write_parquet_atomic
from airflow.exceptions import AirflowSkipException

def ingest_tradier_acct_bal(live: bool = False):
	logger.info("Starting acct bal ingest [ingest_tradier_acct_bal]")

	try:
		acct_client = acctL if live else acct
 
		df = acct_client.get_account_balance()
		if df is None or df.empty:
			logger.warning("No acct bal data [ingest_tradier_acct_bal]")
			return

		acct_id = df["account_number"].iloc[0]
		df["created_date"] = today
		df["created_ts"] = datetime.now().strftime("%H:%M:%S")

		#
		# Prepare landing dir + filepath
		#

		# dir_landing = os.path.join(STOCK_TRADER_DWH, "acct_bal")
		dir_landing = os.path.join(STOCK_TRADER_DWH, "account_af")
		os.makedirs(dir_landing, exist_ok=True)
		fpath_parquet = os.path.join(dir_landing, f"{acct_id}.parquet")

		#
		# Create (if not exists) or Append (if exists)
		#

		if os.path.exists(fpath_parquet):
			df_existing = pd.read_parquet(fpath_parquet)
			df = pd.concat([df_existing, df], ignore_index=True)
			logger.info(f"Appending n_bals={len(df_existing)} to acct={acct_id} [ingest_tradier_acct_bal]")
		else:
			logger.info(f"Creating bal file, acct={acct_id} [ingest_tradier_acct_bal]")

		#
		# Drop Duplicates (uncomment to enforce)
		# - As of script inception, duplicates are ok because:
		# 		A. Low risk of unbounded growth in data volume (few accounts, 25 cols with compressible values, etc)
		# 		B. Account balance values change continuously throughout day, and capturing intraday changes may be useful
		#

		# df.drop_duplicates(subset=["account_number", "created_date"], keep="last", inplace=True)

		write_parquet_atomic(df, fpath_parquet)
		logger.info(f"Acct bal ingest ok, acct={acct_id}, n={len(df)} [ingest_tradier_acct_bal]")

	except AirflowSkipException:
		raise
	except Exception as e:
		logger.error(f"Acct bal ingest fail, acct={acct_id}: {str(e)} [ingest_tradier_acct_bal]")
		raise
