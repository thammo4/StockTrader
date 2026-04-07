#
# FILE: `StockTrader/src/StockTrader/execution/orders.py`
#

""" Idempotent order execution with Parquet-based order log history """


import pandas as pd
from typing import Any
from pathlib import Path
from datetime import datetime
from datetime import timezone

from StockTrader.settings import logger

ACTIVE_ORDER_STATUSES = {
	"PENDING",
	"SUBMITTED",
	"ACKNOWLEDGED",
	"PARTIAL"
}

TERMINAL_ORDER_STATUSES = {
	"FILLED",
	"REJECTED",
	"EXPIRED",
	"CANCELLED",
	"SUBMIT_FAILED",
	"RESPONSE_PARSE_FAILED"
}


class TradierOrderExecutor:

	LOG_COLUMNS = [
		"internal_order_id",
		"idempotency_key",
		"occ",
		"side",
		"quantity",
		"limit_price",
		"status",
		"tradier_order_id",
		"submitted_at",
		"filled_at",
		"fill_price",
		"entry_credit",
		"entry_iv",
		"entry_sigma",
		"entry_vrp_spread",
		"entry_vrp_ratio",
		"entry_iv_cdf",
		"entry_delta",
		"entry_theta",
		"entry_spot_price",
		"entry_dte"
	]


	def __init__ (self, options_order, acct, order_log_path: Path):
		self._options_order = options_order
		self._acct = acct
		self._log_path = order_log_path

	@staticmethod
	def _utcnow_iso() -> str:
		return datetime.now(timezone.utc).isoformat()

	def _normalize_status(self, status: Any) -> str:
		if status is None:
			return ""
		s = str(status).strip().upper().replace(" ", "_")
		return s

	def _build_idempotency_key(self, occ:str, side:str, quantity:int, limit_price:float) -> str:
		return f"{occ}|{side}|{quantity}|{limit_price:.3f}"


	def _load_log(self) -> pd.DataFrame:
		if self._log_path.exists():
			return pd.read_parquet(self._log_path)
		return pd.DataFrame(columns=self.LOG_COLUMNS)

	def _save_log (self, df:pd.DataFrame) -> None:
		tmp = self._log_path.with_suffix(".tmp")
		df.to_parquet(tmp, index=False)
		tmp.rename(self._log_path)

	def submit (self, candidates: pd.DataFrame) -> pd.DataFrame:
		if candidates.empty:
			return pd.DataFrame(columns=["occ", "status", "order_id", "internal_order_id"])
		log = self._load_log()
		current_occs = set(log["occ"].values)
		results = []

		for _, row in candidates.iterrows():
			occ = row["occ"]
			if occ in current_occs:
				logger.info(f"SKIP: {occ}, already in order log")
				continue

			entry = {
				"occ": occ,
				"side": "sell_to_open",
				"quantity": int(row.get("quantity",1)),
				"limit_price": float(row["bid_price"]),
				"status": "PENDING",
				"tradier_order_id": None,
				"submitted_at": datetime.now().isoformat(),
				"filled_at": None,
				"fill_price": None,
				"entry_credit": None,
				"entry_iv": row.get("iv"),
				"entry_sigma": row.get("sigma"),
				"entry_vrp_spread": row.get("vrp_spread"),
				"entry_vrp_ratio": row.get("vrp_ratio"),
				"entry_iv_cdf": row.get("iv_cdf"),
				"entry_delta": row.get("delta"),
				"entry_theta": row.get("theta"),
				"entry_spot_price": row.get("spot_price"),
				"entry_dte": row.get("dte")
			}

			log = pd.concat([log, pd.DataFrame([entry])], ignore_index=True)
			self._save_log(log)

			try:
				response = self._options_order.options_order(
					occ_symbol = occ,
					side = "sell_to_open",
					quantity = entry["quantity"],
					order_type = "limit",
					limit_price = entry["limit_price"]
				)
				order_id = response["order"]["id"]

				log.loc[log["occ"] == occ, "status"] = "SUBMITTED"
				log.loc[log["occ"] == occ, "tradier_order_id"] = str(order_id)
				self._save_log(log)

				logger.info(f"SUBMITTED {occ}, orderid={order_id}")
				results.append({"occ":occ, "status":"SUBMITTED", "order_id":order_id})

			except KeyError:
				log.loc[log["occ"] == occ, "status"] = "RESPONSE_PARSE_FAILED"
				self._save_log(log)
				logger.error(f"PARSE_FAIL {occ}: {response}")
				results.append({"occ":occ, "status":"RESPONSE_PARSE_FAILED", "response":str(response)})

			except Exception as e:
				log.loc[log["occ"] == occ, "status"] = "SUBMIT_FAILED"
				self._save_log(log)
				logger.error(f"FAILED {occ}: {e}")
				results.append({"occ":occ, "status": "SUBMIT_FAILED", "error":str(e)})

		return pd.DataFrame(results) if results else pd.DataFrame(columns=["occ", "status", "order_id"])



	def sync_fill_status (self) -> pd.DataFrame:
		log = self._load_log()
		submitted = log[log["status"] == "SUBMITTED"]

		if submitted.empty:
			return pd.DataFrame()

		all_orders = self._acct.get_orders()
		if all_orders.empty:
			logger.warning("no orders from Tradier [sync_fill_status]")
			return pd.DataFrame()

		all_orders["id"] = all_orders["id"].astype(str)
		changed = []

		for _, row in submitted.iterrows():
			order_id = str(row["tradier_order_id"])
			match = all_orders[all_orders["id"] == order_id]

			if match.empty:
				continue

			tradier_status = match.iloc[0]["status"]
			mask = log["occ"] == row["occ"]

			if tradier_status == "filled":
				fill_price = float(match.iloc[0].get("avg_fill_price", row["limit_price"]))
				entry_credit = 100 * fill_price * row["quantity"]
				log.loc[mask, "status"] = "FILLED"
				log.loc[mask, "fill_price"] = fill_price
				log.loc[mask, "entry_credit"] = entry_credit
				log.loc[mask, "filled_at"] = datetime.now().isoformat()
				changed.append(row["occ"])

			elif tradier_status in ("rejected", "expired", "canceled"):
				log.loc[mask, "status"] = tradier_status.upper()
				changed.append(row["occ"])

		if changed:
			self._save_log(log)

		return log[log["occ"].isin(changed)]