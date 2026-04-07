#
# FILE: `StockTrader/src/execution/worker.py`
#

import json
import redis
import logging
from pathlib import Path

from StockTrader.tradier import options_order, acct, quotes
from StockTrader.execution.orders import TradierOrderExecutor
from StockTrader.execution.portfolio import (
	TradierPositionMonitor,
	BasicM2M,
	ThresholdExitEvaluator
)

logger = logging.getLogger(__name__)

REDIS_HOST = 'rudi.local'
REDIS_PORT = 6379
COMMAND_QUEUE = 'execution:commands'
RESULT_PREFIX = 'execution:results:'

ORDER_LOG_PATH = Path("./logs/order_log.parquet")

def build_components ():
	executor = TradierOrderExecutor(options_order=options_order, acct=acct, order_log_path=ORDER_LOG_PATH)
	monitor = TradierPositionMonitor(acct=acct, order_log_path=ORDER_LOG_PATH)
	marker = BasicM2M(quotes=quotes)
	# def __init__(self, profit_target_pct: float, stop_loss_pct: float, min_dte: int):
	# exit_eval = ThresholdExitEvaluator(profit_target_pct=0.50, stop_loss_pct=1.00, min_dte=5)
	return executor, monitor, marker #, exit_eval

def run_worker():
	r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_response=True)
	executor, monitor, marker = build_components()

	logger.info("execution worker started, awaiting commands...")

	while True:
		_, raw = r.brpop(COMMAND_QUEUE, timeout=0)
		cmd = json.loads()

		command_type = cmd["type"]
		command_id=cmd["id"]
		result_key = f"{RESULT_PREFIX}{command_id}"

		logger.info(f"Processing command: {command_type} ({command_id})")

		try:
			if command_type == "SUBMIT_ORDERS":
				import pandas as pd
				candidates = pd.read_json(cmd["payload"])
				df_result = executor.submit(candidates)
				r.set(result_key, df_result.to_json(), ex=3600)

			elif command_type == "SYNC_FILLS":
				df_result = executor.sync_fill_status()
				r.set(result_key, df_result.to_json(), ex=3600)

			elif command_type == "REFRESH_MARKS":
				positions = monitor.refresh()
				marked = marker.mark(positions)
				r.set(result_key, marked.to_json(), ex=3600)

			elif command_type == "EVALUATE_EXITS":
				positions = monitor.refresh()
				marked = marker.mark(positions)
				exits = exit_eval.evaluate(marked)
				r.set(result_key, exits.to_json(), ex=3600)

				r.set('execution:latest_marks', marked.to_json(), ex=7200)

			# elif command_type == "SUBMIT_EXITS":
			# 	exits = pd.read_json(cmd["payload"])
			# 	exits["side"] = "buy_to_close"
			# 	exits["bid_price"] = exits["ask"]
			# 	for _,pos in exits.iterrows():
			# 		try:
			# 			options_order.options_order(
			# 				occ=pos["occ"],
			# 				side="buy_to_close",
			# 				quantity=int(pos['quantity']),
			# 				order_type = 'limit',
			# 				price=float(pos['ask'])
			# 			)
			# 			logger.info(f"EXIT submitted: {pos['occ']}, reason={pos['exit_reason']}")
			# 		except Exception as e:
			# 			logger.error(f"EXIT failed, occ={pos['occ']}: {e}")
			# 	r.set(result_key, json.dumps({"status":"done"}), ex=3600)

			else:
				logger.warning(f"idk command {command_type}")
				r.set(result_key, json.dumps({"error":"unknown_command"}), ex=3600)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	run_worker()