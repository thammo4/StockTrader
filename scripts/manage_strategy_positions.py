#
# FILE: `StockTrader/scripts/manage_strategy_positions.py`
#

#
# Usage:
# 		python scripts/manage_strategy_positions.py register \
# 			--strategy vrp \
# 			--symbol ORCL \
# 			--occ ORCL260918P00145000 \
# 			--capture-threshold 0.85 \
# 			--price-point mid
#
# 		python scripts/manage_strategy_positions.py list \
# 			--strategy vrp
#
# 		python scripts/manage_strategy_positions.py disable \
# 			--strategy vrp \
# 			--occ ORCL260918P00145000
#
# 		python scripts/manage_strategy_positions.py enable \
# 			--strategy vrp \
# 			--occ ORCL260918P00145000
#

import argparse

from utils.minio_store import MinioStore
from StockTrader.execution.strategy_position_registry import StrategyPositionRegistry

def manage_strategy_positions(
	command: str,
	strategy: str = None,
	occ: str = None,
	symbol: str = None,
	capture_threshold: float = 0.85,
	price_point: str = "mid",
	entry_order_id: int = None,
	show_all: bool = False,
	minio_endpoint: str = None,
	minio_access_key: str = None,
	minio_secret_key: str = None
):

	
	#
	# Define MinIO Client + Strategy Position Registry
	#

	m = MinioStore(endpoint=minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key)

	registry = StrategyPositionRegistry(store=m)

	#
	# Register Position
	#

	if command == "register":
		if not strategy:
			raise ValueError("strategy required for register [manage_strategy_positions]")
		if not occ:
			raise ValueError("occ required for register [manage_strategy_positions]")
		if not symbol:
			raise ValueError("symbol required for register [manage_strategy_positions]")
		if not 0 < capture_threshold <= 1:
			raise ValueError(f"capture_threshold={capture_threshold} not in (0,1] [manage_strategy_positions]")

		if price_point not in {"ask", "bid", "mid"}:
			raise ValueError(f"price_point={price_point} not in {{ask, bid, mid}} [manage_strategy_positions]")

		return registry.register(
			strategy=strategy,
			occ=occ,
			symbol=symbol,
			capture_threshold=capture_threshold,
			price_point=price_point,
			entry_order_id=entry_order_id
		)


	#
	# Enable Position
	#

	if command == "enable":
		if not strategy or not occ:
			raise ValueError(
				f"strategy and occ are required for enable "
				f"(strategy={strategy!r}, occ={occ!r}) "
				f"[manage_strategy_positions]"
			)
		return registry.enable(strategy=strategy, occ=occ)


	#
	# Disable Position
	#

	if command == "disable":
		if not strategy or not occ:
			raise ValueError(
				f"strategy and occ are required for disable "
				f"(strategy={strategy!r}, occ={occ!r}) "
				f"[manage_strategy_positions]"
			)
		return registry.disable(strategy=strategy, occ=occ)


	#
	# List Registry
	#

	if command == "list":
		df = registry.load(strategy=strategy, enabled_only=not show_all)

		if df.empty:
			print("no registered strategy positions")
		else:
			print(df.to_string(index=False))

		return df

	#
	# Unknown Registry Command
	#

	raise ValueError(f"Unknown command={command} [manage_strategy_positions]")




def main():
	parser = argparse.ArgumentParser(description = "Manage automated strategy-position exit policies.")
	subparsers = parser.add_subparsers(dest="command", required=True)

	#
	# Register
	#

	p_register = subparsers.add_parser("register", help="Register an occ position for auto strategy management")
	p_register.add_argument("--strategy", required=True)
	p_register.add_argument("--occ", required=True)
	p_register.add_argument("--symbol", required=True)
	p_register.add_argument("--capture-threshold", type=float, default=0.85)
	p_register.add_argument("--price-point", choices=["ask", "bid", "mid"], default="mid")
	p_register.add_argument("--entry-order-id", type=int, default=None)


	#
	# Enable
	#

	p_enable = subparsers.add_parser("enable", help="Enable auto management for registered position")
	p_enable.add_argument("--strategy", required=True)
	p_enable.add_argument("--occ", required=True)


	#
	# Disable
	#

	p_disable = subparsers.add_parser("disable", help="Disable auto management for registered position")
	p_disable.add_argument("--strategy", required=True)
	p_disable.add_argument("--occ", required=True)


	#
	# List
	#

	p_list = subparsers.add_parser("list", help="List registered strategy positions")
	p_list.add_argument("--strategy", default=None)
	p_list.add_argument("--all", action="store_true", help="Include disabled positions")


	#
	# Common MinIO Args
	#

	for p in [p_register, p_enable, p_disable, p_list]:
		p.add_argument("--minio-endpoint", default=None)
		p.add_argument("--minio-access-key", default=None)
		p.add_argument("--minio-secret-key", default=None)

	args = parser.parse_args()

	return manage_strategy_positions(
		command = args.command,
		strategy = getattr(args, "strategy", None),
		occ = getattr(args, "occ", None),
		symbol = getattr(args, "symbol", None),
		capture_threshold = getattr(args, "capture_threshold", 0.85),
		price_point = getattr(args, "price_point", "mid"),
		entry_order_id = getattr(args, "entry_order_id", None),
		show_all = getattr(args, "all", False),
		minio_endpoint=getattr(args, "minio_endpoint", None),
		minio_access_key=getattr(args, "minio_access_key", None),
		minio_secret_key=getattr(args, "minio_secret_key", None)
	)



if __name__ == "__main__":
	main()
