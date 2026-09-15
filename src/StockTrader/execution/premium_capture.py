#
# FILE: `StockTrader/src/StockTrader/execution/premium_capture.py`
#

from pathlib import Path

from StockTrader.settings import logger

from StockTrader.execution.orchestrator import OrderPipe
from StockTrader.execution.executor import TraderAdapter, SimpleOrderExecutor
from StockTrader.execution.builders.template_builder import TemplateOrderBuilder
from StockTrader.execution.loaders.position_snapshot_loader import PositionSnapshotLoader
from StockTrader.execution.loaders.managed_position_loader import ManagedPositionLoader
from StockTrader.execution.filters.premium_capture import PremiumCaptureLoader
from StockTrader.execution.persisters.result_persister import MinioResultPersister
from StockTrader.execution.strategy_position_registry import StrategyPositionRegistry

from utils.minio_store import MinioStore


SNAPSHOT_BUCKET = "portfolio-snapshots"
ORDERS_BUCKET = "trading-orders"

TEMPLATE_PATH = (
	Path(__file__).resolve().parent
	/ "templates"
	/ "btc_vrp.yml"
)


def run_premium_capture_close(
	snapshot_id: str,
	strategy: str,
	options_client,
	minio_store: MinioStore,
	capture_threshold: float = 0.85,
	price_point: str = "mid",
	dry_run: bool = True
):

	#
	# Validate Runtime Inputs
	#

	if not snapshot_id:
		raise ValueError(
			"snapshot_id required [run_premium_capture_close]"
		)

	if not strategy:
		raise ValueError(
			"strategy required [run_premium_capture_close]"
		)

	if price_point not in {"ask", "bid", "mid"}:
		raise ValueError(
			f"Bad price_point={price_point} "
			f"[run_premium_capture_close]"
		)

	if not 0 < capture_threshold <= 1:
		raise ValueError(
			f"capture_threshold={capture_threshold} not in (0,1] "
			f"[run_premium_capture_close]"
		)

	#
	# Strategy Position Registry
	#
	# Registry determines which OCC positions are eligible for automated
	# management by this strategy.
	#

	registry = StrategyPositionRegistry(
		store=minio_store
	)

	#
	# Load Exact Portfolio Snapshot Produced by Upstream Monitor
	#

	snapshot_loader = PositionSnapshotLoader(
		store=minio_store,
		bucket=SNAPSHOT_BUCKET
	)

	#
	# Restrict Snapshot to Enabled Positions Registered for Strategy
	#

	managed_loader = ManagedPositionLoader(
		inner_loader=snapshot_loader,
		registry=registry,
		strategy=strategy
	)

	#
	# Apply Premium-Capture Exit Logic
	#
	# capture_threshold and price_point act as runtime defaults.
	# If PremiumCaptureLoader supports registry policy overrides, the
	# policy_capture_threshold and policy_price_point columns attached by
	# ManagedPositionLoader take precedence for individual positions.
	#

	loader = PremiumCaptureLoader(
		inner_loader=managed_loader,
		capture_threshold=capture_threshold,
		price_point=price_point
	)

	#
	# Build Buy-To-Close Orders
	#

	builder = TemplateOrderBuilder(
		template_path=str(TEMPLATE_PATH)
	)

	#
	# Tradier Order Execution
	#

	adapter = TraderAdapter(
		options_client=options_client,
		equities_client=None
	)

	executor = SimpleOrderExecutor(
		trader_adapter=adapter
	)

	#
	# Persist Order Submission Results
	#

	persister = MinioResultPersister(
		store=minio_store,
		bucket=ORDERS_BUCKET
	)

	#
	# Assemble Order Pipeline
	#

	pipe = OrderPipe(
		loader=loader,
		builder=builder,
		executor=executor,
		persister=persister
	)

	#
	# Execute Pipeline
	#

	logger.info(
		"Starting premium capture execution pipeline: "
		f"snapshot={snapshot_id}, "
		f"strategy={strategy}, "
		f"threshold={capture_threshold:.2%}, "
		f"price={price_point}, "
		f"dry={dry_run} "
		f"[run_premium_capture_close]"
	)

	try:
		result = pipe.run(
			dry_run=dry_run,
			mart=f"{strategy}_premium_capture",
			snapshot_id=snapshot_id
		)

	except ValueError as e:
		#
		# Empty snapshot / no managed positions / no qualifying
		# premium-capture positions are normal intraday outcomes.
		#
		logger.info(
			f"No premium capture orders: "
			f"snapshot={snapshot_id}, "
			f"strategy={strategy}, "
			f"reason={str(e)} "
			f"[run_premium_capture_close]"
		)

		return None

	logger.info(
		"Premium capture execution pipeline complete: "
		f"snapshot={snapshot_id}, "
		f"strategy={strategy}, "
		f"result={result} "
		f"[run_premium_capture_close]"
	)

	return result
