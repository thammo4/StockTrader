#
# FILE: `StockTrader/src/StockTrader/execution/protocols.py`
#

"""
	Abstract Interface for Execution Pipeline

	Implements:
		- ThresholdCandidateSelector
			- read mart data candidates
			- apply basic filters
		- TradierOrderExecutor
			- uvatradier order submission
			- parquet logs
		- TradierPositionMonitor
			- polls acct positions
			- order log reconciliation
		- SimpleM2M
			- compares live quotes / entry credit
			- computes upl
		- ThresholdExitEvaluator
			- profit target
			- stop loss
			- dte rules
"""

from __future__ import annotations
from typing import Protocol, runtime_checkable
import pandas as pd


@runtime_checkable
class CandidateSelector (Protocol):
	"""
	Identifies mart output candidates to trade.
	"""

	def select (self, candidates:pd.DataFrame, current_positions:pd.DataFrame) -> pd.DataFrame:
		"""
		Returns subset of candidates with 'quantity' column indicating number of contracts to transact.
		"""
		pass


@runtime_checkable
class OrderExecutor (Protocol):
	"""
	Submits orders to broker.
	Logs results.
	"""

	def submit(self, candidates: pd.DataFrame) -> pd.DataFrame:
		"""
		Submits order for each record in input 'candidates' dataframe.
		"""
		pass

	def sync_fill_status (self) -> pd.DataFrame:
		""" Polls broker for fill status of all submitted orders -> updates order log. Returns changed statuses."""
		pass


@runtime_checkable
class PositionMonitor (Protocol):
	""" Reconcile broker position state with local order log"""
	def refresh (self) -> pd.DataFrame:
		""" Returns position dataframe: broker state + entry context"""
		pass

@runtime_checkable
class M2M (Protocol):
	""" Computes unrealized pnl using live quote data. """
	def mark (self, positions: pd.DataFrame) -> pd.DataFrame:
		"""Returns positions augmented with live quotes and pnl columns"""
		pass


@runtime_checkable
class ExitEvaluator (Protocol):
	"""Identifies positions to close out"""
	def evaluate (self, marked_positions: pd.DataFrame) -> pd.DataFrame:
		"""Returns subset of positions to close out"""
		pass

