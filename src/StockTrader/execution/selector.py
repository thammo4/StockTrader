#
# FILE: `StockTrader/src/StockTrader/execution/selector.py`
#


import pandas as pd
from pathlib import Path

class ThresholdCandidateSelector:
	"""Select candidates not already in the portfolio"""
	def select (self, candidates:pd.DataFrame, current_positions:pd.DataFrame) -> pd.DataFrame:
		current_occs = set()
		if not current_positions.empty and "occ" in current_positions.columns:
			current_occs = set(current_positions["occ"].values)

		# selected = candidates[~candidates["occ"].isin(current_occs)].copy()
		selected = candidates[~candidates["occ"].astype(str).isin(current_occs)].copy()

		selected["quantity"] = 1

		return selected
