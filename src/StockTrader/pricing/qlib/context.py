#
# FILE: `StockTrader/src/StockTrader/pricing/qlib/context.py`
#

import QuantLib as ql
from typing import Tuple
from functools import lru_cache


class QLibContext:
	def __init__ (self):
		self.calendar = ql.UnitedStates(ql.UnitedStates.NYSE)
		self.day_counter = ql.Actual365Fixed()
		self._date_cache = {}

	def parse_date (self, date_str: str) -> ql.Date:
		if date_str not in self._date_cache:
			year, month, day = map(int, date_str.split("-"))
			self._date_cache[date_str] = ql.Date(day, month, year)
		return self._date_cache[date_str]

	def set_eval_date (self, date_str: str) -> ql.Date:
		eval_date = self.parse_date(date_str)
		ql.Settings.instance().evaluationDate = eval_date
		return eval_date

_context: QLibContext = None

def get_context() -> QLibContext:
	global _context
	if _context is None:
		_context = QLibContext()
	return _context

