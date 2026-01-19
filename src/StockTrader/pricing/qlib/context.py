#
# FILE: `StockTrader/src/StockTrader/pricing/qlib/context.py`
#


"""
QuantLib Context Management.

Functionality/Purpose:
	1. Map string dates to QuantLib date objects (e.g. ql.Date)
	2. Define global QuantLib evaluation dates prior to pricing
	3. Construct shared calendar and day counter QuantLib objects
"""

import QuantLib as ql
from typing import Tuple
from datetime import date


class QLibContext:
	def __init__ (self):
		self.calendar = ql.UnitedStates(ql.UnitedStates.NYSE)
		self.day_counter = ql.Actual365Fixed()
		self._date_cache = {}

	def to_ql_date (self, d: date) -> ql.Date:
		"""Convert datetime.date  obj to QuantLib ql.Date (+cache)"""
		if d not in self._date_cache:
			self._date_cache[d] = ql.Date(d.day, d.month, d.year)
		return self._date_cache[d]

	def set_eval_date (self, d: date) -> ql.Date:
		eval_date = self.to_ql_date(d)
		ql.Settings.instance().evaluationDate = eval_date
		return eval_date


#
# Singleton Pattern ~ Module-Level Variable
#

# _context: QLibContext = None
_context: Optional[QlibContext] = None

def get_context() -> QLibContext:
	global _context
	if _context is None:
		_context = QLibContext()
	return _context

