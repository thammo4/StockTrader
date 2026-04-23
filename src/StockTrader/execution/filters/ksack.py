#
# FILE: `StockTrader/src/StockTrader/execution/filters/ksack.py`
#

import numpy as np
import pandas as pd
from StockTrader.settings import logger


def solve_ksack01(credits: np.ndarray, margins: np.ndarray, capacity: float, min_credit: float) -> np.ndarray:
    n = len(credits)
    if n == 0:
        return np.array([], dtype=bool)

    scale = 1

    w = np.round(margins * scale).astype(int)
    v = np.round(credits * scale).astype(int)
    W = int(np.floor(capacity * scale))

    w = np.clip(w, 1, None)

    dp = np.zeros(W + 1, dtype=np.int64)
    keep = np.zeros((n, W + 1), dtype=bool)

    for i in range(n):
        for c in range(W, w[i] - 1, -1):
            if dp[c - w[i]] + v[i] > dp[c]:
                dp[c] = dp[c - w[i]] + v[i]
                keep[i, c] = True

    selected = np.zeros(n, dtype=bool)
    c = W
    for i in range(n - 1, -1, -1):
        if keep[i, c]:
            selected[i] = True
            c -= w[i]

    total_credit = credits[selected].sum() if selected.any() else 0.0
    if total_credit < min_credit:
        logger.info(f"ksack credit = {total_credit:.2f} < min(credit) = {min_credit:.2f} [ksack]")
        return np.zeros(n, dtype=bool)

    total_margin = margins[selected].sum() if selected.any() else 0.0
    logger.info(
        f"ksack={selected.sum()}/{n}, credit={total_credit:.2f}, margin={total_margin:.2f}, cap={capacity:.2f} [ksack]"
    )

    return selected
