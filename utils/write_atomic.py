#
# `StockTrader/utils/write_atomic.py`
#

import os


#
# PARQUET
#


def write_parquet_atomic(df, fpath_target):
    """Write DF to tmp file in local directory -> replace with target"""
    fpath_tmp = fpath_target + ".tmp"

    try:
        df.to_parquet(fpath_tmp, index=False, engine="pyarrow")
        os.replace(fpath_tmp, fpath_target)
    except Exception:
        if os.path.exists(fpath_tmp):
            os.remove(fpath_tmp)
        raise
