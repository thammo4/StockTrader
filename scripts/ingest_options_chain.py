#
# FILE: `StockTrader/scripts/ingest_options_chain.py`
#

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from StockTrader.tradier import options_data
from StockTrader.settings import STOCK_TRADER_DWH, logger, today


def ingest_options_chain(symbol):
    try:
        df = options_data.get_chain_all(symbol=symbol)

        if df.empty:
            logger.warning(f"No options chain data, symbol={symbol} [ingest_options_chain]")
            return

        df["created_date"] = today

        dwh_options = os.path.join(STOCK_TRADER_DWH, "options")
        fpath_parquet = os.path.join(dwh_options, f"{symbol}.parquet")

        if os.path.exists(fpath_parquet):
            df_existing = pd.read_parquet(fpath_parquet)
            df = pd.concat([df_existing, df])
            df = df.drop_duplicates(subset=["symbol", "created_date"])

        df.to_parquet(fpath_parquet, index=False, engine="pyarrow")

        logger.info(f"Options chain data loaded ok, symbol={symbol} [ingest_options_chain]")

    except Exception as e:
        logger.error(f"Failed options chain ingestion, symbol={symbol}: {str(e)} [ingest_options_chain]")


if __name__ == "__main__":
    BIG_LIST = [
        "BK",
        "STT",
        "CL",
        "HIG",
        "BG",
        "ED",
        "KEY",
        "CFG",
        "MCK",
        "DE",
        "PG",
        "SWK",
        "PNC",
        "CHD",
        "CME",
        "PFE",
        "AXP",
        "GLW",
        "WFC",
        "TRV",
        "GIS",
        "MTB",
        "FITB",
        "IR",
        "UNP",
        "HBAN",
        "SHW",
        "MET",
        "CPB",
        "GS",
        "TT",
        "KMB",
        "TFC",
        "PRU",
        "LLY",
        "CVX",
        "LIN",
        "PFG",
        "BALL",
        "BWA",
        "NSC",
        "CNP",
        "FMC",
        "KR",
        "PPG",
        "JCI",
        "AWK",
        "KO",
        "EIX",
        "JNJ",
        "MRO",
        "ABT",
        "EQT",
        "HWM",
        "HUBB",
        "MKC",
        "NTRS",
        "EMR",
        "HRL",
        "MRK",
        "AIZ",
        "GE",
        "AMP",
        "HSY",
        "WEC",
        "BDX",
        "SJM",
        "IP",
        "PEP",
        "EFX",
        "GD",
        "GL",
        "WY",
        "MMM",
        "AEE",
        "ADM",
        "TGT",
        "F",
        "PEG",
        "ROK",
        "DUK",
        "LOW",
        "CHRW",
        "MMC",
        "PCAR",
        "PCG",
        "AEP",
        "ATO",
        "HON",
        "K",
        "OKE",
        "UPS",
        "ALLE",
        "WMB",
        "EVRG",
        "MCO",
        "VMC",
        "XEL",
        "ETN",
        "IBM",
        "FCX",
        "ITW",
        "NI",
        "CLX",
        "ETR",
        "AOS",
        "BA",
        "LNT",
        "PH",
        "SPGI",
        "CE",
        "AIG",
        "CAG",
        "CMI",
        "HAL",
        "HES",
        "HLT",
        "EMN",
        "OXY",
        "PPL",
        "SNA",
        "NEM",
        "RTX",
        "ECL",
        "HAS",
        "TXT",
        "DIS",
        "WST",
        "CAT",
        "GPC",
        "SLB",
        "AJG",
        "MAR",
        "GWW",
        "ZBH",
        "MSI",
        "CTAS",
        "DAL",
        "MAS",
        "TXN",
        "ALL",
        "BAX",
        "APH",
        "AAL",
        "ODFL",
        "SW",
        "IVZ",
        "MS",
        "NDSN",
        "TSN",
        "PGR",
        "RVTY",
        "TROW",
        "DRI",
        "TSCO",
        "BRO",
        "DG",
        "HPQ",
        "APD",
        "MCD",
        "NUE",
        "SYK",
        "TFX",
        "STZ",
        "SO",
        "CF",
        "EL",
        "BEN",
        "J",
        "ROL",
        "ADP",
        "MDT",
        "CINF",
        "IRM",
        "ZTS",
        "NXPI",
        "APA",
        "LEN",
        "AFL",
        "DOV",
        "FICO",
        "PHM",
        "COO",
        "KIM",
        "V",
        "IFF",
        "GNRC",
        "L",
        "PKG",
        "DPZ",
        "TER",
        "AVGO",
        "HUM",
        "JBHT",
        "VTRS",
        "IPG",
        "BR",
        "FRT",
        "RJF",
        "WMT",
        "BBWI",
        "CMCSA",
        "REG",
        "NKE",
        "ADI",
        "BBY",
        "ES",
        "JBL",
        "MA",
        "PNR",
        "TYL",
        "AMAT",
        "FAST",
        "DGX",
        "RL",
        "LUV",
        "UAL",
        "WRB",
        "FIS",
        "HCA",
        "INTC",
        "USB",
        "WM",
        "AMD",
        "DHR",
        "EQR",
        "LDOS",
        "MSCI",
        "O",
        "SYY",
        "BXP",
        "WDC",
        "CAH",
        "SCHW",
        "DVN",
        "ESS",
        "FDX",
        "NDAQ",
        "PAYX",
        "RF",
        "SBUX",
        "VRSK",
        "CCL",
        "PSA",
        "UDR",
        "CBOE",
        "EG",
        "MSFT",
        "KLAC",
        "TECH",
        "COST",
        "JKHY",
        "KKR",
        "AAPL",
        "EXR",
        "MAA",
        "ORCL",
        "UNH",
        "AVB",
        "DHI",
        "FDS",
        "HD",
        "LH",
        "MU",
        "EXPD",
        "IT",
        "STX",
        "UHS",
        "AMGN",
        "CSX",
        "LRCX",
        "VLO",
        "AES",
        "CPT",
        "KDP",
        "ROP",
        "ADBE",
        "CI",
        "EA",
        "GEN",
        "ROST",
        "AON",
        "D",
        "INTU",
        "PLD",
        "VZ",
        "T",
        "CDW",
        "CSCO",
        "NEE",
        "MO",
        "BX",
        "COR",
        "CB",
        "DFS",
        "DOC",
        "PNW",
        "QCOM",
        "STE",
        "MTCH",
        "MGM",
        "OMC",
        "GILD",
        "TJX",
        "BLK",
        "IEX",
        "LVS",
        "ACN",
        "GRMN",
        "MCHP",
        "RMD",
        "SBAC",
        "BMY",
        "AVY",
        "ULTA",
        "NTAP",
        "NRG",
        "HST",
        "MLM",
        "NVDA",
        "POOL",
        "STLD",
        "TDG",
        "ARE",
        "APTV",
        "COF",
        "CTSH",
        "CCI",
        "GEHC",
        "TMUS",
        "NOC",
        "AMT",
        "DTE",
        "EBAY",
        "LMT",
        "VRSN",
        "BKNG",
        "CVS",
        "EXPE",
        "JNPR",
        "FE",
        "KMI",
        "MPWR",
        "PWR",
        "RCL",
        "URI",
        "YUM",
        "GOOGL",
        "GOOG",
        "C",
        "EQIX",
        "LKQ",
        "PAYC",
        "SRE",
        "VTR",
        "BAC",
        "RSG",
        "A",
        "CEG",
        "EOG",
        "XOM",
        "CRM",
        "WAB",
        "EXC",
        "GPN",
        "ICE",
        "MKTX",
        "JPM",
        "COP",
        "WYNN",
        "SPG",
        "SYF",
        "DLR",
        "META",
        "MOS",
        "TRGP",
        "TAP",
        "TMO",
        "FANG",
        "LYB",
        "TEL",
        "PM",
        "MPC",
        "HII",
        "XYL",
        "INVH",
        "MDLZ",
        "PSX",
        "ABBV",
        "NWSA",
        "NWS",
        "WBA",
        "ELV",
        "HPE",
        "KHC",
        "FTV",
        "VST",
        "WTW",
        "LW",
        "BKR",
        "TPR",
        "VICI",
        "DD",
        "CTVA",
        "FOXA",
        "FOX",
        "AMCR",
        "DOW",
        "LHX",
        "PARA",
        "OTIS",
        "CARR",
        "CTRA",
        "KVUE",
        "VLTO",
        "SPY",
    ]
    for s in BIG_LIST:
        ingest_options_chain(s)