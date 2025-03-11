#
# FILE: `StockTrader/scripts/etl_bopm_data.py`
#

import os
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from StockTrader.settings import STOCK_TRADER_MARKET_DATA, logger


def etl_bopm_data(symbol, return_df=False):
    """
    Extract, Transform, Load Data to Prep Input for Binomial Options Pricing Model (BOPM)

    Collect data from locally stored parquet files regarding historical closing stock prices, option prices, dividends, and 3-Month TBills

    Returns:
            Processed and Merged DataFrame if return_df=True else None
    """
    try:
        logger.info(f"Starting etl for bopm data, symbol={symbol} [etl_bopm_data]")

        #
        # Read various types of data sets from `StockTrader/data` parquet files
        #

        def extract_from_parquet(whatchu_need, symbol=None):
            """
            Read in various types of data from parquet files for a given symbol

            Args:
                    whatchu_need (str): Data sought ('ohlcv_bar', 'options', 'dividend', 'fred')
                    symbol (str): Stock ticker symbol

            Returns:
                    DataFrame with parquet data and formatted date columns
            """
            try:
                if whatchu_need == "dividends":
                    whatchu_need = "dividend"

                if whatchu_need not in ["ohlcv_bar", "options", "dividend", "fred"]:
                    logger.error(f"No filetype requested, symbol={symbol} [etl_bopm_data]")
                    return None

                parquet_file = (
                    f"{symbol}_{whatchu_need}_data.parquet" if whatchu_need != "fred" else "fred_data.parquet"
                )
                fpath_parquet = os.path.join(STOCK_TRADER_MARKET_DATA, parquet_file)

                df = spark.read.parquet(fpath_parquet)
                if df.isEmpty():
                    logger.warning(f"Empty dframe, what={whatchu_need}, symbol={symbol} [etl_bopm_data]")
                    return None

                #
                # Cast Date Columns from Unix TS in Parquet File to Date Type
                #

                if whatchu_need in ["ohlcv_bar", "options"]:
                    df = df.withColumn("date", to_date(from_unixtime(col("date") / 1e9)))
                    if whatchu_need == "options":
                        df = df.withColumn("expiration", to_date(from_unixtime(col("expiration") / 1e9)))
                elif whatchu_need == "dividend":
                    df = df.withColumn("ex_date", to_date(from_unixtime(col("ex_date") / 1e9)))
                elif whatchu_need == "fred":
                    df = df.withColumn("fred_date", to_date(from_unixtime(col("fred_date") / 1e9)))
                return df

            except Exception as e:
                logger.error(
                    f"Failed to extract parquet data for symbol={symbol}, data={whatchu_need}: {str(e)} [etl_bopm_data]"
                )
                logger.debug(f"Stack trace: {traceback.format_exc()}")
                raise

        #
        # Construct Time-Intervals for the Start and End of Each Dividend Distribution Period
        #

        def construct_div_periods(div_df):
            """
            Construct time intervals representing dividend distribution periods per ex-dividend dates.

            Args:
                    div_df (Spark DataFrame): Historical dividend data with ex_date and cash_amount

            Returns:
                    DataFrame originally input with start_date and end_date columns appended.
            """
            try:
                logger.info(f"Constructing dividend periods, symbol={symbol} [etl_bopm_data]")

                if div_df.isEmpty():
                    logger.warning("Empty dividend dataframe [etl_bopm_data]")
                    # return div_df
                    return None

                cols_required = ["symbol", "ex_date", "cash_amount"]
                cols_missing = [c for c in cols_required if c not in div_df.columns]
                if cols_missing:
                    logger.error(f"Missing columns from div_df: {cols_missing} [etl_bopm_data]")
                    raise ValueError(f"Missing columns {cols_missing} from dividend dataframe")
                    return None

                #
                # Define Spark Window Specification
                # Define Current Period Start Date Per Prev Period Ex-Dividend Date
                #

                window_spec = Window.partitionBy("symbol").orderBy("ex_date")
                div_df = div_df.withColumn("prev_ex_date", F.lag("ex_date", 1).over(window_spec))
                div_period_df = div_df.withColumn(
                    "start_date",
                    F.when(F.col("prev_ex_date").isNull(), F.add_months(F.col("ex_date"), -3)).otherwise(
                        F.col("prev_ex_date")
                    ),
                )
                div_period_df = div_period_df.withColumn("end_date", F.date_sub(F.col("ex_date"), 1))
                div_period_df = div_period_df.select("cash_amount", "symbol", "ex_date", "start_date", "end_date")

                logger.debug(f"Created dividend periods, symbol={symbol} [etl_bopm_data]")
                return div_period_df

            except Exception as e:
                logger.error(f"Failed to construct dividend periods, symbol={symbol}: {str(e)} [etl_bopm_data]")
                logger.debug(f"Stack trace: {traceback.format_exc()}")
                raise

        #
        # Merge Dividend Data to Historical Options Data (Cross-Join and Filter)
        # Two Cases:
        # 	1. Interval denoting option contract's remaining life (date,expiration) is proper subset of a single dividend period.
        # 	2. Interval denoting option contract's remaining life spans more than one dividend period.
        # 		• Each dividend period's impact on option price must be prorated per the magnitude of its intersection with the option's life.
        #

        def merge_options_dividends(options_df, div_periods):
            """
            Merge historical option price data and historical dividend distribution data.
            Each option contract (row) can span one or multiple dividend distribution periods.

            Args:
                    options_df (Spark DataFrame): Options data with cols: ['date', 'expiration', 'ttm']
                    div_periods (Spark DataFrame): Dividend periods with cols: ['start_date', 'end_date', 'cash_amount']

            Returns:
                    DataFrame containing historical option prices and time-weighted dividend amounts
            """
            try:
                logger.info(f"Merging options and dividends, symbol={symbol} [etl_bopm_data]")

                if options_df is None or options_df.isEmpty():
                    logger.warning("Options data empty [etl_bopm_data]")
                    return None
                if div_periods is None or div_periods.isEmpty():
                    logger.warning("Dividend data empty [etl_bopm_data]")
                    default_schema = options_df.schema_add("div_amt", "double").add("div_count", "integer")
                    return None

                cols_required_options = ["date", "expiration", "ttm", "midprice", "strike", "call_put", "act_symbol"]
                cols_missing_options = [c for c in cols_required_options if c not in options_df.columns]
                if cols_missing_options:
                    logger.warning(f"No options data, symbol={symbol} [etl_bopm_data]")
                    return None

                cols_required_dividends = ["cash_amount", "start_date", "end_date", "ex_date"]
                cols_missing_dividends = [c for c in cols_required_dividends if c not in div_periods.columns]
                if cols_missing_dividends:
                    logger.warning(f"No dividend data, symbol={symbol} [etl_bopm_data]")
                    return None

                #
                # Cross-Join Dividend Data to Historical Option Data
                # 	• This creates all possible combinations of (options,dividends) order pairs in a single data set
                # 	• Each option row is repeated for every row in the dividend data set -> #rows in xjoin = (#options_rows) * (#dividend_rows)
                #

                logger.debug("Cross-joining options and dividends, symbol={symbol} [etl_bopm_data]")

                df_joined = options_df.crossJoin(div_periods)

                #
                # Filter The Cross-Joined (Options,Dividends) Dataset for Relevant Time Intervals
                # 	1. dividend.start_date <= option.date < dividend.end_date
                # 	2. dividend.start_date <= option.expiration < dividend.end_date
                # 	3. option.date <= dividend.start_date < dividend.end_date <= option.expiration
                #

                logger.debug("Filtering xjoin for time interval intersection")

                df_periods = df_joined.filter(
                    ((F.col("date") >= F.col("start_date")) & (F.col("date") <= F.col("end_date")))
                    | ((F.col("expiration") >= F.col("start_date")) & (F.col("expiration") <= F.col("end_date")))
                    | ((F.col("date") <= F.col("start_date")) & (F.col("expiration") >= F.col("end_date")))
                )

                logger.debug(f"Filtering for {symbol} [etl_bopm_data]")

                if df_periods.isEmpty():
                    logger.warning("Zero rows for (options,dividends) filtered xjoin")
                    return options_df.withColumn("div_amt", F.lit(0.0)).withColumn("div_count", F.lit(0))

                #
                # Define Each Dividend Period Interval As it Intersects with Option Contract's Life
                #

                logger.debug("Prorating dividends, symbol={symbol} [etl_bopm_data]")

                df_intersect = (
                    df_periods.withColumn("period_start", F.greatest(F.col("date"), F.col("start_date")))
                    .withColumn("period_end", F.least(F.col("expiration"), F.col("end_date")))
                    .withColumn("period_days", F.datediff(F.col("period_end"), F.col("period_start")) + 1)
                    .withColumn("period_weight", F.col("period_days") / (F.col("ttm") + 1))
                )

                #
                # Prorate (e.g. weight) Dividend Impact Per The Number of Days in the Intersection
                #

                df_intersect_weighted = df_intersect.withColumn(
                    "div_weighted", F.col("cash_amount") * F.col("period_weight")
                )

                #
                # Aggregate the Weighted Dividend Amount to Assign a Single Dividend Value to Each Option
                #

                logger.debug("Aggregating prorated dividends per option")

                df_intersect_weighted_grouped = df_intersect_weighted.groupBy(
                    "date", "expiration", "ttm", "midprice", "strike", "call_put", "act_symbol"
                ).agg(F.sum("div_weighted").alias("div_amt"), F.count("ex_date").alias("div_count"))

                logger.info(f"Merged (options,dividends) for {symbol}")

                return df_intersect_weighted_grouped

            except Exception as e:
                logger.error(f"Failed to merge {symbol} (options,dividends): {str(e)} [etl_bopm_data]")
                logger.debug(f"Stack trace: {traceback.format_exc()}")
                raise

        #
        # Verify That The Stock Symbol Pays Dividends.
        # Stop If It Does Not.
        #

        lacks_dividends_marker = os.path.join(STOCK_TRADER_MARKET_DATA, f"{symbol}_lacks_dividends.txt")
        if os.path.exists(lacks_dividends_marker):
            logger.warning(f"{symbol} does not pay dividends [etl_bopm_data]")
            logger.info("Done [etl_bopm_data]")
            return None

        spark = SparkSession.builder.appName("ETL_BOPM_DATA").master("local[*]").getOrCreate()

        #
        # Define Requisite Datasets
        # 	• ohlcv: Closing Prices and Historical Volatility Estimate
        # 	• options: Historical Option Closing Price Data
        # 	• dividends: Historical dividend distribution data and ex-dividend dates
        # 	• fred: Historical Federal Reserve interest rates for 3-Month Treasury Bill
        #

        try:
            logger.info(f"Reading data from parquet for {symbol}")

            df_ohlcv = extract_from_parquet("ohlcv_bar", symbol)
            df_options = extract_from_parquet("options", symbol)

            df_dividends = extract_from_parquet("dividends", symbol)
            df_dividends = construct_div_periods(df_dividends)

            df_fred = extract_from_parquet("fred")
            df_fred = df_fred.withColumnRenamed("fred_date", "month_end")

        except Exception as e:
            logger.error("Failed to read parquet data, symbol={symbol}: {str(e)} [etl_bopm_data]")
            logger.debug(f"Stack trace: {traceback.format_exc()}")
            raise

        try:

            #
            # Construct Merged Dataset
            #

            logger.info("Constructing merged dataset, symbol={symbol} [etl_bopm_data]")

            df_merged = merge_options_dividends(df_options, df_dividends)
            df_merged = df_merged.join(df_ohlcv.select("date", "close", "vol_estimate"), on="date", how="left")
            df_merged = df_merged.withColumn("month_end", F.last_day(F.col("date")))
            df_merged = df_merged.join(df_fred, on="month_end", how="left")
            df_merged = df_merged.drop("month_end").dropna()
            df_merged = df_merged.orderBy("date", "expiration", "call_put", "strike")

            logger.info(f"Merged dataset ok, symbol={symbol} [etl_bopm_data]")
        except AnalysisException as e:
            logger.error(f"Failed merge, analysis, symbol={symbol}: {str(e)} [etl_bopm_data]")
            logger.debug(f"Stack trace: {traceback.format_exc()}")
            raise
        except Exception as e:
            logger.error(f"Failed merged wtf, symbol={symbol}: {str(e)} [etl_bopm_data]")
            logger.debug(f"Stack trace: {traceback.format_exc()}")
            raise

        try:

            #
            # Store Merged Dataset as Parquet for Downstream Use as Input in QuantLib BOPM
            #

            fpath_parquet = os.path.join(STOCK_TRADER_MARKET_DATA, f"{symbol}_bopm_data.parquet")
            logger.info(f"Creating bopm parquet, symbol={symbol} [etl_bopm_data]")

            df_merged.write.parquet(fpath_parquet, mode="overwrite")
            logger.info(f"Created bopm parquet: {fpath_parquet}")
        except Exception as e:
            logger.error(f"Failed to create merged parquet, symbol={symbol}: {str(e)} [etl_bopm_data]")
            logger.debug(f"Stack trace: {traceback.format_exc()}")
            raise

        logger.info("Done [etl_bopm_data]")
        if return_df:
            return df_merged

    except Exception as e:
        logger.error(f"{symbol} bullshittin: {str(e)}")
        logger.debug(f"Stack trace: {traceback.format_exc()}")
        if return_df:
            return None
        raise
