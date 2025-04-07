#!/bin/bash

#
# FILE: `StockTrader/scripts/dolt_historical_ticker.sh`
# Execute SQL query to DoltHub to download raw CSV of historical options chain data for a single ticker symbol
#
 
#
# Verify Global Directory Variables Defined...
#

if [ -z "$STOCK_TRADER_MARKET_DATA" ]; then
	echo "Error: STOCK_TRADER_MARKET_DATA undefined."
	exit 1;
fi


#
# Verify Correct Usage...
#

if [ "$#" -lt 1 ]; then
	echo "Usage: $0 <act_symbol> [output_dir] [past_days]";
	echo "Example: ./dolt_historical_ticker.sh IBM";
	echo "Default output_dir: $STOCK_TRADER_MARKET_DATA";
	exit 1;
fi


#
# Verify dolt is installed
#

if ! dolt version &> /dev/null; then
	echo "Error: Dolt either not installed or not in PATH"
	exit 1
fi

echo "Running...";

#
# Parse Arguments
#

act_symbol=$1
output_dir=${2:-$STOCK_TRADER_MARKET_DATA};
past_days=${3:-365}


#
# Define Output File and Print to Standard Output
#

output_file="$output_dir/${act_symbol}_options_data.parquet";

echo "act_symbol: $act_symbol";
echo "output_dir: $output_dir";
echo "output_file: ${output_file}";

query="SELECT
	CAST(1e9*UNIX_TIMESTAMP(\`date\`) AS UNSIGNED) AS date,
	CAST(1e9*UNIX_TIMESTAMP(\`expiration\`) AS UNSIGNED) AS expiration,
	DATEDIFF(\`expiration\`, \`date\`) AS ttm,
	CAST(.50*(\`bid\` + \`ask\`) AS DOUBLE) AS midprice,
	CAST(\`strike\` AS DOUBLE) AS strike,
	\`call_put\`,
	\`act_symbol\`
FROM \`option_chain\`
WHERE \`act_symbol\`='$act_symbol'
AND \`date\` >= DATE_SUB('$(date +%Y-%m-%d)', INTERVAL $past_days DAY)"

echo -e "Query:\n${query}";

dolt sql -r parquet -q "$query" > "$output_file";

#
# Extract the first and last date from options data parquet using duckdb
#

date_range=$(duckdb -csv -c "SELECT MIN(date), MAX(date) FROM '${output_file}'" | tail -n +2);
date_range_txt="$output_dir/${act_symbol}_date_range.txt";

#
# If the start_date and end_date are valid, then create a text file in the $output_dir whose name is ${act_symbol}_date_range.txt such that:
# 	1. the first line of ${act_symbol}_date_range.txt is the start_date
# 	2. the second line of the ${act_symbol}_date_range.txt is the end_date
#
start_date=$(echo "$date_range" | cut -d, -f1);
end_date=$(echo "$date_range" | cut -d, -f2);

#
# If there does not exist a start_date and end_date, then remove the $output_file and exit
#

if [ -z "$start_date" ] || [ "$start_date" = "NULL" ] || [ -z "$end_date" ] || [ "$end_date" = "NULL" ]; then
	echo "JUNK: $output_file";
	echo "SEEYA: $output_file";
	rm -f "$output_file";
	exit 1;
fi

echo "Date Range File: $date_range_txt"
echo "Dates: start=${start_date}, end=${end_date}";

echo "$start_date" > "$date_range_txt";
echo "$end_date" >> "$date_range_txt";

echo -e "\nDone.";