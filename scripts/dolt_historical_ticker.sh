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

query="SELECT \`date\`, \`expiration\`, DATEDIFF(\`expiration\`, \`date\`) AS ttm, .50*(\`bid\` + \`ask\`) AS midprice, \`strike\`, \`call_put\`, \`act_symbol\`
FROM \`option_chain\`
WHERE \`act_symbol\`='$act_symbol'
AND \`date\` >= DATE_SUB('$(date +%Y-%m-%d)', INTERVAL $past_days DAY)"

echo -e "Query:\n${query}";

dolt sql -r parquet -q "$query" > "$output_file";

is_file_empty() {
	[ ! -s "$1" ]
}

echo -e "\nDone.";