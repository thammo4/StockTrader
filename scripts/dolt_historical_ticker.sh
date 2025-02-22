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
	echo "Usage: $0 <act_symbol> [output_dir]";
	echo "Example: ./dolt_historical_ticker.sh IBM";
	echo "Default output_dir: $STOCK_TRADER_MARKET_DATA";
	exit 1;
fi


#
# Verify dolt is installed
#

if ! sudo dolt version &> /dev/null; then
	echo "Error: Dolt either not installed or not in PATH"
	exit 1
fi

echo "Running...";

act_symbol=$1
output_dir=${2:-$STOCK_TRADER_MARKET_DATA};
output_file="$STOCK_TRADER_MARKET_DATA/${act_symbol}_options_data.csv";

echo "act_symbol: $act_symbol";
echo "output_dir: $output_dir";
echo "output_file: ${output_file}";

query="SELECT \`date\`, \`expiration\`, DATEDIFF(\`expiration\`, \`date\`) AS ttm, .50*(\`bid\` + \`ask\`) AS midprice, \`strike\`, \`call_put\`, \`act_symbol\`
FROM \`option_chain\`
WHERE \`act_symbol\`='$act_symbol'
AND \`date\` >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)"

echo -e "Query:\n${query}";

# sudo -E dolt sql -q "$query" | sed '1d;3d;$d' > "$output_file";
sudo -E dolt sql -r csv -q "$query" > "$output_file"
# sed -i '' '$d' "$output_file";

is_file_empty() {
	[ ! -s "$1" ]
}


#
# If the `options_chain` table in DoltHub has no data,
# delete the empty CSV file created.
#

if is_file_empty "$output_file"; then
	echo "EMPTY: $output_file";
	echo "REMOVING: $output_file";
	rm "$output_file";
	exit 1;
else
	echo "SAVED: $output_file";
	echo -e "HEAD\n";
	nl "$output_file" | head -n 5;
	echo "";
	echo -e "TAIL\n";
	nl "$output_file" | tail -n 5;
	echo "-----------------------";
fi

echo "Done.";