#!/bin/bash

#
# FILE: `StockTrader/options/download_dolt_data.sh`
#
# This script iterates over a list of symbols (passed as the only argument) in a text file located in the same working directory.
# For each symbol, it calls the `dolt_me.sh` script, which exectutes a MySQL query to retrieve historical options data from DoltHub `option_chain` table.
#

# is_empty_csv() {
# 	local filename="$1";
# 	if [ ! -s "$filename" ]; then
# 		return 0;
# 	else
# 		return 1;
# }

if [ "$#" -ne 1 ]; then
	echo "Usage: $0 <stock_list_text_file>";
	exit 1;
fi;

input_file=$1;

error_symbols=();

if [ ! -f "$input_file" ]; then
	echo "Error: $input_file not found";
	exit 1;
fi

while IFS= read -r symbol
do
	echo "Processing: $symbol";

	if ! ./dolt_me.sh "$symbol"; then
		echo "ERROR: $symbol.";
		error_symbols+=("$symbol");
	fi

	echo "------------------------------";

done < "$input_file";

if [ ${#error_symbols[@]} -ne 0 ]; then
	echo "List of Errors:";
	printf "'%s\n' ${error_symbols[@]}";
else
	echo "All symbols kosher".
fi

echo "Download - Done.";