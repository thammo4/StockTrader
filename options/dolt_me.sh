#!/bin/bash

#
# FILE: `StockTrader/options/dolt_me.sh`
#
# Retrieve historical stock options data from DoltHub `option_chain` table.
# Make small changes to MySQL plaintext table of results and store as local and valid CSV.
#

#
# Count/Check Arguments
#

if [ "$#" -ne 1 ]; then
	echo "Usage: $0 <act_symbol>";
	exit 1;
fi

echo "Running...";


#
# Assign Ticker Symbol and Date Bound Args to Script Vars
#

act_symbol=$1
echo "act_symbol: $act_symbol";

#
# Create Output File to Store Data
#

output_file="/Users/thammons/Desktop/msds/tradier/StockTrader/options/${act_symbol}_options_data.csv";

echo "Output File: ${output_file}";


#
# (Construct SQL Query using CLI Symbol Arg) -> (Execute Dolt SQL Command) -> (Save Returned Garb to File)
#

query="SELECT \`date\`, \`expiration\`, DATEDIFF(\`expiration\`, \`date\`) AS ttm, .50 * (\`bid\` + \`ask\`) AS midprice, \`strike\`, \`call_put\`, \`act_symbol\` 
       FROM \`option_chain\` 
       WHERE \`act_symbol\`='$act_symbol' AND \`date\` >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)"
echo "Query:>>>${query}";


#
# Execute Dolt -> Tweak Text Output Received
#   • Remove the 1st row of the table that immediately precedes the column titles, begins/ends with `+`
#   • Removes the 3rd row of the table that immediately follows after the column titles, begins/ends with `+`
#   • Removes the last line of the table which immediately follows after the final record returned in the result set, begins/ends with `+`.
#

sudo dolt sql -q "$query" | sed '1d;3d;$d' > "$output_file"
sed -i '' '$d' "$output_file";


# Remove last line only if it's empty
# sed -i '' '${/^$/d;}' "$output_file"



#
# Confrim Success or Inform Caller about Error
#

is_file_empty() {
    [ ! -s "$1" ]
}

if is_file_empty "$output_file"; then
    echo "EMPTY: $output_file";
    echo "Removing $output_file";
    rm "$output_file";
    exit 1;
else
    echo "SAVED: $output_file";
    echo "HEAD"; echo "";
    nl "$output_file" | head -n 5
    echo "";
    echo "TAIL"; echo "";
    nl "$output_file" | tail -n 5
    echo "--------------------------";
fi



# if [ $? -eq 0 ]; then
#     echo "Data saved to $output_file"

#     echo "HEAD"; echo "";
#     nl "$output_file" | head -n 5
#     echo "";

#     echo "TAIL"; echo "";
#     nl "$output_file" | tail -n 5
#     echo "---------------";
# else
#     echo "Failed to retrieve data"
#     exit 1
# fi

echo " DoltMe - Done."


#
# OUTPUT SAMPLE - Moody's Corp ($MCO)
#

# % ./dolt_me.sh MCO
# Running...
# act_symbol: MCO
# ~/StockTrader/options/MCO_options_data.csv
# Query:>>>SELECT `date`, `expiration`, DATEDIFF(`expiration`, `date`) AS ttm, 50 * (`bid` + `ask`) AS midprice, `strike`, `call_put`, `act_symbol` 
#        FROM `option_chain` 
#        WHERE `act_symbol`='MCO' AND `date` >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
# ~/StockTrader/options/MCO_options_data.csv
# HEAD
#
#      1  | date       | expiration | ttm | midprice | strike | call_put | act_symbol |
#      2  | 2023-09-15 | 2023-09-15 | 0   | 10140.0  | 240.00 | Call     | MCO        |
#      3  | 2023-09-15 | 2023-09-15 | 0   | 2.50     | 240.00 | Put      | MCO        |
#      4  | 2023-09-15 | 2023-09-15 | 0   | 9150     | 250.00 | Call     | MCO        |
#      5  | 2023-09-15 | 2023-09-15 | 0   | 2.50     | 250.00 | Put      | MCO        |
#
# TAIL
#
#  15961  | 2024-08-28 | 2024-10-18 | 51  | 10725.00 | 590.00 | Put      | MCO        |
#  15962  | 2024-08-28 | 2024-10-18 | 51  | 225.00   | 600.00 | Call     | MCO        |
#  15963  | 2024-08-28 | 2024-10-18 | 51  | 11645.00 | 600.00 | Put      | MCO        |
#  15964  | 2024-08-28 | 2024-10-18 | 51  | 220.00   | 620.00 | Call     | MCO        |
#  15965  | 2024-08-28 | 2024-10-18 | 51  | 13685.00 | 620.00 | Put      | MCO        |
# ---------------
# Done.