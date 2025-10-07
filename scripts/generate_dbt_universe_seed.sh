#
# FILE: `StockTrader/scripts/generate_dbt_universe_seed.sh`
#

#!/usr/bin/env bash
set -euo pipefail

cd $STOCK_TRADER_HOME

STOCK_SYMBOL_TXT="largecap_all.txt"
STOCK_SYMBOL_CSV="dbt/seeds/universe/largecap_all.csv"

echo "Running generate_dbt_universe_seed.sh"
echo "Symbol text source: $STOCK_SYMBOL_TXT"
echo "Symbol csv target: $STOCK_SYMBOL_CSV"

# mkdir -p "$(dirname '$STOCK_SYMBOL_CSV')"
mkdir -p $(dirname $STOCK_SYMBOL_CSV)

echo "Creating $STOCK_SYMBOL_CSV from $STOCK_SYMBOL_TXT"

awk 'NF && $0 !~ /^$#/' "$STOCK_SYMBOL_TXT" \
| sed 's/^[ \t]*//;s/[ \t]*$//' \
| awk '{print toupper($0)}' \
| awk 'BEGIN{print "symbol"} {print}' \
> "$STOCK_SYMBOL_CSV"

if [ ! -s "$STOCK_SYMBOL_CSV" ]; then
	echo "ERROR: empty file $STOCK_SYMBOL_CSV" >&2
	exit 1
fi

# command -v dos2unix > /dev/null 2>&1 && dos2unix "$CSV" || true
command -v dos2unix > /dev/null 2>&1 && dos2unix "$STOCK_SYMBOL_CSV" || true

echo "Done."