#!/bin/bash

#
# FILE: `StockTrader/dev/pull_changes.sh`
#

cd $HOME/uvatradier

git fetch origin
if [ $(git rev-list HEAD...origin/main --count) != 0 ]; then
	echo "Pulling changes uvatradier..."
	git pull origin main
else
	echo "No change uvatradier"
fi


cd $HOME/StockTrader

git fetch origin
if [ $(git rev-list HEAD...origin/main --count) != 0 ]; then
	echo "Pulling changes StockTrader..."
	git pull origin main
else
	echo "No change StockTrader"
fi


