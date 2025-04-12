#!/bin/bash

BASE_DIR="$HOME/Desktop/StockTrader"
JDK_DIR="$HOME/.jdk"
SPARK_VERSION="3.5.5"
SPARK_DIR="$HOME/spark"

ENV_VARS=$(cat <<EOF

# StockTrader Environment Setup
export STOCK_TRADER_HOME="$BASE_DIR"
export STOCK_TRADER_MARKET_DATA="\$STOCK_TRADER_HOME/data"
export STOCK_TRADER_LOG="\$STOCK_TRADER_HOME/logs"
export STOCK_TRADER_DWH="\$STOCK_TRADER_MARKET_DATA/warehouse"
EOF
)

if ! grep -q "StockTrader Environment Setup" ~/.zshrc; then
	echo "$ENV_VARS" >> ~/.zshrc
	echo "Added environment vars to ~/.zshrc: STOCK_TRADER_HOME, STOCK_TRADER_MARKET_DATA, STOCK_TRADER_DWH, STOCK_TRADER_LOG"
else
	echo "Env vars configured."
fi
echo "Done."

echo "Loading updated zshrc"
source ~/.zshrc

echo "Installing Python Version 3.12..."
brew install python@3.12

echo "Installing Dolt..."
brew install dolt

echo "Installing DuckDB..."
brew install duckdb

echo "Creating Application Directory Structure..."
mkdir -p $STOCK_TRADER_HOME
mkdir -p $STOCK_TRADER_MARKET_DATA
mkdir -p $STOCK_TRADER_LOG
mkdir -p $STOCK_TRADER_DWH

echo "Configuring DoltHub Historical Options Chain DB..."
cd $STOCK_TRADER_HOME
dolt config --global --add user.email "yo@mama.com"
dolt config --global --add user.name "yo mama"

echo "Initializing DoltHub Repo..."
dont init

echo "Cloning post-no-preference/options..."
dolt clone post-no-preference/options
cp -R options/.dolt .

echo "Pulling Latest Options Chain Data..."
dolt pull origin master
