#!/bin/bash

#
# FILE: `StockTrader/dev/env_setup/gcp_vm_debian.sh`
#

set -e
exec > >(tee /var/log/stocktrader-startup.log) 2>&1

echo "Starting StockTrader dev env setup..."
echo "TS: $(date)"
echo "User: $(whoami)"


echo "Updating system packages..."

sudo apt-get update


echo "Installing python dev tools, git..."
sudo apt install -y python3-pip
sudo apt install -y python3-venv
sudo apt install -y git

echo "Cloning StockTrader repo..."
cd $HOME
git clone https://github.com/thammo4/StockTrader.git
git clone https://github.com/thammo4/uvatradier.git


echo "Creating setup completion marker..."
cd $HOME/StockTrader
touch /tmp/stocktrader-setup-complete

echo "Setup Done at $(date)" >> /tmp/stocktrader-setup-complete

echo "Stocktrader dev env setup ok"
echo "TS: $(date)"
echo "Log: /var/log/stocktrader-startup.log"
