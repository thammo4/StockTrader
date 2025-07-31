#!/bin/bash

#
# FILE: `StockTrader/dev/env_setup/gcp_vm_debian.sh`
#


cd $HOME
sudo apt-get update
sudo apt install -y python3-pip
sudo apt install -y python3-venv
sudo apt install -y git

git clone https://github.com/thammo4/StockTrader.git
git clone https://github.com/thammo4/uvatradier.git

cd $HOME/StockTrader
