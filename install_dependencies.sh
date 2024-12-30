#!/bin/bash
python3 -m venv .venv
source .venv/bin/activate
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip libpq-dev
pip install -r requirements.txt
