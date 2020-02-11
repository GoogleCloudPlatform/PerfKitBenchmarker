#!/bin/bash
sudo apt update
sudo apt install -y python3-venv
/usr/bin/python3 -m venv $HOME/my_virtualenv
source $HOME/my_virtualenv/bin/activate
git clone https://github.com/GoogleCloudPlatform/PerfKitBenchmarker.git
pip install --upgrade pip
pip install -r $HOME/PerfKitBenchmarker/requirements.txt
