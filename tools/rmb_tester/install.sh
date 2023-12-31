#!/usr/bin/env bash
echo ">> creating and activating the virtual environment .."
python3 -m venv venv
source venv/bin/activate
echo ">> installing .."
python3 -m pip install --upgrade pip
pip install -r ./requirements.txt
echo "deactivating the virtual environment .."
deactivate
echo ">> install complete!"
echo ">> to activate the virtual environment use 'source venv/bin/activate'"
echo ">> or use ./test-live-nodes.sh script"
echo ">> example: MNEMONIC=[MNEMONIC] ./test-live-nodes.sh [dev,qa,test,main]"
echo ">> example: MNEMONIC=[MNEMONIC] TIMEOUT=[SECONDS] RMB_BIN=[BINARY-PATH] ./test-live-nodes.sh [dev,qa,test,main]"
