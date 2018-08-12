#!/bin/sh

python3 -m virtualenv venv_test
. venv_test/bin/activate
pip install pyconnect --no-cache-dir
python -m pyconnect.pyconnectsink
echo "Nice, no errors :)"
deactivate
rm -rf venv_test