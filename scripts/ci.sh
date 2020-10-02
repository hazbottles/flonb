#!/bin/bash

set -ex

if [ -d "./testenv" ]; then
  rm -rf ./testenv
fi
python -m venv testenv  # NB what if testenv already exists?
source ./testenv/bin/activate

pip install .
bash ./scripts/install_test_requirements.sh
bash ./scripts/run_linters.sh
bash ./scripts/run_tests.sh
