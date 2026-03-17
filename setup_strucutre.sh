#!/bin/bash
# run this script from the directory where all your .py files currently are
# e.g.:  bash setup_structure.sh

set -e

echo "Creating directory structure..."
mkdir -p pipeline
mkdir -p services
mkdir -p utils
mkdir -p logs

echo "Moving pipeline files..."
mv algo_signal_check.py   pipeline/
mv client_signal_check.py pipeline/
mv order_executer.py      pipeline/
mv error_handler.py       pipeline/
mv sync.py                pipeline/

echo "Moving service files..."
mv algo_service.py        services/
mv segment_service.py     services/

echo "Moving utility files..."
mv db_helpers.py          utils/
mv schema.py              utils/
mv setup_logging.py       utils/

echo "Creating __init__.py files..."
touch __init__.py
touch pipeline/__init__.py
touch services/__init__.py
touch utils/__init__.py

echo "Done! Directory structure:"
find . -type f -name "*.py" | sort