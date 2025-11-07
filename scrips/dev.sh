#!/usr/bin/env bash
set -e

# Activate virtual environment if present
if [ -d ".venv" ]; then
  source .venv/bin/activate
fi

echo "🚀 Running Textual app..."
python -m textual run src/bb-beam/main.py

