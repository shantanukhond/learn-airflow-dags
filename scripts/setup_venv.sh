#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if command -v python3.11 >/dev/null 2>&1; then
  PYTHON=python3.11
elif command -v python3.10 >/dev/null 2>&1; then
  PYTHON=python3.10
else
  PYTHON=python3
fi

echo "Using $PYTHON ($($PYTHON --version))"

$PYTHON -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements-dev.txt

echo ""
echo "Done. Activate with:"
echo "  source .venv/bin/activate"
