#!/usr/bin/env bash
# Start the API in development mode with auto-reload
set -euo pipefail

cd "$(dirname "$0")/.."

uvicorn candata_api.app:create_app --factory --reload --port 8000
