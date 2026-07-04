#!/usr/bin/env bash
set -euo pipefail

echo "ODL ha-vm Connector run.sh reached"
exec python3 -u /app/odl_ha_connector.py
