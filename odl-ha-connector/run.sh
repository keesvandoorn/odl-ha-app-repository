#!/usr/bin/with-contenv bash
set -euo pipefail

echo "ODL ha-vm Connector run.sh reached"
echo "ODL ha-vm Connector environment bridge active"
exec python3 -u /app/odl_ha_connector.py
