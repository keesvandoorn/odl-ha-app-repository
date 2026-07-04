#!/usr/bin/env python3
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

APP_VERSION = "0.1.2"
OPTIONS_FILE = Path("/data/options.json")

BUFFER_DIR = Path("/data/buffer")
STATUS_DIR = Path("/data/status")
STATE_DIR = Path("/data/state")
STATUS_FILE = STATUS_DIR / "odl-ha-connector-status.json"

def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def load_options():
    if not OPTIONS_FILE.exists():
        return {}
    with OPTIONS_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)

def write_status(status):
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATUS_FILE.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    tmp.replace(STATUS_FILE)

def supervisor_logs_probe(endpoint, token):
    result = {
        "checked_at": utc_now(),
        "endpoint": endpoint,
        "token_present": bool(token),
        "reachable": False,
        "http_status": None,
        "bytes_read": 0,
        "error_type": None,
    }

    if not token:
        result["error_type"] = "missing_supervisor_token"
        return result

    try:
        req = urllib.request.Request(
            endpoint,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read(2048)
            result["http_status"] = resp.status
            result["bytes_read"] = len(data)
            result["reachable"] = 200 <= resp.status < 300
            return result

    except urllib.error.HTTPError as e:
        result["http_status"] = e.code
        result["error_type"] = "http_error"
        return result

    except Exception as e:
        result["error_type"] = type(e).__name__
        return result

def main():
    BUFFER_DIR.mkdir(parents=True, exist_ok=True)
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    options = load_options()

    collector_id = options.get("collector_id", "ha-vm")
    source_mode = options.get("source_mode", "supervisor_core_logs_follow")
    logs_endpoint = options.get("supervisor_logs_endpoint", "http://supervisor/core/logs")
    follow_endpoint = options.get("supervisor_logs_follow_endpoint", "http://supervisor/core/logs/follow")
    collector_token = options.get("collector_token", "")
    supervisor_token = os.environ.get("SUPERVISOR_TOKEN", "")

    probe = supervisor_logs_probe(logs_endpoint, supervisor_token)

    status = {
        "module": "ODL_HA_CONNECTOR",
        "version": APP_VERSION,
        "phase": "3A_repository_skeleton_supervisor_logs_only",
        "result": "OK" if probe.get("reachable") else "WARNING",
        "updated_at": utc_now(),
        "collector_id": collector_id,
        "source_mode": source_mode,
        "supervisor_token_present": bool(supervisor_token),
        "odl_collector_token_configured": bool(collector_token),
        "supervisor_logs_endpoint": logs_endpoint,
        "supervisor_logs_follow_endpoint": follow_endpoint,
        "supervisor_logs_probe": probe,
        "buffer_dir": str(BUFFER_DIR),
        "status_dir": str(STATUS_DIR),
        "state_dir": str(STATE_DIR),
        "gateway_enabled": False,
        "log_reader_enabled": False,
        "log_content_printed": False,
        "message": "Repository 3A skeleton only; Supervisor log endpoint probe only; no follow reader and no Gateway POST."
    }

    write_status(status)

    print("ODL ha-vm Connector run phase reached")
    print("ODL ha-vm Connector skeleton started")
    print(f"version={APP_VERSION}")
    print(f"collector_id={collector_id}")
    print(f"source_mode={source_mode}")
    print(f"supervisor_token_present={bool(supervisor_token)}")
    print(f"odl_collector_token_configured={bool(collector_token)}")
    print(f"supervisor_logs_endpoint_reachable={probe.get('reachable')}")
    print(f"supervisor_logs_http_status={probe.get('http_status')}")
    print("gateway_enabled=False")
    print("log_reader_enabled=False")
    print("log_content_printed=False")
    print("status_file=/data/status/odl-ha-connector-status.json")

    while True:
        time.sleep(60)
        probe = supervisor_logs_probe(logs_endpoint, supervisor_token)
        status["updated_at"] = utc_now()
        status["supervisor_logs_probe"] = probe
        status["result"] = "OK" if probe.get("reachable") else "WARNING"
        write_status(status)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("ODL ha-vm Connector skeleton stopping")
        sys.exit(0)
