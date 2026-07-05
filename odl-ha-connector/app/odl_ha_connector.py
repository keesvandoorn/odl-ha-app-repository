#!/usr/bin/env python3
import datetime as dt
import json
import os
import re
import signal
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

VERSION = "0.2.0"
STATUS_FILE = Path("/data/status/odl-ha-connector-status.json")
OPTIONS_FILE = Path("/data/options.json")

LOG_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} "
    r"\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+"
    r"(?P<level>[A-Z]+)\s+"
    r"\((?P<thread>[^)]*)\)\s+"
    r"\[(?P<logger>[^\]]+)\]"
)

SEVERITY_MAP = {
    "DEBUG": "debug",
    "INFO": "info",
    "WARNING": "warning",
    "WARN": "warning",
    "ERROR": "error",
    "ERR": "error",
    "CRITICAL": "critical",
    "FATAL": "critical",
    "TRACE": "debug",
}

stop_requested = False

counters = {
    "records_seen_total": 0,
    "records_parsed_total": 0,
    "records_unparsed_total": 0,
    "records_skipped_total": 0,
    "follow_connect_total": 0,
    "follow_disconnect_total": 0,
    "follow_error_total": 0,
    "local_heartbeat_total": 0,
    "status_write_total": 0,
}

last = {
    "last_record_at": None,
    "last_parsed_at": None,
    "last_unparsed_at": None,
    "last_status_write_at": None,
    "last_heartbeat_at": None,
    "last_follow_connect_at": None,
    "last_follow_disconnect_at": None,
    "last_error_at": None,
    "last_error_type": None,
    "last_http_status": None,
    "last_parse_status": None,
    "last_severity": None,
    "last_service": None,
}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def log(message: str) -> None:
    print(f"{utc_now()} | {message}", flush=True)


def load_options() -> dict:
    if not OPTIONS_FILE.exists():
        return {}
    with OPTIONS_FILE.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def normalize_severity(level: str) -> str:
    return SEVERITY_MAP.get((level or "").upper(), "notice")


def handle_stop(signum, frame) -> None:
    global stop_requested
    stop_requested = True
    log(f"stop_signal_received=True signal={signum}")


def safe_bool(value: bool) -> bool:
    return bool(value)


def write_status(options: dict, state: str = "running", result: str = "OK", extra: dict | None = None) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)

    supervisor_token_present = bool(os.environ.get("SUPERVISOR_TOKEN"))
    collector_token_configured = bool((options.get("collector_token") or "").strip())

    data = {
        "module": "ODL_HA_CONNECTOR",
        "version": VERSION,
        "state": state,
        "result": result,
        "updated_at": utc_now(),
        "collector_id": options.get("collector_id", "ha-vm"),
        "host": "homeassistant",
        "host_role": "ha-vm",
        "connector": "home-assistant",
        "source_type": "log",
        "source_mode": options.get("source_mode", "supervisor_core_logs_follow"),
        "gateway_enabled": False,
        "log_reader_enabled": True,
        "raw_log_content_printed": False,
        "supervisor_token_present": supervisor_token_present,
        "odl_collector_token_configured": collector_token_configured,
        "supervisor_logs_follow_endpoint": options.get(
            "supervisor_logs_follow_endpoint",
            "http://supervisor/core/logs/follow",
        ),
        "supervisor_logs_follow_reachable": safe_bool(last["last_http_status"] == 200),
        "supervisor_logs_follow_http_status": last["last_http_status"],
        "records_seen_total": counters["records_seen_total"],
        "records_parsed_total": counters["records_parsed_total"],
        "records_unparsed_total": counters["records_unparsed_total"],
        "records_skipped_total": counters["records_skipped_total"],
        "follow_connect_total": counters["follow_connect_total"],
        "follow_disconnect_total": counters["follow_disconnect_total"],
        "follow_error_total": counters["follow_error_total"],
        "local_heartbeat_total": counters["local_heartbeat_total"],
        "status_write_total": counters["status_write_total"],
        **last,
    }

    if extra:
        data.update(extra)

    tmp = STATUS_FILE.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
    tmp.replace(STATUS_FILE)

    counters["status_write_total"] += 1
    last["last_status_write_at"] = utc_now()


def parse_line(line: str) -> None:
    counters["records_seen_total"] += 1
    last["last_record_at"] = utc_now()

    # Supervisor kan eventueel stream-prefixes gebruiken. Die worden niet gelogd.
    if line.startswith("data: "):
        line = line[6:]

    if not line.strip():
        counters["records_skipped_total"] += 1
        return

    match = LOG_RE.match(line)
    if match:
        counters["records_parsed_total"] += 1
        last["last_parsed_at"] = utc_now()
        last["last_parse_status"] = "parsed"
        last["last_severity"] = normalize_severity(match.group("level"))
        last["last_service"] = match.group("logger")
    else:
        counters["records_unparsed_total"] += 1
        last["last_unparsed_at"] = utc_now()
        last["last_parse_status"] = "unparsed"
        last["last_severity"] = "notice"
        last["last_service"] = None


def local_heartbeat(options: dict) -> None:
    counters["local_heartbeat_total"] += 1
    last["last_heartbeat_at"] = utc_now()
    write_status(options, state="running", result="OK")


def follow_logs(options: dict) -> None:
    endpoint = options.get("supervisor_logs_follow_endpoint", "http://supervisor/core/logs/follow")
    token = os.environ.get("SUPERVISOR_TOKEN", "")

    if not token:
        last["last_error_at"] = utc_now()
        last["last_error_type"] = "missing_supervisor_token"
        write_status(options, state="error", result="ERROR")
        log("version=0.2.0 supervisor_token_present=False log_reader_enabled=False raw_log_content_printed=False")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "text/plain",
    }

    heartbeat_interval = int(options.get("heartbeat_interval_seconds", 60))
    next_heartbeat = time.time()
    next_summary = time.time() + 30
    next_status = time.time() + 5

    log("version=0.2.0")
    log(f"collector_id={options.get('collector_id', 'ha-vm')}")
    log("source_mode=supervisor_core_logs_follow")
    log("gateway_enabled=False")
    log("log_reader_enabled=True")
    log("raw_log_content_printed=False")
    log("supervisor_token_present=True")
    log(f"odl_collector_token_configured={bool((options.get('collector_token') or '').strip())}")
    log(f"status_file={STATUS_FILE}")

    write_status(options, state="starting", result="OK")

    while not stop_requested:
        try:
            req = urllib.request.Request(endpoint, headers=headers, method="GET")

            counters["follow_connect_total"] += 1
            last["last_follow_connect_at"] = utc_now()

            with urllib.request.urlopen(req, timeout=60) as response:
                last["last_http_status"] = int(response.getcode() or 0)
                write_status(options, state="running", result="OK")
                log(f"supervisor_logs_follow_reachable=True supervisor_logs_follow_http_status={last['last_http_status']}")

                while not stop_requested:
                    now = time.time()

                    if now >= next_heartbeat:
                        local_heartbeat(options)
                        next_heartbeat = now + heartbeat_interval

                    raw = response.readline()
                    if not raw:
                        counters["follow_disconnect_total"] += 1
                        last["last_follow_disconnect_at"] = utc_now()
                        write_status(options, state="reconnecting", result="WARNING")
                        break

                    # De inhoud wordt alleen intern verwerkt en nooit geprint.
                    line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                    parse_line(line)

                    now = time.time()
                    if now >= next_status:
                        write_status(options, state="running", result="OK")
                        next_status = now + 5

                    if now >= next_summary:
                        log(
                            "records_seen_total={seen} records_parsed_total={parsed} "
                            "records_unparsed_total={unparsed} last_record_at={last_record}"
                            .format(
                                seen=counters["records_seen_total"],
                                parsed=counters["records_parsed_total"],
                                unparsed=counters["records_unparsed_total"],
                                last_record=last["last_record_at"],
                            )
                        )
                        next_summary = now + 30

        except urllib.error.HTTPError as exc:
            counters["follow_error_total"] += 1
            last["last_error_at"] = utc_now()
            last["last_error_type"] = "HTTPError"
            last["last_http_status"] = int(exc.code)
            write_status(options, state="reconnecting", result="ERROR")
            log(f"supervisor_logs_follow_reachable=False supervisor_logs_follow_http_status={exc.code}")
            time.sleep(5)

        except Exception as exc:
            counters["follow_error_total"] += 1
            last["last_error_at"] = utc_now()
            last["last_error_type"] = exc.__class__.__name__
            write_status(options, state="reconnecting", result="WARNING")
            log(f"supervisor_logs_follow_reachable=False error_type={exc.__class__.__name__}")
            time.sleep(5)

    write_status(options, state="stopped", result="OK", extra={"graceful_stop": True})
    log("connector_stopped=True graceful_stop=True")


def main() -> int:
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    Path("/data/status").mkdir(parents=True, exist_ok=True)
    Path("/data/state").mkdir(parents=True, exist_ok=True)
    Path("/data/buffer").mkdir(parents=True, exist_ok=True)

    options = load_options()
    follow_logs(options)
    return 0


if __name__ == "__main__":
    sys.exit(main())
