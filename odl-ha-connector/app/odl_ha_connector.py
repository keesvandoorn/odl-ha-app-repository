#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import signal
import sys
import time
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

VERSION = "0.3.1"
STATUS_FILE = Path("/data/status/odl-ha-connector-status.json")
OPTIONS_FILE = Path("/data/options.json")

DEFAULT_OPTIONS = {
    "gateway_url": "http://192.168.178.100:18080/odl/v1/ingest",
    "collector_id": "ha-vm",
    "collector_token": "",
    "source_mode": "supervisor_core_logs_follow",
    "supervisor_logs_endpoint": "http://supervisor/core/logs",
    "supervisor_logs_follow_endpoint": "http://supervisor/core/logs/follow",
    "heartbeat_interval_seconds": 60,
    "batch_size": 50,
    "max_buffer_records": 10000,
    "log_level": "info",
}

SEVERITY_MAP = {
    "TRACE": "debug",
    "DEBUG": "debug",
    "INFO": "info",
    "NOTICE": "notice",
    "WARNING": "warning",
    "WARN": "warning",
    "ERROR": "error",
    "ERR": "error",
    "CRITICAL": "critical",
    "FATAL": "critical",
}

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

TEXT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "ha_ts_level_thread_logger",
        re.compile(
            r"^(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)"
            r"\s+(?P<level>TRACE|DEBUG|INFO|NOTICE|WARNING|WARN|ERROR|ERR|CRITICAL|FATAL)"
            r"\s+\((?P<thread>[^)]*)\)\s+\[(?P<logger>[^\]]+)\]\s*(?P<message>.*)$",
            re.IGNORECASE,
        ),
    ),
    (
        "ha_ts_level_logger",
        re.compile(
            r"^(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)"
            r"\s+(?P<level>TRACE|DEBUG|INFO|NOTICE|WARNING|WARN|ERROR|ERR|CRITICAL|FATAL)"
            r"\s+\[(?P<logger>[^\]]+)\]\s*(?P<message>.*)$",
            re.IGNORECASE,
        ),
    ),
    (
        "ha_iso_level_thread_logger",
        re.compile(
            r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)"
            r"\s+(?P<level>TRACE|DEBUG|INFO|NOTICE|WARNING|WARN|ERROR|ERR|CRITICAL|FATAL)"
            r"\s+\((?P<thread>[^)]*)\)\s+\[(?P<logger>[^\]]+)\]\s*(?P<message>.*)$",
            re.IGNORECASE,
        ),
    ),
    (
        "ha_ts_level_service_colon",
        re.compile(
            r"^(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)"
            r"\s+(?P<level>TRACE|DEBUG|INFO|NOTICE|WARNING|WARN|ERROR|ERR|CRITICAL|FATAL)"
            r"\s+(?P<logger>[A-Za-z0-9_.-]+)\s*:\s*(?P<message>.*)$",
            re.IGNORECASE,
        ),
    ),
    (
        "level_thread_logger_no_ts",
        re.compile(
            r"^(?P<level>TRACE|DEBUG|INFO|NOTICE|WARNING|WARN|ERROR|ERR|CRITICAL|FATAL)"
            r"\s+\((?P<thread>[^)]*)\)\s+\[(?P<logger>[^\]]+)\]\s*(?P<message>.*)$",
            re.IGNORECASE,
        ),
    ),
]

counters: dict[str, int] = {
    "records_seen_total": 0,
    "records_parsed_total": 0,
    "records_unparsed_total": 0,
    "records_skipped_total": 0,
    "records_normalized_total": 0,
    "json_lines_total": 0,
    "text_lines_total": 0,
    "sse_data_lines_total": 0,
    "sse_control_lines_total": 0,
    "follow_connect_total": 0,
    "follow_disconnect_total": 0,
    "follow_error_total": 0,
    "heartbeat_total": 0,
    "status_write_total": 0,
    "initial_snapshot_fetch_total": 0,
    "initial_snapshot_lines_total": 0,
    "initial_snapshot_error_total": 0,
}

last: dict[str, Any] = {
    "last_record_at": None,
    "last_parsed_at": None,
    "last_unparsed_at": None,
    "last_status_write_at": None,
    "last_heartbeat_at": None,
    "last_follow_connect_at": None,
    "last_follow_disconnect_at": None,
    "last_http_status": None,
    "last_snapshot_http_status": None,
    "last_snapshot_at": None,
    "last_snapshot_line_count": None,
    "last_error_type": None,
    "last_error_message": None,
    "last_parse_status": None,
    "last_parser_pattern": None,
    "last_service": None,
    "last_severity": None,
    "last_event_type": None,
    "last_message_length": None,
    "last_odl_record_summary": None,
}

stop_requested = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def log(message: str) -> None:
    print(f"{utc_now()} | {message}", flush=True)


def safe_bool(value: Any) -> bool:
    return bool(value)


def load_options() -> dict:
    data = DEFAULT_OPTIONS.copy()
    if OPTIONS_FILE.exists():
        try:
            loaded = json.loads(OPTIONS_FILE.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                data.update(loaded)
        except Exception as exc:
            last["last_error_type"] = "options_read_error"
            last["last_error_message"] = exc.__class__.__name__
    return data


def normalize_severity(value: str | None) -> str:
    if not value:
        return "notice"
    return SEVERITY_MAP.get(str(value).upper(), "notice")


def normalize_timestamp(value: str | None) -> str:
    if not value:
        return utc_now()

    raw = str(value).strip().replace(",", ".")
    try:
        if raw.endswith("Z"):
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        elif re.search(r"[+-]\d{2}:?\d{2}$", raw):
            if re.search(r"[+-]\d{4}$", raw):
                raw = raw[:-5] + raw[-5:-2] + ":" + raw[-2:]
            dt = datetime.fromisoformat(raw)
        else:
            if "T" in raw:
                dt = datetime.fromisoformat(raw)
            else:
                dt = datetime.fromisoformat(raw.replace(" ", "T"))

            if dt.tzinfo is None:
                local_tz = None
                if ZoneInfo is not None:
                    try:
                        local_tz = ZoneInfo("Europe/Amsterdam")
                    except Exception:
                        local_tz = None
                if local_tz is None:
                    local_tz = datetime.now().astimezone().tzinfo
                dt = dt.replace(tzinfo=local_tz)

        return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except Exception:
        return utc_now()


def strip_transport_prefix(line: str) -> tuple[str, str]:
    stripped = line.strip("\r\n")
    if not stripped:
        return "", "empty"

    if stripped.startswith(":"):
        counters["sse_control_lines_total"] += 1
        return "", "sse_control"

    if stripped.startswith("event:"):
        counters["sse_control_lines_total"] += 1
        return "", "sse_event"

    if stripped.startswith("id:"):
        counters["sse_control_lines_total"] += 1
        return "", "sse_id"

    if stripped.startswith("retry:"):
        counters["sse_control_lines_total"] += 1
        return "", "sse_retry"

    if stripped.startswith("data:"):
        counters["sse_data_lines_total"] += 1
        return stripped[5:].strip(), "sse_data"

    return stripped, "plain"


def sanitize_line_for_parsing(line: str) -> str:
    return ANSI_RE.sub("", line).strip()


def record_summary(record: dict[str, Any]) -> dict[str, Any]:
    metadata = record.get("metadata", {})
    message = record.get("message", "")
    return {
        "odl_version": record.get("odl_version"),
        "collector_id": record.get("collector_id"),
        "host": record.get("host"),
        "host_role": record.get("host_role"),
        "connector": record.get("connector"),
        "source_type": record.get("source_type"),
        "service": record.get("service"),
        "severity": record.get("severity"),
        "event_type": record.get("event_type"),
        "parse_status": metadata.get("parse_status"),
        "parser_pattern": metadata.get("parser_pattern"),
        "metadata_keys": sorted(metadata.keys()),
        "message_present": bool(message),
        "message_length": len(message),
        "clock_skew_seconds_present": "clock_skew_seconds" in record,
        "raw_log_content_in_status": False,
    }


def build_record(
    *,
    timestamp: str,
    service: str,
    severity: str,
    event_type: str,
    message: str,
    parse_status: str,
    parser_pattern: str,
    thread: str | None = None,
    transport: str = "plain",
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "parse_status": parse_status,
        "parser_pattern": parser_pattern,
        "source_mode": "supervisor_core_logs_follow",
        "transport": transport,
        "raw_line_stored": False,
    }
    if thread:
        metadata["thread"] = thread

    return {
        "odl_version": "1.0",
        "timestamp": timestamp,
        "collector_id": "ha-vm",
        "host": "homeassistant",
        "host_role": "ha-vm",
        "connector": "home-assistant",
        "source_type": "log",
        "service": service or "homeassistant",
        "severity": severity,
        "event_type": event_type,
        "message": message,
        "metadata": metadata,
    }


def parse_json_payload(payload: dict[str, Any], transport: str) -> dict[str, Any] | None:
    message = (
        payload.get("message")
        or payload.get("msg")
        or payload.get("log")
        or payload.get("record")
        or payload.get("line")
        or ""
    )
    level = payload.get("level") or payload.get("severity") or payload.get("levelname")
    logger_name = payload.get("logger") or payload.get("name") or payload.get("source") or "homeassistant"
    ts = payload.get("time") or payload.get("timestamp") or payload.get("created") or payload.get("asctime")
    thread = payload.get("thread") or payload.get("threadName")

    if not message and not level and not ts:
        return None

    return build_record(
        timestamp=normalize_timestamp(str(ts) if ts else None),
        service=str(logger_name or "homeassistant"),
        severity=normalize_severity(str(level) if level else None),
        event_type="log_event",
        message=str(message) if message else "Home Assistant JSON log event",
        parse_status="parsed",
        parser_pattern="json_payload",
        thread=str(thread) if thread else None,
        transport=transport,
    )


def parse_text_payload(text: str, transport: str) -> dict[str, Any] | None:
    clean = sanitize_line_for_parsing(text)
    for pattern_name, pattern in TEXT_PATTERNS:
        match = pattern.match(clean)
        if not match:
            continue

        data = match.groupdict()
        return build_record(
            timestamp=normalize_timestamp(data.get("timestamp")),
            service=data.get("logger") or "homeassistant",
            severity=normalize_severity(data.get("level")),
            event_type="log_event",
            message=data.get("message") or "Home Assistant log event",
            parse_status="parsed",
            parser_pattern=pattern_name,
            thread=data.get("thread"),
            transport=transport,
        )

    return None


def parse_line(line: str) -> None:
    counters["records_seen_total"] += 1
    last["last_record_at"] = utc_now()

    payload, transport = strip_transport_prefix(line)
    if not payload:
        counters["records_skipped_total"] += 1
        last["last_parse_status"] = "skipped"
        last["last_parser_pattern"] = transport
        return

    record: dict[str, Any] | None = None

    if payload.startswith("{") and payload.endswith("}"):
        counters["json_lines_total"] += 1
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, dict):
                record = parse_json_payload(parsed, transport)
        except Exception:
            record = None
    else:
        counters["text_lines_total"] += 1

    if record is None:
        record = parse_text_payload(payload, transport)

    if record is None:
        counters["records_unparsed_total"] += 1
        last["last_unparsed_at"] = utc_now()
        last["last_parse_status"] = "unparsed"
        last["last_parser_pattern"] = "unmatched"
        last["last_service"] = "homeassistant"
        last["last_severity"] = "notice"
        last["last_event_type"] = "log_event_unparsed"
        last["last_message_length"] = len(payload)
        last["last_odl_record_summary"] = record_summary(
            build_record(
                timestamp=utc_now(),
                service="homeassistant",
                severity="notice",
                event_type="log_event_unparsed",
                message="Unparsed Home Assistant log line",
                parse_status="unparsed",
                parser_pattern="unmatched",
                transport=transport,
            )
        )
        return

    counters["records_parsed_total"] += 1
    counters["records_normalized_total"] += 1
    last["last_parsed_at"] = utc_now()
    last["last_parse_status"] = "parsed"
    last["last_parser_pattern"] = record["metadata"].get("parser_pattern")
    last["last_service"] = record.get("service")
    last["last_severity"] = record.get("severity")
    last["last_event_type"] = record.get("event_type")
    last["last_message_length"] = len(record.get("message", ""))
    last["last_odl_record_summary"] = record_summary(record)


def write_status(options: dict, state: str = "running", result: str = "OK", extra: dict | None = None) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)

    supervisor_token_present = bool(os.environ.get("SUPERVISOR_TOKEN"))
    collector_token_configured = bool(str(options.get("collector_token", "")).strip())

    status = {
        "version": VERSION,
        "updated_at": utc_now(),
        "state": state,
        "result": result,
        "collector_id": options.get("collector_id", "ha-vm"),
        "source_mode": options.get("source_mode", "supervisor_core_logs_follow"),
        "gateway_enabled": False,
        "log_reader_enabled": True,
        "raw_log_content_printed": False,
        "raw_log_content_in_status": False,
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
        "records_normalized_total": counters["records_normalized_total"],
        "json_lines_total": counters["json_lines_total"],
        "text_lines_total": counters["text_lines_total"],
        "sse_data_lines_total": counters["sse_data_lines_total"],
        "sse_control_lines_total": counters["sse_control_lines_total"],
        "follow_connect_total": counters["follow_connect_total"],
        "follow_disconnect_total": counters["follow_disconnect_total"],
        "follow_error_total": counters["follow_error_total"],
        "heartbeat_total": counters["heartbeat_total"],
        "status_write_total": counters["status_write_total"],
        "initial_snapshot_fetch_total": counters["initial_snapshot_fetch_total"],
        "initial_snapshot_lines_total": counters["initial_snapshot_lines_total"],
        "initial_snapshot_error_total": counters["initial_snapshot_error_total"],
        "supervisor_logs_snapshot_http_status": last["last_snapshot_http_status"],
        "last": last,
    }

    if extra:
        status["extra"] = extra

    tmp = STATUS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(STATUS_FILE)

    counters["status_write_total"] += 1
    last["last_status_write_at"] = utc_now()


def heartbeat(options: dict) -> None:
    counters["heartbeat_total"] += 1
    last["last_heartbeat_at"] = utc_now()
    write_status(options, state="running", result="OK")
    log(
        "heartbeat "
        f"records_seen_total={counters['records_seen_total']} "
        f"records_parsed_total={counters['records_parsed_total']} "
        f"records_unparsed_total={counters['records_unparsed_total']} "
        f"records_normalized_total={counters['records_normalized_total']} "
        f"last_parser_pattern={last['last_parser_pattern']}"
    )



def log_startup(options: dict) -> None:
    log(f"version={VERSION}")
    log(f"collector_id={options.get('collector_id', 'ha-vm')}")
    log("source_mode=supervisor_core_logs_follow")
    log("gateway_enabled=False")
    log("log_reader_enabled=True")
    log("raw_log_content_printed=False")
    log("raw_log_content_in_status=False")
    log(f"supervisor_token_present={bool(os.environ.get('SUPERVISOR_TOKEN'))}")
    log(f"odl_collector_token_configured={bool(str(options.get('collector_token', '')).strip())}")
    log(f"status_file={STATUS_FILE}")


def iter_snapshot_payloads(body: str):
    stripped = body.strip()
    if not stripped:
        return

    if stripped.startswith("[") or stripped.startswith("{"):
        try:
            parsed = json.loads(stripped)
        except Exception:
            parsed = None

        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict):
                    yield json.dumps(item, ensure_ascii=False)
                else:
                    yield str(item)
            return

        if isinstance(parsed, dict):
            yield json.dumps(parsed, ensure_ascii=False)
            return

    for line in body.splitlines():
        if line.strip():
            yield line


def fetch_initial_snapshot(options: dict) -> None:
    endpoint = options.get("supervisor_logs_endpoint", "http://supervisor/core/logs")
    token = os.environ.get("SUPERVISOR_TOKEN")

    counters["initial_snapshot_fetch_total"] += 1
    last["last_snapshot_at"] = utc_now()

    if not token:
        counters["initial_snapshot_error_total"] += 1
        last["last_error_type"] = "missing_supervisor_token_snapshot"
        write_status(options, state="error", result="ERROR")
        log("initial_snapshot_reachable=False reason=missing_supervisor_token")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "text/plain, application/json",
        "User-Agent": "odl-ha-vm-connector/0.3.1",
    }

    try:
        req = urllib.request.Request(endpoint, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=20) as response:
            last["last_snapshot_http_status"] = int(response.getcode() or 0)
            body = response.read().decode("utf-8", errors="replace")

        line_count = 0
        for payload in iter_snapshot_payloads(body):
            line_count += 1
            parse_line(payload)

        counters["initial_snapshot_lines_total"] += line_count
        last["last_snapshot_line_count"] = line_count

        write_status(options, state="running", result="OK")
        log(
            "initial_snapshot_reachable=True "
            f"initial_snapshot_http_status={last['last_snapshot_http_status']} "
            f"initial_snapshot_lines_total={line_count} "
            f"records_seen_total={counters['records_seen_total']} "
            f"records_parsed_total={counters['records_parsed_total']} "
            f"records_unparsed_total={counters['records_unparsed_total']} "
            f"records_normalized_total={counters['records_normalized_total']} "
            f"last_parser_pattern={last['last_parser_pattern']}"
        )

    except Exception as exc:
        counters["initial_snapshot_error_total"] += 1
        last["last_error_type"] = exc.__class__.__name__
        last["last_error_message"] = exc.__class__.__name__
        write_status(options, state="warning", result="WARNING")
        log(f"initial_snapshot_reachable=False initial_snapshot_error_type={exc.__class__.__name__}")

def handle_signal(signum: int, _frame: Any) -> None:
    global stop_requested
    stop_requested = True
    log(f"stop_signal_received=True signal={signum}")


def follow_logs(options: dict) -> None:
    endpoint = options.get("supervisor_logs_follow_endpoint", "http://supervisor/core/logs/follow")
    token = os.environ.get("SUPERVISOR_TOKEN")
    heartbeat_interval = int(options.get("heartbeat_interval_seconds", 60))

    if not token:
        last["last_error_type"] = "missing_supervisor_token"
        write_status(options, state="error", result="ERROR")
        log("version=0.3.1 supervisor_token_present=False")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "text/plain, text/event-stream, application/json",
        "User-Agent": "odl-ha-vm-connector/0.3.1",
    }

    log("version=0.3.1")
    log(f"collector_id={options.get('collector_id', 'ha-vm')}")
    log("source_mode=supervisor_core_logs_follow")
    log("gateway_enabled=False")
    log("log_reader_enabled=True")
    log("raw_log_content_printed=False")
    log("raw_log_content_in_status=False")
    log("supervisor_token_present=True")
    log(f"odl_collector_token_configured={bool(str(options.get('collector_token', '')).strip())}")
    log(f"status_file={STATUS_FILE}")

    write_status(options, state="starting", result="OK")

    next_heartbeat = time.time() + 5

    while not stop_requested:
        try:
            req = urllib.request.Request(endpoint, headers=headers, method="GET")
            counters["follow_connect_total"] += 1
            last["last_follow_connect_at"] = utc_now()

            with urllib.request.urlopen(req, timeout=30) as response:
                last["last_http_status"] = int(response.getcode() or 0)
                write_status(options, state="running", result="OK")
                log(f"supervisor_logs_follow_reachable=True supervisor_logs_follow_http_status={last['last_http_status']}")

                for raw in response:
                    if stop_requested:
                        break

                    try:
                        line = raw.decode("utf-8", errors="replace")
                    except Exception:
                        line = ""

                    parse_line(line)

                    now = time.time()
                    if now >= next_heartbeat:
                        heartbeat(options)
                        next_heartbeat = now + heartbeat_interval

                counters["follow_disconnect_total"] += 1
                last["last_follow_disconnect_at"] = utc_now()
                write_status(options, state="reconnecting", result="WARNING")

        except urllib.error.HTTPError as exc:
            counters["follow_error_total"] += 1
            last["last_http_status"] = int(exc.code)
            last["last_error_type"] = "http_error"
            last["last_error_message"] = f"HTTP {exc.code}"
            write_status(options, state="error", result="ERROR")
            log(f"supervisor_logs_follow_reachable=False supervisor_logs_follow_http_status={exc.code}")
            time.sleep(10)

        except Exception as exc:
            counters["follow_error_total"] += 1
            last["last_error_type"] = exc.__class__.__name__
            last["last_error_message"] = exc.__class__.__name__
            write_status(options, state="error", result="ERROR")
            log(f"follow_error_type={exc.__class__.__name__}")
            time.sleep(10)

    write_status(options, state="stopping", result="OK")
    log("connector_stopping=True")


def main() -> int:
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    options = load_options()
    heartbeat_interval = int(options.get("heartbeat_interval_seconds", 60))

    try:
        log_startup(options)
        write_status(options, state="starting", result="OK")

        fetch_initial_snapshot(options)

        reader = threading.Thread(target=follow_logs, args=(options,), daemon=True)
        reader.start()

        next_heartbeat = time.time() + 5
        while not stop_requested:
            now = time.time()
            if now >= next_heartbeat:
                heartbeat(options)
                next_heartbeat = now + heartbeat_interval
            time.sleep(1)

        write_status(options, state="stopping", result="OK")
        log("connector_stopping=True")

    finally:
        write_status(options, state="stopped", result="OK", extra={"graceful_stop": True})
        log("connector_stopped=True graceful_stop=True")

    return 0


if __name__ == "__main__":
    sys.exit(main())
