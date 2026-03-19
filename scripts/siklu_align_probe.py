#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


def load_env_file() -> None:
    env_path = Path(os.environ.get("JAKE_ENV_FILE") or os.environ.get("JAKE_LOCAL_ENV_FILE") or ".env")
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe Siklu alignment telemetry over SSH/CLI.")
    parser.add_argument("--ip", required=True)
    parser.add_argument("--username", default=os.environ.get("SIKLU_USERNAME", "admin"))
    parser.add_argument("--password", default=os.environ.get("SIKLU_PASSWORD", ""))
    parser.add_argument(
        "--commands",
        default=os.environ.get("SIKLU_ALIGN_REMOTE_COMMANDS", "show alignment-status;show rf;show radio;show system"),
        help="Semicolon-separated remote commands to run",
    )
    parser.add_argument("--port", default=os.environ.get("SIKLU_SSH_PORT", "22"))
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("SIKLU_ALIGN_TIMEOUT", "2.5")))
    return parser.parse_args()


def _expect_script(ip: str, username: str, password: str, port: str, commands: list[str], timeout: float) -> str:
    remote = "\n".join(commands + ["exit"])
    escaped_remote = remote.replace("\\", "\\\\").replace('"', '\\"')
    escaped_password = password.replace("\\", "\\\\").replace('"', '\\"')
    escaped_username = username.replace("\\", "\\\\").replace('"', '\\"')
    escaped_ip = ip.replace("\\", "\\\\").replace('"', '\\"')
    escaped_port = str(port).replace("\\", "\\\\").replace('"', '\\"')
    timeout_s = max(1, int(timeout))
    return f"""
set timeout {timeout_s}
log_user 0
spawn ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p {escaped_port} {escaped_username}@{escaped_ip}
expect {{
  -re "(?i)yes/no" {{ send "yes\\r"; exp_continue }}
  -re "(?i)password:" {{ send "{escaped_password}\\r" }}
  timeout {{ puts "__ERROR__:ssh_timeout"; exit 11 }}
  eof {{ puts "__ERROR__:ssh_eof"; exit 12 }}
}}
expect {{
  -re {{[#>$] ?$}} {{}}
  timeout {{ puts "__ERROR__:prompt_timeout"; exit 13 }}
  eof {{ puts "__ERROR__:prompt_eof"; exit 14 }}
}}
send "{escaped_remote}\\r"
expect eof
puts $expect_out(buffer)
"""


def run_commands(ip: str, username: str, password: str, port: str, commands: list[str], timeout: float) -> str:
    if not password:
        raise RuntimeError("SIKLU_PASSWORD is not configured")
    proc = subprocess.run(
        ["expect", "-c", _expect_script(ip, username, password, port, commands, timeout)],
        capture_output=True,
        text=True,
        timeout=timeout + 4.0,
        check=False,
    )
    output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    if proc.returncode != 0:
        raise RuntimeError(output.strip() or f"expect exited {proc.returncode}")
    return output


RSSI_PATTERNS = [
    re.compile(r"(?i)\b(?:current\s*)?rssi\b[^-0-9]*(-?\d+(?:\.\d+)?)"),
    re.compile(r"(?i)\brx\s*rssi\b[^-0-9]*(-?\d+(?:\.\d+)?)"),
]
CINR_PATTERNS = [
    re.compile(r"(?i)\bcinr\b[^-0-9]*(-?\d+(?:\.\d+)?)"),
    re.compile(r"(?i)\bsnr\b[^-0-9]*(-?\d+(?:\.\d+)?)"),
]
MAX_RSSI_PATTERNS = [
    re.compile(r"(?i)\bmax(?:imum)?\s*(?:seen\s*)?rssi\b[^-0-9]*(-?\d+(?:\.\d+)?)"),
    re.compile(r"(?i)\bpeak\s*rssi\b[^-0-9]*(-?\d+(?:\.\d+)?)"),
]
EXPECTED_RSSI_PATTERNS = [
    re.compile(r"(?i)\bexpected\s*rssi\b[^-0-9]*(-?\d+(?:\.\d+)?)"),
    re.compile(r"(?i)\bcalculated\s*rssi\b[^-0-9]*(-?\d+(?:\.\d+)?)"),
]
ALIGNMENT_PATTERNS = [
    re.compile(r"(?i)\balignment status\b[^a-z0-9]*(.+)"),
    re.compile(r"(?i)\balignment mode\b[^a-z0-9]*(.+)"),
]


def first_number(text: str, patterns: list[re.Pattern[str]]) -> float | None:
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            try:
                return float(match.group(1))
            except (TypeError, ValueError):
                continue
    return None


def first_text(text: str, patterns: list[re.Pattern[str]]) -> str | None:
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            value = match.group(1).strip()
            if value:
                return value[:120]
    return None


def infer_alignment_mode(text: str, alignment_status: str | None) -> bool:
    hay = text.lower()
    if alignment_status and "align" in alignment_status.lower():
        return True
    return "alignment mode" in hay or "alignment status" in hay or "rf led" in hay and "orange" in hay


def main() -> int:
    load_env_file()
    args = parse_args()
    commands = [chunk.strip() for chunk in str(args.commands).split(";") if chunk.strip()]
    try:
        raw_output = run_commands(args.ip, args.username, args.password, str(args.port), commands, args.timeout)
    except Exception as exc:
        print(json.dumps({"ip": args.ip, "collector_ok": False, "collector_error": str(exc), "source": "ssh_expect"}))
        return 0

    alignment_status = first_text(raw_output, ALIGNMENT_PATTERNS)
    payload: dict[str, Any] = {
        "ip": args.ip,
        "source": "ssh_expect",
        "collector_ok": True,
        "alignment_mode": infer_alignment_mode(raw_output, alignment_status),
        "alignment_status": alignment_status,
        "current_rssi": first_number(raw_output, RSSI_PATTERNS),
        "current_cinr": first_number(raw_output, CINR_PATTERNS),
        "max_seen_rssi": first_number(raw_output, MAX_RSSI_PATTERNS),
        "expected_rssi": first_number(raw_output, EXPECTED_RSSI_PATTERNS),
        "raw_excerpt": raw_output[-4000:],
    }
    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
