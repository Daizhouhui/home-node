#!/usr/bin/env python3
"""Generate a public list of TCP SSTP endpoints from VPN Gate."""

import base64
import csv
import json
import os
import re
import sys
from urllib.error import URLError
from urllib.request import Request, urlopen

REPO_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.environ.get(
    "SSTP_NODES_FILE",
    os.path.join(REPO_DIR, "generated", "sstp-nodes.txt"),
)
VPNGATE_API = os.environ.get(
    "VPNGATE_API",
    "https://www.vpngate.net/api/iphone/",
)
VPNGATE_MIRROR = os.environ.get(
    "VPNGATE_MIRROR",
    "https://raw.githubusercontent.com/fdciabdul/Vpngate-Scraper-API/main/json/data.json",
)
HTTP_TIMEOUT = max(5, int(os.environ.get("HTTP_TIMEOUT", "30")))
MAX_NODES = max(1, int(os.environ.get("MAX_NODES", "500")))

TCP_PROTO_RE = re.compile(
    r"^\s*proto\s+tcp(?:[46])?(?:-client)?\b",
    re.IGNORECASE | re.MULTILINE,
)
REMOTE_RE = re.compile(
    r"^\s*remote\s+\S+\s+(\d+)\b",
    re.IGNORECASE | re.MULTILINE,
)
HOST_RE = re.compile(
    r"^(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}$"
)


def fetch_text(url):
    request = Request(url, headers={"User-Agent": "vpngate-sstp-list/1.0"})
    with urlopen(request, timeout=HTTP_TIMEOUT) as response:
        return response.read().decode("utf-8", "replace")


def parse_csv(text):
    lines = [line for line in text.splitlines() if line.strip()]
    header_index = next(
        (
            index
            for index, line in enumerate(lines)
            if line.lstrip("#\ufeff").startswith("HostName")
        ),
        None,
    )
    if header_index is None:
        raise ValueError("VPN Gate CSV header (HostName) was not found")

    header = next(csv.reader([lines[header_index].lstrip("#\ufeff")]))
    positions = {}
    for index, column in enumerate(header):
        key = column.strip().lstrip("*").lower()
        positions[key] = index

    required = ("hostname", "ip", "countrylong", "countryshort")
    missing = [key for key in required if key not in positions]
    config_index = positions.get("openvpn_configdata_base64")
    if config_index is None:
        config_index = next(
            (i for i, column in enumerate(header) if "base64" in column.lower()),
            None,
        )
    if missing or config_index is None:
        raise ValueError("VPN Gate CSV is missing required columns")

    rows = []
    for line in lines[header_index + 1 :]:
        fields = next(csv.reader([line]))
        required_indexes = [positions[key] for key in required] + [config_index]
        if len(fields) <= max(required_indexes):
            continue
        rows.append(
            {
                "host": fields[positions["hostname"]].strip(),
                "ip": fields[positions["ip"]].strip(),
                "country": fields[positions["countrylong"]].strip(),
                "country_code": fields[positions["countryshort"]].strip(),
                "config_b64": fields[config_index].strip(),
            }
        )
    return rows


def parse_mirror_json(data):
    items = data if isinstance(data, list) else [data]
    servers = []
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("servers"), list):
            servers.extend(item["servers"])
        elif isinstance(item, dict):
            servers.append(item)

    rows = []
    for server in servers:
        rows.append(
            {
                "host": str(server.get("hostname") or server.get("host") or "").strip(),
                "ip": str(server.get("ip") or "").strip(),
                "country": str(
                    server.get("countrylong")
                    or server.get("country_long")
                    or server.get("country")
                    or ""
                ).strip(),
                "country_code": str(
                    server.get("countryshort") or server.get("country_short") or ""
                ).strip(),
                "config_b64": str(
                    server.get("openvpn_configdata_base64")
                    or server.get("config_b64")
                    or ""
                ).strip(),
            }
        )
    return rows


def fetch_vpngate():
    try:
        rows = parse_csv(fetch_text(VPNGATE_API))
        if rows:
            print(f"VPN Gate API returned {len(rows)} nodes")
            return rows, "official-api"
        raise ValueError("VPN Gate API returned no nodes")
    except (OSError, URLError, ValueError, csv.Error) as error:
        print(f"VPN Gate API failed; trying mirror: {error}", file=sys.stderr)

    try:
        rows = parse_mirror_json(json.loads(fetch_text(VPNGATE_MIRROR)))
        if rows:
            print(f"VPN Gate mirror returned {len(rows)} nodes")
            return rows, "github-mirror"
    except (OSError, URLError, ValueError, json.JSONDecodeError) as error:
        print(f"VPN Gate mirror failed: {error}", file=sys.stderr)

    raise RuntimeError("VPN Gate API and mirror both failed")


def to_sstp_nodes(rows):
    nodes = []
    for row in rows:
        encoded_config = row.get("config_b64", "")
        try:
            config = base64.b64decode(encoded_config).decode("utf-8", "replace")
        except (ValueError, TypeError):
            continue

        if not TCP_PROTO_RE.search(config):
            continue
        match = REMOTE_RE.search(config)
        if not match:
            continue

        port = int(match.group(1))
        if not 1 <= port <= 65535:
            continue

        host = row.get("host", "").strip().rstrip(".")
        if not host:
            continue
        if not host.lower().endswith(".opengw.net"):
            host = f"{host}.opengw.net"
        if not HOST_RE.fullmatch(host):
            continue
        nodes.append(f"{host}:{port}")
    return nodes


def write_nodes(nodes):
    unique_nodes = list(dict.fromkeys(nodes))[:MAX_NODES]
    if not unique_nodes:
        raise RuntimeError("No TCP SSTP nodes were found; refusing to overwrite the previous list")

    output_dir = os.path.dirname(OUTPUT_FILE)
    os.makedirs(output_dir, exist_ok=True)
    temporary_file = f"{OUTPUT_FILE}.tmp"
    with open(temporary_file, "w", encoding="utf-8", newline="\n") as output:
        output.write("\n".join(unique_nodes) + "\n")
    os.replace(temporary_file, OUTPUT_FILE)
    return unique_nodes


def main():
    rows, source = fetch_vpngate()
    nodes = to_sstp_nodes(rows)
    if not nodes:
        raise RuntimeError("VPN Gate data contained no TCP SSTP endpoints")

    result = write_nodes(nodes)
    print(f"Source: {source}")
    print(f"Generated {len(result)} unique endpoints: {os.path.relpath(OUTPUT_FILE, REPO_DIR)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Generation failed: {type(error).__name__}: {error}", file=sys.stderr)
        raise SystemExit(1) from error
