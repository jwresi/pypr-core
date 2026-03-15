# Network Mapper (Read-Only)

This tool maps the `192.168.44.0/24` MikroTik network in **read-only** mode and stores snapshots in a compact SQLite DB.

## Files
- `network_mapper.py`: scanner + reporter
- `webui_server.py`: read-only WebUI/API server
- `webui/`: frontend files (dashboard, path trace, interactive map, live tools)
- `network_map.db`: SQLite database (created after first scan)
- `network_graph_latest.json`: exported node/edge map (optional)

## Read-only guarantee
The script only uses API `print/select/monitor` style reads. It does not call configuration-changing commands.

## Run

```bash
.venv/bin/python network_mapper.py scan --subnet 192.168.44.0/24
.venv/bin/python network_mapper.py report
.venv/bin/python network_mapper.py export-graph --out network_graph_latest.json
```

## WebUI

```bash
.venv/bin/python webui_server.py --host 127.0.0.1 --port 8088
```

Open `http://127.0.0.1:8088`

Pages:
- `Overview`: latest scan health and run-scan control
- `Devices`: inventory + per-device detail
- `Path Trace`: MAC chain with green/red hop arrows
- `Network Map`: interactive topology visualization + path highlighting
- `Live Tools`: on-demand read-only `mac-scan` and short sniffer probes
- `Outliers`: one-way counter outlier list

Optional size tuning:

```bash
# default keeps bridge-host entries for VLAN 20 (+ null-VID transit rows)
.venv/bin/python network_mapper.py scan --host-vid 20

# keep all VLANs in bridge-host table
.venv/bin/python network_mapper.py scan --host-vid -1
```

## What it stores
- Device identity/model/version
- Interface counters and link state
- Neighbor table (topology hints)
- Bridge VLAN/port/host learning
- Router PPP active + ARP snapshots
- Interface one-way outliers (`tx_only` / `rx_only`) based on counter deltas vs previous scan

## Database size control
- Keeps only latest `--keep-scans` snapshots (default 20)
- Uses normalized schema + indexes in a single SQLite DB
