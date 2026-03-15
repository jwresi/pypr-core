# Jake API

Deterministic local API for common network-operations queries.

## Start

```bash
python3 scripts/jake_api_server.py --host 127.0.0.1 --port 8787
```

Compatibility wrapper:

```bash
python3 jake_api_server.py --host 127.0.0.1 --port 8787
```

## Endpoints

- `GET /health`
- `GET /api/server-info`
- `GET /api/subnet-health?subnet=192.168.44.0/24`
- `GET /api/online-customers?scope=000007`
- `GET /api/site-summary?site_id=000007`
- `GET /api/building-health?building_id=000007.055`
- `GET /api/building-customer-count?building_id=000007.055`
- `GET /api/building-flap-history?building_id=000007.055`
- `GET /api/rogue-dhcp-suspects?building_id=000007.055`
- `GET /api/recovery-ready-cpes?building_id=000007.055`
- `GET /api/trace-mac?mac=30:68:93:C1:C8:34`
- `GET /api/cpe-state?mac=30:68:93:C1:C8:34`
- `GET /api/find-cpe-candidates?site_id=000007&building_id=000007.055&oui=30:68:93&access_only=true&limit=20`
- `GET /api/site-alerts?site_id=000007`
- `GET /api/netbox-device?name=000007.055.SW04`
- `GET /api/tauc/network-name-list?status=ONLINE`
- `GET /api/tauc/network-details?network_id=...`
- `GET /api/tauc/preconfiguration-status?network_id=...`
- `GET /api/tauc/pppoe-status?network_id=...`
- `GET /api/tauc/device-id?sn=...&mac=...`
- `GET /api/tauc/device-detail?device_id=...`
- `GET /api/tauc/device-internet?device_id=...`
- `GET /api/tauc/olt-devices`
- `GET /api/vilo/server-info`
- `GET /api/vilo/inventory?page_index=1&page_size=20`
- `GET /api/vilo/audit?site_id=000007` or `GET /api/vilo/audit?building_id=000007.055`
- `POST /api/vilo/inventory-search`
- `GET /api/vilo/subscribers?page_index=1&page_size=20`
- `POST /api/vilo/subscribers-search`
- `GET /api/vilo/networks?page_index=1&page_size=20`
- `POST /api/vilo/networks-search`
- `GET /api/vilo/devices?network_id=...`
- `POST /api/vilo/devices-search`

## Verified Examples

```bash
curl -s 'http://127.0.0.1:8787/api/site-summary?site_id=000007' | jq
curl -s 'http://127.0.0.1:8787/api/building-health?building_id=000007.055' | jq
curl -s 'http://127.0.0.1:8787/api/online-customers?scope=000007' | jq
curl -s 'http://127.0.0.1:8787/api/trace-mac?mac=30:68:93:C1:C8:34' | jq
curl -s 'http://127.0.0.1:8787/api/vilo/server-info' | jq
curl -s 'http://127.0.0.1:8787/api/vilo/audit?site_id=000007' | jq
```

## Notes

- This bypasses AnythingLLM's unreliable agent transport for high-value network facts.
- Responses are generated from deterministic code in `mcp/jake_ops_mcp.py`.
- Current live inputs are local `network_map.db`, plus configured Bigmac, Alertmanager, NetBox, optional cnWave exporter sources, TAUC, and Vilo when available.


## Natural Query Mode

Jake API also exposes a deterministic natural-language query endpoint and CLI for common operator questions.

HTTP:

```bash
curl -s 'http://127.0.0.1:8787/api/query?q=how%20many%20customers%20are%20currently%20online%20for%20000007%3F' | jq
curl -s 'http://127.0.0.1:8787/api/query?q=are%20you%20witnessing%20any%20odd%20behavior%20on%20the%20192.168.44.0/24%20network%3F' | jq
curl -s 'http://127.0.0.1:8787/api/query?q=how%20many%20customers%20are%20online%20on%20000007.055.SW04%3F' | jq
```

CLI:

```bash
python3 scripts/jake_query.py 'how many customers are currently online for 000007?' | jq
python3 scripts/jake_query.py 'how many customers are online on 000007.055.SW04?' | jq
python3 scripts/jake_query.py 'trace MAC 30:68:93:C1:C8:34' | jq
```

## Regression Suite

Use the deterministic regression suite to verify that high-value operator questions still resolve to grounded Jake actions and internally consistent outputs.

```bash
make test-jake
make test-rename-sheet
make test-all
```

Equivalent direct command:

```bash
python3 scripts/run_jake_regression_suite.py
```

Rename sheet regression:

```bash
python3 scripts/run_rename_sheet_regression.py
```

Supported patterns currently include:

- subnet odd-behavior checks
- site/building/switch summaries
- online customer counts by site
- switch-level probable CPE counts
- MAC trace and CPE state
- probable TP-Link or Vilo candidate searches
- basic Vilo inventory, subscriber, network, and device queries
- Vilo audit and reconciliation against the latest scan and customer port map


## Building Scope Rule

When the query scope is a building identifier such as `000007.055`, Jake treats that as the full building block: all switches whose identities begin with that prefix, for example `000007.055.SW01`, `000007.055.SW02`, `000007.055.SW03`, and so on.


Additional natural query patterns:

- `which ports are flapping on 000007.055?`
- `show rogue dhcp suspects on 000007.055`
- `show recovery-ready cpes on 000007.055`


## Query Summary

Use `GET /api/query-summary?q=...` to get only the deterministic operator summary for a natural-language query.

Examples:

```bash
curl -s 'http://127.0.0.1:8787/api/query-summary?q=104%20tapscott%204b%20outage' | jq
curl -s 'http://127.0.0.1:8787/api/query-summary?q=how%20many%20customers%20are%20currently%20online%20for%20000007%3F' | jq
curl -s 'http://127.0.0.1:8787/api/query-summary?q=vilo%20audit%20000007' | jq
```

## Preferred AnythingLLM MCP Usage

For common operator questions in AnythingLLM, prefer a single MCP call to:

```text
MCP::jake_ops_mcp::query_summary
```

Example:

```text
Call MCP::jake_ops_mcp::query_summary with {"query":"we have a reported outage at 104 tapscott unit 4b. tell me everything you can about it."}
```

This returns:
- `query`
- `matched_action`
- `params`
- `operator_summary`
- `result`

Use lower-level tools like `get_online_customers`, `trace_mac`, or `get_subnet_health` only when raw structured output is explicitly required.
