# TAUC MCP

TAUC MCP adds direct TAUC cloud, ACS, and OLT access behind a single local MCP server.

## Scope

- shared TAUC request signing
- mutual TLS client certificate authentication
- TAUC cloud network-system-management and Aginet access
- ACS device lookup and state inspection
- OLT and ONU inventory/state inspection
- a small set of explicit write tools

Write tools are disabled by default.

## Entry Points

- [mcp/tauc_mcp.py](/Users/jono/projects/tik-troubleshoot-2-16-26/mcp/tauc_mcp.py)
- [tauc_mcp.py](/Users/jono/projects/tik-troubleshoot-2-16-26/tauc_mcp.py)

## Environment

Common variables:

```bash
TAUC_ENABLE_WRITES=false
TAUC_VERIFY_SSL=true
TAUC_CLIENT_CERT=/path/to/client.crt
TAUC_CLIENT_KEY=/path/to/client.key
TAUC_CLIENT_KEY_PASSWORD=
TAUC_CA_CERT=/path/to/root.crt
```

Cloud:

```bash
TAUC_CLOUD_BASE_URL=https://use1-tauc-openapi.tplinkcloud.com
TAUC_CLOUD_AUTH_TYPE=oauth2
TAUC_CLOUD_CLIENT_ID=...
TAUC_CLOUD_CLIENT_SECRET=...
```

ACS:

```bash
TAUC_ACS_BASE_URL=https://example-acs-domain
TAUC_ACS_AUTH_TYPE=aksk
TAUC_ACS_ACCESS_KEY=...
TAUC_ACS_SECRET_KEY=...
```

ACS with OAuth2:

```bash
TAUC_ACS_AUTH_TYPE=oauth2
TAUC_ACS_CLIENT_ID=...
TAUC_ACS_CLIENT_SECRET=...
```

OLT:

```bash
TAUC_OLT_BASE_URL=https://example-olt-domain
TAUC_OLT_AUTH_TYPE=aksk
TAUC_OLT_ACCESS_KEY=...
TAUC_OLT_SECRET_KEY=...
```

OLT with OAuth2:

```bash
TAUC_OLT_AUTH_TYPE=oauth2
TAUC_OLT_CLIENT_ID=...
TAUC_OLT_CLIENT_SECRET=...
```

Prefix-specific cert overrides are also supported:

```bash
TAUC_ACS_CLIENT_CERT=...
TAUC_ACS_CLIENT_KEY=...
TAUC_ACS_CA_CERT=...
TAUC_OLT_CLIENT_CERT=...
TAUC_OLT_CLIENT_KEY=...
TAUC_OLT_CA_CERT=...
```

## Tool Surface

Readonly ACS tools:

- `acs_get_device_id`
- `acs_get_device_detail`
- `acs_get_internet`
- `acs_get_wifi`
- `acs_get_lan_config`
- `acs_get_dhcp_config`
- `acs_get_tr_tree`
- `acs_get_task_result`

Readonly cloud tools:

- `cloud_get_network_name_list`
- `cloud_get_network_details`
- `cloud_get_network`
- `cloud_get_preconfiguration_status`
- `cloud_get_pppoe_status`
- `cloud_get_pppoe_credentials`
- `cloud_get_wifi_transmit_power`

Readonly OLT tools:

- `olt_get_devices`
- `olt_get_device_ids`
- `olt_get_device`
- `olt_get_device_name`
- `olt_get_pon_ports`
- `olt_get_onu_devices`
- `olt_get_onu_admin_status`
- `olt_get_onu_description`
- `olt_get_reboot_status`

Write tools:

- `cloud_update_network`
- `cloud_delete_network`
- `cloud_reset_device`
- `cloud_set_pppoe_credentials`
- `cloud_set_wifi_transmit_power`
- `cloud_block_client`
- `cloud_unblock_client`
- `acs_reboot_device`
- `olt_reboot_device`
- `olt_reboot_onus`
- `olt_set_onu_admin_status`

These require:

```bash
TAUC_ENABLE_WRITES=true
```

## Notes

- The server signs requests with the documented `Content-MD5`, `Timestamp`, `Nonce`, `Path` format and `HMAC-SHA256`.
- OAuth2 token retrieval uses:
  - Cloud: `/v1/openapi/token`
  - ACS: `/v1/openapi/token`
  - OLT: `/token`
- The first cut assumes `client.crt` + `client.key`. `p12` support is not implemented yet.
- The server reads the local repo `.env` automatically when launched directly.
