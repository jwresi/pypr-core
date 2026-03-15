# Cambium Remediation Sheet

Generated from the latest authenticated transport scan and direct follow-up probes on 2026-03-14.

## Hard Unreachable

These radios fail immediately with `Network is unreachable` on HTTPS and TCP `443`.
This is not a credential problem.

| Device | NetBox IP | Location | Likely Cause | Exact Fix |
| --- | --- | --- | --- | --- |
| 324 Howard Ave V5000 | 192.168.44.154 | 324 Howard Ave, Brooklyn, NY 11233 | Management IP no longer reachable from the `192.168.44.0/24` scan path, or IP is stale in NetBox | Verify the live management IP on the radio. If correct, restore mgmt VLAN/path. If changed, update NetBox primary IP. |
| 721 Fenimore V1000 | 192.168.44.151 | 726 Fenimore St, Brooklyn, NY 11203 | Same class of issue | Verify live mgmt IP and restore path or correct NetBox. |
| 728 E New York V5000 | 192.168.44.152 | 728 E New York Ave, Brooklyn, NY 11203 | Same class of issue | Verify live mgmt IP and restore path or correct NetBox. |
| 2045 Union V5000 | 192.168.44.31 | 2045 Union St, Brooklyn, NY 11212 | Same class of issue | Verify live mgmt IP and restore path or correct NetBox. |
| 2069 Union V5000 | 192.168.44.32 | 2069 Union St, Brooklyn, NY 11212 | Same class of issue | Verify live mgmt IP and restore path or correct NetBox. |

## Times Out On 443

These radios have an IP in NetBox and do not fail as unroutable, but HTTPS/API never responds before timeout.

| Device | NetBox IP | Location | Likely Cause | Exact Fix |
| --- | --- | --- | --- | --- |
| 1142 Lenox Rd V1000 | 192.168.44.40 | 1142 Lenox Rd, Brooklyn, NY 11212 | Radio reachable at L3 path but cnWave web/API is not answering | Check radio power/health, HTTPS service, packet loss, or mgmt ACL on `443`. Confirm IP is still current. |
| 1196 E NY Ave V2000 | 192.168.44.69 | 1196 E New York Ave, Brooklyn, NY 11212 | Same class of issue | Check radio power/health, HTTPS service, packet loss, or mgmt ACL on `443`. Confirm IP is still current. |
| 1629 Park Pl V1000 | 192.168.44.57 | 1629 Park Pl, Brooklyn, NY 11233 | Same class of issue | Check radio power/health, HTTPS service, packet loss, or mgmt ACL on `443`. Confirm IP is still current. |

## Missing NetBox Management IP

These radios cannot be scanned because NetBox has no primary management IP on the device.

| Device | NetBox Status | Location | Likely Cause | Exact Fix |
| --- | --- | --- | --- | --- |
| 1371 St Marks Ave V5000 | staged | 1371 St Marks Ave, Brooklyn, NY 11233 | Inventory incomplete | Add the correct primary management IP in NetBox and move device out of `staged` when appropriate. |
| 1578 Sterling Av V5000 | active | 1578 Sterling Pl, Brooklyn, NY 11213 | Inventory incomplete | Add the correct primary management IP in NetBox. |
| 1640 Sterling Pl V2000 | staged | 1640 Sterling Pl, Brooklyn, NY 11233 | Inventory incomplete | Add the correct primary management IP in NetBox and move device out of `staged` when appropriate. |

## Notes

- Scanner auth is fixed. The radios that are working now authenticate successfully with JSON login to `/local/userLogin`.
- The current backend is already using live Cambium coordinates and neighbor-derived peer links from the successful scans.
- Remaining failures are now inventory or network-path problems, not credential-format problems.
