# Jake MikroTik Knowledge Pack

This is the curated MikroTik knowledge pack for Jake.

Use it as high-signal context. Do not treat forum posts as authoritative unless backed by field evidence or official docs.

## 1. Official docs to trust first

### RouterOS Documentation index
- https://help.mikrotik.com/docs/

Why it matters:
- canonical RouterOS manual
- stable reference point for command usage and feature behavior

### Bridging and Switching
- https://help.mikrotik.com/docs/spaces/ROS/pages/328068/Bridging%2Band%2BSwitching

Why it matters:
- bridge hardware offload behavior
- what sends packets to CPU vs switch chip
- bridge properties that disable or affect hardware offload
- switch-chip reset caveats when changing certain settings

High-value notes:
- `hw=yes/no` is per bridge port
- some bridge features are software-only
- packets switched in hardware often never reach CPU, so `torch`/`sniffer` can miss them
- some bridge/Ethernet setting changes can trigger switch-chip resets

### Layer2 misconfiguration
- https://help.mikrotik.com/docs/spaces/ROS/pages/19136718/Layer2%20misconfiguration

Why it matters:
- exact failure modes for common MikroTik mistakes

High-value notes:
- VLAN on a slave/master mismatch can cause partial DHCP weirdness
- “VLAN on a bridge in a bridge” / bad bridge design causes flooding, loops, MAC access issues, and instability
- split horizon disables hardware offload
- if you need CPU visibility for debugging, hardware learning behavior matters

### RoMON
- https://help.mikrotik.com/docs/spaces/ROS/pages/8978569/RoMON

Why it matters:
- management overlay independent of normal L2/L3 forwarding
- useful when IP path is broken

High-value notes:
- RoMON SSH is the correct secure CLI method over RoMON
- RoMON is not visible in `sniffer` or `torch`
- with bridge hardware offload, RoMON behaves like multicast and is flooded unless restricted

### MAC server
- https://help.mikrotik.com/docs/spaces/ROS/pages/98795539/MAC%2Bserver

Why it matters:
- MAC Telnet / MAC WinBox / MAC Ping management edge cases

High-value notes:
- MAC Telnet is MikroTik-to-MikroTik only
- restrict allowed interfaces deliberately
- useful fallback, but RoMON SSH is usually the better secure traversal method

## 2. Version-aware release notes and field-impact notes

### RouterOS 7.21.3 stable
- https://forum.mikrotik.com/t/v7-21-3-stable-is-released/268547

High-value note:
- `bridge - fixed dhcp-snooping incorrectly disabling HW offloading on QCA8337, Atheros8327 switch chips (introduced in v7.20)`

Why it matters:
- if you use DHCP snooping or related bridge features on affected chips, version matters

### RouterOS 7.20.1 stable
- https://forum.mikrotik.com/t/v7-20-1-stable-is-released/265492

Why it matters:
- anchor release thread for regressions reported against 7.20.1

## 3. Curated forum gotchas worth remembering

### PPPoE discovery leaking across VLANs on 7.20.1
- https://forum.mikrotik.com/t/pppoe-and-vlans-issue-on-7-20-1/265575

Observed claim:
- after upgrade to 7.20.1, PADI frames leaked across VLANs on a bridge with `vlan-filtering=yes`
- same MAC appeared active on multiple PPPoE VLANs
- downgrade to 7.19 resolved it for affected users

How Jake should use this:
- if a user reports PPPoE discovery cross-talk after 7.20.1 upgrade, flag version regression as a serious possibility
- do not claim it as universal truth without checking RouterOS version and topology

### CRS3xx switching vs bridging
- https://forum.mikrotik.com/t/crs3xx-switching-vs-bridging/169416/3

High-value summary:
- on CRS3xx, the bridge VLAN filtering model is the normal switching model
- “bridging” and “switching” are not separate operational worlds there if hardware offload is active

How Jake should use this:
- avoid giving outdated advice that treats CRS3xx bridge VLAN filtering as inherently “software only”

### Hardware offload expectations vary by platform
- https://forum.mikrotik.com/t/v7-17-2-stable-is-released/181257/138
- https://forum.mikrotik.com/t/hEX-PoE-bridge-mode-is-only-a-switch/182489/4
- https://forum.mikrotik.com/t/vlan-bridge-switch-chip-nat-only-using-one-core-rb-3011-uias-rm/178871/7

High-value summary:
- not all switch-chip families support the same offload behavior
- IPQ-PPE and some older chips have important caveats
- bridge VLAN filtering is correct on CRS3xx/CRS5xx, CCR2116/CCR2216, but can disable or limit offload on other platforms

How Jake should use this:
- always anchor hardware-offload advice to board model and switch chip family

## 4. Local repo knowledge Jake should combine with docs

### Read-only network mapping toolkit
- `/Users/jono/projects/tik-troubleshoot-2-16-26/NETWORK_MAP.md`
- `/Users/jono/projects/tik-troubleshoot-2-16-26/tools.md`
- `/Users/jono/projects/tik-troubleshoot-2-16-26/ros7_form.md`

Why it matters:
- your local truth for scan flow, inventory model, and field-tested RouterOS reasoning

### NetBox-backed MikroTik templating
- `/Users/jono/projects/tikfig/README.md`
- `/Users/jono/projects/tikfig/templates/router.jinja2`
- `/Users/jono/projects/tikfig/templates/switch.jinja2`

Why it matters:
- maps intended state from NetBox into RouterOS config structure

### LLDP topology logic
- `/Users/jono/projects/test/crs-vlan-detective/README.md`
- `/Users/jono/projects/test/crs-vlan-detective/agent.md`

Why it matters:
- useful discovery patterns for CRS/CCR fabrics

## 5. Operating rules for Jake

- Prefer live `ssh_mcp` evidence over forum anecdotes.
- Use official MikroTik docs before forum advice.
- Treat forum reports as “possible regression or field gotcha”, not gospel.
- Tie all RouterOS advice to:
  - version
  - board/model
  - switch chip family
  - whether hardware offload is active
- When explaining weird `torch`/`sniffer` results, remember that switched packets may never hit CPU.
- For recovery paths, prefer:
  1. IP SSH
  2. RoMON SSH
  3. MAC access only as fallback
