# Jake CPE Knowledge Pack

This is the curated CPE knowledge pack for Jake.

Use it when the user asks about subscriber CPE behavior, onboarding, DHCP vs PPPoE mode, remote recovery, or cloud-managed device troubleshooting.

Treat official vendor docs as primary. Treat mirrored manuals and field notes as secondary.

## 1. Device families Jake should recognize

### Vilo 5 / Vilo mesh

Observed local OUI:
- `E8:DA:00`

Why it matters:
- These units often appear on customer-facing access ports where WAN mode mistakes look like upstream network problems.
- They can sit in recovery-like states for a long time before calling home or completing setup.

Operational pattern from field work:
- If a Vilo is expected to run PPPoE but is only broadcasting DHCP discover on customer VLAN 20, the device is likely in the wrong WAN mode or not fully provisioned.
- If moved temporarily to a recovery/config VLAN and it receives repeated DHCP offers but never completes lease acceptance, the remaining problem is usually CPE-side, not switch-side.
- Some Vilo recoveries are slow. A short watch window is not enough; 15 to 30 minutes is reasonable before declaring the unit stuck.

Official-or-near-official references:
- Vilo quick-start guidance says broadband failure on the main router should be checked against WAN cabling and internet method, including PPPoE credentials when PPPoE is used.
- Vilo app workflow is the primary provisioning path.

Secondary reference:
- `https://manuals.plus/m/7173bcb10feb9171766b59864eb146ccc8f9b82f675a11215a84c86163b148d8`
  - Use only as mirrored quick-start guidance, not canonical vendor documentation.

High-value notes:
- The WAN/LAN port is dual-role: WAN on the main router, LAN on sub routers.
- LED state is useful for field triage:
  - flashing red on the main router means broadband failure
  - solid amber means ready for configuration
- If PPPoE is required, incorrect PPPoE credentials can look like generic broadband failure.

How Jake should use this:
- When a Vilo is seen on VLAN 20 with DHCP discover and no PPPoE, do not blame the MikroTik path first.
- Confirm whether the device is meant to be in router mode or staged on a config VLAN.
- For a suspected stuck Vilo, recommend:
  1. verify access-port VLAN correctness
  2. verify no rogue DHCP on customer VLAN
  3. place on recovery/config VLAN if appropriate
  4. watch for at least 15 to 30 minutes
  5. if still `offered`-only with no ARP/call-home, treat as bad/stuck CPE

### TP-Link HC220 / Aginet

Observed local OUIs commonly grouped as TP-Link/Aginet:
- `30:68:93`
- `60:83:E7`
- `7C:F1:7E`
- `D8:44:89`
- `DC:62:79`
- `E4:FA:C4`

Why it matters:
- HC220 units can run either Router Mode or Access Point Mode.
- They support multiple WAN types, so DHCP traffic alone does not prove the network is wrong.
- They may be managed locally, by TP-Link cloud, or by TAUC in ISP environments.

Primary references:
- TP-Link HC220-G5 support page
- TP-Link HC220-G5 user guide
- TP-Link HC220-G5 service-provider product page

Vendor-backed facts Jake should remember:
- HC220 supports:
  - Router Mode
  - Access Point Mode
- Supported WAN types include:
  - Dynamic IP
  - Static IP
  - PPPoE
- Management can include:
  - local web management
  - TP-Link cloud / TP-Link ID
  - TAUC cloud management on service-provider builds
- Some firmware capabilities are controlled from TAUC, not exposed in the local GUI. TP-Link’s support notes explicitly mention features such as Auto Update being hidden in Web GUI/Aginet App but controlled through TAUC tasks on some builds.

Key official references:
- TP-Link HC220-G5 user guide PDF:
  - `https://static.tp-link.com/upload/manual/2022/202203/20220308/UG_HC220-G5.pdf`
- TP-Link service-provider HC220-G5 page:
  - `https://www.tp-link.com/us/service-provider/home-wifi-system/hc220-g5/`
- TP-Link HC220-G5 support/download page:
  - `https://www.tp-link.com/us/support/download/hc220-g5/`

High-value notes from those references:
- On HC220-G5, Access Point Mode is the default mode.
- Router Mode is the mode with the full WAN feature set.
- In Router Mode, PPPoE username/password can be entered directly in the local UI.
- In Access Point Mode, NAT and some router-style features are not supported.
- TP-Link explicitly lists TAUC cloud management for the HC220-G5 service-provider build.

How Jake should use this:
- If an HC220 is on a customer VLAN and only DHCP is seen, first consider that it may be in AP mode or Router Mode with Dynamic IP selected.
- If PPPoE is expected but not seen, check whether the unit build/site standard actually expects PPPoE or DHCP recovery/staging.
- If TAUC is in play, prefer TAUC state over guesswork about local UI exposure because some controls/features may only exist in TAUC.

## 2. TAUC / cloud-management guidance

What is verified:
- TP-Link’s service-provider materials tie HC220 service-provider management to TAUC.
- TP-Link support notes mention TAUC Task and TAUC Network Health on HC220-G5 firmware/support pages.

What is now verified locally:
- TAUC API documentation exists locally for cloud, ACS, and OLT surfaces.
- Jake now has a TAUC MCP integration and deterministic TAUC read paths.

How Jake should speak about TAUC:
- Prefer TAUC state when a TP-Link/Aginet CPE is likely cloud-managed and TAUC is configured.
- Use Jake's deterministic TAUC read paths before speculating about local GUI state.
- Treat TAUC as a likely source of truth for:
  - cloud adoption status
  - WAN mode/config state
  - firmware version
  - online/offline history
  - remote diagnostics

## 3. Cross-vendor CPE troubleshooting rules

- Distinguish physical presence from service presence:
  - bridge-host MAC present on access port = physically seen
  - PPP active / ARP / lease / call-home = service state
- Do not assume DHCP on customer VLAN 20 is correct. It may be:
  - wrong WAN mode
  - rogue DHCP contamination
  - temporary staging path
- For Vilo and HC220 alike:
  - PPPoE expected + only DHCP seen = mode/config mismatch is likely
  - repeated DHCP offer with no lease completion = CPE-side fault is likely
- On TP-Link service-provider builds, local UI visibility may not tell the whole truth if TAUC controls the feature remotely.
- On Vilo, app-led provisioning and delayed cloud contact can make short diagnostic windows misleading.

## 4. Local repo signals Jake should combine with this pack

- `/Users/jono/projects/tik-troubleshoot-2-16-26/tools.md`
- `/Users/jono/projects/tik-troubleshoot-2-16-26/ros7_form.md`
- `/Users/jono/projects/tikbreak/000001.R1.rsc`
- `/Users/jono/projects/tikbreak/000003.R1.rsc`
- `/Users/jono/projects/tikbreak/000004.R1.rsc`

Why they matter:
- They already encode OUI grouping and some historical Vilo/TP-Link staging assumptions.
- They help Jake correlate live MikroTik observations with local field patterns.

## 5. Operating rules for Jake

- Prefer live `ssh_mcp` evidence over device-brand stereotypes.
- Use official vendor docs before mirrored manuals.
- Treat TAUC as a potential primary truth source for HC220 only after API integration is real.
- For Vilo 5 and HC220 questions, explicitly state whether the conclusion is based on:
  - live MikroTik evidence
  - local repo knowledge
  - official vendor docs
  - secondary/manual mirror content
