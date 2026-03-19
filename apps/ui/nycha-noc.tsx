import React, { Fragment, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import maplibregl from "maplibre-gl";
import siteCoords from "./site-coords.json";
import buildingProfiles from "./building-profiles.json";

type Status = "online" | "degraded" | "offline" | "unknown";

type BuildingLayout = {
  id: string;
  sourceBuildingId: string;
  name: string;
  shortLabel: string;
  address: string;
  development: string;
  floors: number;
  x: number;
  y: number;
};

type RadioLayout = {
  id: string;
  name: string;
  shortLabel: string;
  address: string;
  ip?: string;
  model: string;
  role: string;
  anchorBuildingId: string;
  x: number;
  y: number;
};

type LinkDef = {
  from: string;
  to: string;
  strength: "strong" | "medium" | "weak";
  freq: string;
  model: string;
  kind: string;
};

type JakeAlert = {
  annotations?: {
    summary?: string;
    description?: string;
    location?: string;
    device_name?: string;
  };
  labels?: {
    name?: string;
    severity?: string;
    site_id?: string;
  };
};

type BuildingHealth = {
  building_id: string;
  device_count: number;
  outlier_count: number;
  probable_cpe_count: number;
  devices: Array<{ identity: string; ip: string; model: string; version: string }>;
};

type BuildingCustomerCount = {
  building_id: string;
  count: number;
  switch_count: number;
  access_port_count: number;
  vendor_summary: Record<string, number>;
  results: PortRecord[];
};

type CpeNetworkContext = {
  network_id?: string;
  network_name?: string;
  network_status?: string;
  uptime?: string;
  vilo_online_num?: number;
  vilo_offline_num?: number;
  device_online_num?: number;
  device_offline_num?: number;
  wan_ip_address?: string;
  public_ip_address?: string;
  firmware_version?: string;
  installer?: string;
  subscriber_id?: string;
  flags?: string[];
};

type BuildingCpeIntelligence = {
  building_id: string;
  vendor_summary: Record<string, number>;
  customer_count: number;
  access_port_count: number;
  switch_count: number;
  dark_building: boolean;
  vilo: {
    configured: boolean;
    count: number;
    firmware_versions: Record<string, number>;
    dark_candidate_count: number;
    error?: string;
    rows: Array<{
      device_mac: string;
      classification?: string;
      inventory_status?: string;
      device_sn?: string;
      subscriber_id?: string;
      subscriber?: { subscriber_id?: string; first_name?: string; last_name?: string; email?: string } | null;
      subscriber_hint?: { source?: string; label?: string; display?: string } | null;
      network?: CpeNetworkContext | null;
      sighting?: { identity?: string; on_interface?: string; port_status?: string; building_id?: string } | null;
    }>;
  };
  tauc: {
    configured: boolean;
    count: number;
    rows: Array<{
      network_name?: string;
      site_id?: string;
      expected_prefix?: string;
      mac?: string;
      sn?: string;
      wan_mode?: string;
      mesh_nodes?: number;
    }>;
  };
};

type CpeContext = {
  mac: string;
  vendor: string;
  building_id?: string | null;
  vilo?: {
    classification?: string;
    inventory_status?: string;
    device_sn?: string;
    subscriber_id?: string;
    subscriber?: { subscriber_id?: string; first_name?: string; last_name?: string; email?: string } | null;
    subscriber_hint?: { source?: string; label?: string; display?: string } | null;
    network?: CpeNetworkContext | null;
    sighting?: { identity?: string; on_interface?: string; port_status?: string; building_id?: string } | null;
    error?: string;
  } | null;
  tauc?: {
    network_name?: string;
    site_id?: string;
    expected_prefix?: string;
    wan_mode?: string;
    mesh_nodes?: number;
    sn?: string;
  } | null;
};

type PortRecord = {
  identity: string;
  ip: string;
  mac: string;
  on_interface: string;
  vid: number;
  local: number;
  external: number;
};

type PortIssue = {
  identity: string;
  interface: string;
  status?: string;
  issues?: string[];
  fixes?: string[];
  comment?: string;
  last_link_up_time?: string;
  link_downs?: string;
};

type IssueResponse = {
  count: number;
  ports: PortIssue[];
};

type BuildingModel = {
  building_id: string;
  site_id: string;
  address?: string;
  known_units: string[];
  floors_inferred_from_units: number;
  exact_unit_port_matches: Array<{
    network_name: string;
    unit: string;
    classification: string;
    switch_identity: string;
    interface: string;
    mac: string;
    evidence_sources?: string[];
  }>;
  unit_state_decisions: Array<{
    unit: string;
    state: "online" | "unknown";
    network_name?: string | null;
    mac?: string | null;
    sources: string[];
    switch_identity?: string | null;
    interface?: string | null;
  }>;
  live_port_pool: Array<{
    switch_identity: string;
    interface: string;
    mac: string;
    vid: number;
  }>;
  switches: Array<{
    identity: string;
    ip: string;
    model?: string;
    version?: string;
    served_units: string[];
    served_floors: number[];
    exact_match_count: number;
    direct_neighbors: Array<{
      from_identity: string;
      from_interface: string;
      to_identity: string;
      neighbor_address?: string;
      platform?: string;
      version?: string;
    }>;
  }>;
  direct_neighbor_edges: Array<{
    from_identity: string;
    from_interface: string;
    to_identity: string;
    neighbor_address?: string;
    platform?: string;
    version?: string;
  }>;
  radios: Array<{
    id?: string;
    name: string;
    type: string;
    model: string;
    status: string;
  }>;
  coverage: {
    known_unit_count: number;
    exact_unit_port_match_count: number;
    exact_unit_port_coverage_pct: number;
    live_port_pool_count: number;
    switch_count: number;
    direct_neighbor_edge_count: number;
  };
  data_gaps: {
    building_geometry: string;
    full_unit_to_port_mapping: string;
    switch_floor_placement: string;
  };
};

type SiteSummary = {
  site_id: string;
  devices_total: number;
  switches_count: number;
  outlier_count: number;
  active_alerts?: JakeAlert[];
  online_customers: {
    count: number;
    counting_method: string;
    matched_routers: Array<{ identity: string; ip: string }>;
  };
  bridge_host_summary?: {
    total?: number;
    tplink?: number;
    vilo?: number;
  };
  scan?: {
    id: number;
    finished_at: string;
    subnet: string;
    api_reachable: number;
  };
};

type SiteTopology = {
  radios: Array<{
    name: string;
    type: string;
    model: string;
    ip?: string;
    location: string;
    status: string;
    resolved_building_id?: string;
    address_units?: string[];
    latitude?: number | null;
    longitude?: number | null;
    coordinate_source?: string;
    alerts?: JakeAlert[];
  }>;
  radio_links: Array<{
    name: string;
    kind: string;
    from_label?: string;
    to_label?: string;
    location?: string;
    status?: string;
    from_radio_id?: string;
    to_radio_id?: string;
    from_latitude?: number | null;
    from_longitude?: number | null;
    to_latitude?: number | null;
    to_longitude?: number | null;
    from_building_id?: string;
    to_building_id?: string;
    inferred?: boolean;
  }>;
  addresses: Array<{
    address: string;
    building_id?: string;
    units: string[];
    network_names: string[];
    latitude?: number | null;
    longitude?: number | null;
  }>;
  buildings?: Array<{
    building_id: string;
    customer_count: number;
    health: BuildingHealth;
    known_units: string[];
    latitude?: number | null;
    longitude?: number | null;
  }>;
};

type SiteCoord = {
  lat: number;
  lon: number;
};

type BuildingProfile = {
  massingType: "tower" | "midrise-slab";
  authoritativeFloors?: number;
  roofNodeLabel?: string;
  roofNodeKind?: "router" | "roof-switch";
  basementLabel?: string;
  switchFloorOverrides?: Record<string, number>;
};

type BuildingLive = BuildingLayout & {
  status: Status;
  customerCount: number;
  deviceCount: number;
  alertCount: number;
  outlierCount: number;
  knownUnits: string[];
  buildingHealth?: BuildingHealth;
  buildingCustomerCount?: BuildingCustomerCount;
  flapHistory?: IssueResponse;
  rogueDhcp?: IssueResponse;
  recoveryReady?: IssueResponse;
  buildingModel?: BuildingModel;
  profile?: BuildingProfile;
  cpeIntelligence?: BuildingCpeIntelligence;
};

type BuildingDataEntry = {
  buildingHealth?: BuildingHealth;
  buildingCustomerCount?: BuildingCustomerCount;
  flapHistory?: IssueResponse;
  rogueDhcp?: IssueResponse;
  recoveryReady?: IssueResponse;
  buildingModel?: BuildingModel;
  cpeIntelligence?: BuildingCpeIntelligence;
};

type TopologyNode = {
  id: string;
  label: string;
  kind: "radio" | "router" | "roof-switch" | "switch" | "external";
  status: Status;
  detail?: string;
};

type TopologyEdge = {
  from: string;
  to: string;
  label?: string;
  inferred?: boolean;
};

type HoverCardData = {
  title: string;
  subtitle?: string;
  facts: Array<{ label: string; value: string }>;
};

type RadioLive = RadioLayout & {
  status: Status;
  alert?: JakeAlert | null;
  knownUnits?: string[];
  latitude?: number | null;
  longitude?: number | null;
};

type PortWithStatus = PortRecord & {
  status: Status;
  statusLabel: string;
  notes: string[];
};

type UnitBox = {
  unit: string;
  floor: number;
  port: PortWithStatus | null;
  inferred: boolean;
  status: Status;
};

type UnitPrismProps = {
  unit: UnitBox;
  selected: boolean;
  onSelectUnit: (unit: UnitBox) => void;
  onInspectPort: (port: PortWithStatus) => void;
  compact?: boolean;
};

type SummaryFocus = "devices" | "ports" | "flaps" | "units" | "matches" | "coverage" | null;

const STATUS_COLOR: Record<Status, string> = {
  online: "#22c55e",
  degraded: "#f59e0b",
  offline: "#ef4444",
  unknown: "#38bdf8",
};

const STATUS_BG: Record<Status, string> = {
  online: "#052e16",
  degraded: "#451a03",
  offline: "#450a0a",
  unknown: "#082f49",
};

const LINK_COLOR = {
  strong: "#22c55e",
  medium: "#f59e0b",
  weak: "#ef4444",
};

const MAP_STYLE = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["/tiles/{z}/{x}/{y}.png"],
      tileSize: 256,
      maxzoom: 19,
      attribution: "© OpenStreetMap contributors",
    },
  },
  layers: [
    {
      id: "osm",
      type: "raster",
      source: "osm",
      paint: {
        "raster-opacity": 0.4,
        "raster-saturation": -0.35,
        "raster-contrast": 0.12,
        "raster-brightness-max": 0.86,
      },
    },
  ],
} as {
  version: number;
  sources: Record<string, {
    type: "raster";
    tiles: string[];
    tileSize: number;
    maxzoom: number;
    attribution: string;
  }>;
  layers: Array<Record<string, unknown>>;
};

const BUILDING_LAYOUTS: BuildingLayout[] = [
  {
    id: "000007.055",
    sourceBuildingId: "000007.055",
    name: "728 E New York Ave",
    shortLabel: "728 E NY",
    address: "728 E New York Ave, Brooklyn, NY 11203",
    development: "NYCHA Headend",
    floors: 16,
    x: 120,
    y: 280,
  },
  {
    id: "000007.058",
    sourceBuildingId: "000007.058",
    name: "955 Rutland Rd",
    shortLabel: "955 Rutland",
    address: "955 Rutland Rd, Brooklyn, NY 11212",
    development: "NYCHA Transport",
    floors: 4,
    x: 250,
    y: 180,
  },
  {
    id: "000007.004",
    sourceBuildingId: "000007.004",
    name: "1145 Lenox Rd",
    shortLabel: "1145 Lenox",
    address: "1145 Lenox Rd, Brooklyn, NY 11212",
    development: "Lenox Cluster",
    floors: 4,
    x: 395,
    y: 240,
  },
  {
    id: "000007.053",
    sourceBuildingId: "000007.053",
    name: "725 Howard Ave",
    shortLabel: "725 Howard",
    address: "725 Howard Ave, Brooklyn, NY 11212",
    development: "NYCHA Transport",
    floors: 4,
    x: 530,
    y: 155,
  },
];

const LEGACY_TRANSPORT_LINKS: LinkDef[] = [
  {
    from: "000007.055",
    to: "000007.058",
    strength: "strong",
    freq: "5.8GHz",
    model: "Legacy transport",
    kind: "Fallback transport",
  },
  {
    from: "000007.058",
    to: "000007.004",
    strength: "medium",
    freq: "5.8GHz",
    model: "Legacy transport",
    kind: "Fallback transport",
  },
  {
    from: "000007.004",
    to: "000007.053",
    strength: "medium",
    freq: "5.8GHz",
    model: "Legacy transport",
    kind: "Fallback transport",
  },
];

const DEFAULT_JAKE_BASE_URL =
  typeof window !== "undefined" && window.location.port === "8787"
    ? window.location.origin
    : "";

const UI_STATE_STORAGE_KEY = "nycha-noc-ui-state-v1";
const BUILDING_DATA_BATCH_SIZE = 6;

const BUILDING_PROFILES = buildingProfiles as Record<string, BuildingProfile>;
const COMPOUND_SITE_OVERRIDES: Record<string, { rootSwitchIdentity: string; members: string[]; mode: "agg-fed branch site" | "switch-fed branch site" }> = {
  "000007.051": {
    rootSwitchIdentity: "000007.051.AG01",
    members: ["000007.052", "000007.060", "000007.061"],
    mode: "agg-fed branch site",
  },
};
const STATIC_SITE_COORDS = siteCoords as Record<string, SiteCoord>;
const LEGACY_LAYOUT_BY_SOURCE_ID = new Map(BUILDING_LAYOUTS.map((layout) => [layout.sourceBuildingId, layout]));

function normalizeAddressKey(value: string) {
  return value
    .toLowerCase()
    .replace(/\beast\b/g, "e")
    .replace(/\bwest\b/g, "w")
    .replace(/\bavenue\b/g, "ave")
    .replace(/\bstreet\b/g, "st")
    .replace(/\broad\b/g, "rd")
    .replace(/\bplace\b/g, "pl")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function addressStemKey(value: string) {
  const base = value.split(",")[0]?.trim() ?? value;
  return base
    .toLowerCase()
    .replace(/\beast\b/g, "e")
    .replace(/\bwest\b/g, "w")
    .replace(/\bavenue\b/g, "ave")
    .replace(/\bstreet\b/g, "st")
    .replace(/\broad\b/g, "rd")
    .replace(/\bplace\b/g, "pl")
    .replace(/\s+/g, " ")
    .trim();
}

const STATIC_SITE_COORDS_BY_NORMALIZED = new Map(
  Object.entries(STATIC_SITE_COORDS).map(([address, coord]) => [normalizeAddressKey(address), coord]),
);
const STATIC_SITE_COORDS_BY_STEM = new Map(
  Object.entries(STATIC_SITE_COORDS).map(([address, coord]) => [addressStemKey(address), coord]),
);

function ifaceLabel(iface?: string | null) {
  return iface && iface.trim() ? iface : "unknown";
}

function isPresent<T>(value: T | null | undefined): value is T {
  return value != null;
}

function compareInterfaceLabels(a?: string | null, b?: string | null) {
  return ifaceLabel(a).localeCompare(ifaceLabel(b), undefined, { numeric: true, sensitivity: "base" });
}

function portKey(identity: string, iface?: string | null) {
  return `${identity}:${ifaceLabel(iface)}`;
}

function portRenderKey(port: { identity: string; on_interface?: string | null; mac: string }) {
  return `${portKey(port.identity, port.on_interface)}:${port.mac}`;
}

function vendorFromMac(mac: string) {
  const lower = mac.toLowerCase();
  if (lower.startsWith("30:68:93") || lower.startsWith("60:83:e7") || lower.startsWith("7c:f1:7e") || lower.startsWith("dc:62:79")) return "TP-Link";
  if (lower.startsWith("e8:da:00")) return "Vilo";
  return "Unknown";
}

function normalizeMac(mac: string | null | undefined) {
  return String(mac ?? "").trim().toLowerCase();
}

function titleCaseWords(value: string) {
  return value
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function shortAddressLabel(address: string) {
  const base = address.split(",")[0]?.trim() ?? address;
  return base
    .replace(/\bRoad\b/gi, "Rd")
    .replace(/\bAvenue\b/gi, "Ave")
    .replace(/\bStreet\b/gi, "St")
    .replace(/\bPlace\b/gi, "Pl");
}

function deriveBuildingName(address: string) {
  return titleCaseWords(shortAddressLabel(address));
}

function normalizeSiteToken(value: string) {
  return value
    .toLowerCase()
    .replace(/\beast new york\b/g, "eny")
    .replace(/\be ny\b/g, "eny")
    .replace(/\bsaint\b/g, "st")
    .replace(/\bavenue\b/g, "ave")
    .replace(/\broad\b/g, "rd")
    .replace(/\bstreet\b/g, "st")
    .replace(/\bplace\b/g, "pl")
    .replace(/[^a-z0-9]+/g, "");
}

function buildingMatchesEndpoint(building: Pick<BuildingLayout, "name" | "shortLabel" | "address">, label?: string | null, location?: string | null) {
  const buildingTokens = [
    normalizeSiteToken(building.name),
    normalizeSiteToken(building.shortLabel),
    normalizeSiteToken(building.address),
  ];
  const labelToken = label ? normalizeSiteToken(label) : "";
  const locationToken = location ? normalizeSiteToken(location) : "";
  if (locationToken && buildingTokens.some((token) => token === locationToken)) return true;
  if (labelToken && buildingTokens.some((token) => token.includes(labelToken) || labelToken.includes(token))) return true;
  return false;
}

function findMatchingBuilding(
  buildings: Array<Pick<BuildingLayout, "id" | "sourceBuildingId" | "name" | "shortLabel" | "address"> & { status?: Status }>,
  label?: string | null,
  buildingId?: string | null,
  location?: string | null,
) {
  if (buildingId) {
    const exact = buildings.find((building) => canonicalBuildingIdOf(building) === buildingId);
    if (exact) return exact;
  }
  const candidates = buildingId
    ? buildings.filter((building) => canonicalBuildingIdOf(building) === buildingId)
    : buildings;
  if (!candidates.length) return null;
  const exactLocation = location
    ? candidates.find((building) => normalizeSiteToken(building.address) === normalizeSiteToken(location))
    : null;
  if (exactLocation) return exactLocation;
  return candidates.find((building) => buildingMatchesEndpoint(building, label, location)) ?? null;
}

function radioMatchesEndpoint(radio: Pick<RadioLive, "name" | "shortLabel" | "address">, label?: string | null, location?: string | null) {
  const radioTokens = [
    normalizeSiteToken(radio.name),
    normalizeSiteToken(radio.shortLabel),
    normalizeSiteToken(radio.address),
  ];
  const labelToken = label ? normalizeSiteToken(label) : "";
  const locationToken = location ? normalizeSiteToken(location) : "";
  if (locationToken && radioTokens.some((token) => token === locationToken)) return true;
  if (labelToken && radioTokens.some((token) => token.includes(labelToken) || labelToken.includes(token))) return true;
  return false;
}

function inferFloorsFromUnits(units: string[]) {
  const floors = units
    .map((unit) => Number.parseInt(unit, 10))
    .filter((value) => Number.isFinite(value) && value > 0);
  return floors.length ? Math.max(...floors) : 4;
}

function canonicalBuildingIdOf(building: Pick<BuildingLayout, "id" | "sourceBuildingId">) {
  return building.sourceBuildingId || building.id;
}

function buildingIdFromIdentity(identity?: string | null) {
  const match = String(identity || "").match(/^(\d{6}\.\d{3})\./);
  return match ? match[1] : null;
}

function isSwitchLikeIdentity(identity?: string | null) {
  return /\.(?:AG|SW|RFSW|R)\d*/i.test(String(identity || ""));
}

function compoundSiteContext(building: BuildingLive, allBuildings: BuildingLive[]) {
  const localBuildingId = canonicalBuildingIdOf(building);
  for (const [rootId, override] of Object.entries(COMPOUND_SITE_OVERRIDES)) {
    const clusterIds = [rootId, ...override.members];
    if (!clusterIds.includes(localBuildingId)) continue;
    const cluster = clusterIds
      .map((buildingId) => allBuildings.find((entry) => canonicalBuildingIdOf(entry) === buildingId))
      .filter(isPresent);
    if (cluster.length < 2) return null;
    return {
      rootSwitchIdentity: override.rootSwitchIdentity,
      mode: override.mode,
      cluster,
      downstreamCount: override.members.length,
    };
  }
  const evaluateRoot = (candidate: BuildingLive) => {
    const candidateId = canonicalBuildingIdOf(candidate);
    const edges = candidate.buildingModel?.direct_neighbor_edges ?? [];
    const localSwitches = new Set(
      [
        ...(candidate.buildingModel?.switches.map((entry) => entry.identity) ?? []),
        ...(candidate.buildingHealth?.devices.map((entry) => entry.identity) ?? []),
      ].filter((identity) => isSwitchLikeIdentity(identity)),
    );
    const localAggSwitches = [...localSwitches].filter((identity) => identity.includes(".AG"));
    const explicitAggMembers = new Map<string, Set<string>>();
    for (const aggIdentity of localAggSwitches) {
      for (const otherBuilding of allBuildings) {
        const otherId = canonicalBuildingIdOf(otherBuilding);
        if (otherId === candidateId) continue;
        for (const edge of otherBuilding.buildingModel?.direct_neighbor_edges ?? []) {
          if (edge.to_identity !== aggIdentity) continue;
          const remoteBuildingId = buildingIdFromIdentity(edge.from_identity) ?? otherId;
          if (!remoteBuildingId || remoteBuildingId === candidateId) continue;
          const rows = explicitAggMembers.get(aggIdentity) ?? new Set<string>();
          rows.add(remoteBuildingId);
          explicitAggMembers.set(aggIdentity, rows);
        }
      }
    }
    const aggRanked = [...explicitAggMembers.entries()]
      .map(([switchIdentity, downstream]) => ({ switchIdentity, downstream: [...downstream] }))
      .sort((a, b) => b.downstream.length - a.downstream.length || a.switchIdentity.localeCompare(b.switchIdentity));
    if (aggRanked[0]?.downstream.length) {
      return {
        root: candidate,
        rootSwitchIdentity: aggRanked[0].switchIdentity,
        downstream: aggRanked[0].downstream,
        mode: "agg-fed branch site" as const,
      };
    }
    const downstreamBySwitch = new Map<string, Set<string>>();
    for (const edge of edges) {
      if (!localSwitches.has(edge.from_identity) || !isSwitchLikeIdentity(edge.to_identity)) continue;
      const remoteBuildingId = buildingIdFromIdentity(edge.to_identity);
      if (!remoteBuildingId || remoteBuildingId === candidateId) continue;
      const rows = downstreamBySwitch.get(edge.from_identity) ?? new Set<string>();
      rows.add(remoteBuildingId);
      downstreamBySwitch.set(edge.from_identity, rows);
    }
    const ranked = [...downstreamBySwitch.entries()]
      .map(([switchIdentity, downstream]) => ({ switchIdentity, downstream: [...downstream] }))
      .sort((a, b) => {
        const aRank = a.switchIdentity.includes(".AG") ? 2 : a.switchIdentity.includes("RFSW") ? 1 : 0;
        const bRank = b.switchIdentity.includes(".AG") ? 2 : b.switchIdentity.includes("RFSW") ? 1 : 0;
        return b.downstream.length - a.downstream.length || bRank - aRank || a.switchIdentity.localeCompare(b.switchIdentity);
      });
    const selected = ranked[0];
    if (!selected || selected.downstream.length === 0) return null;
    return {
      root: candidate,
      rootSwitchIdentity: selected.switchIdentity,
      downstream: selected.downstream,
      mode: selected.switchIdentity.includes(".AG") ? "agg-fed branch site" : "switch-fed branch site",
    };
  };

  const directRoot = evaluateRoot(building);
  const resolved = directRoot
    ?? allBuildings
      .map((candidate) => evaluateRoot(candidate))
      .filter(isPresent)
      .find((context) => context.downstream.includes(localBuildingId));
  if (!resolved) return null;
  const members = resolved.downstream
    .map((buildingId) => allBuildings.find((entry) => canonicalBuildingIdOf(entry) === buildingId))
    .filter(isPresent);
  const cluster = [resolved.root, ...members.filter((entry) => canonicalBuildingIdOf(entry) !== canonicalBuildingIdOf(resolved.root))];
  if (cluster.length < 2) return null;
  return {
    rootSwitchIdentity: resolved.rootSwitchIdentity,
    mode: resolved.mode,
    cluster,
    downstreamCount: resolved.downstream.length,
  };
}

function addressLayoutId(address: string, sourceBuildingId: string, duplicateCount: number) {
  if (duplicateCount <= 1) return sourceBuildingId;
  return `${sourceBuildingId}:${radioIdFromName(address)}`;
}

function unitsPerFloorEstimate(units: string[]) {
  if (!units.length) return 0;
  const counts = new Map<number, number>();
  for (const unit of units) {
    const floor = floorNumber(unit);
    if (!floor) continue;
    counts.set(floor, (counts.get(floor) ?? 0) + 1);
  }
  return Math.max(...counts.values(), 0);
}

function synthesizeProfile(building: Pick<BuildingLayout, "id" | "sourceBuildingId" | "floors"> & Pick<BuildingLive, "knownUnits" | "buildingModel" | "buildingCustomerCount">) {
  const canonicalId = canonicalBuildingIdOf(building);
  const explicit = BUILDING_PROFILES[canonicalId];
  if (canonicalId === "000007.055") {
    return {
      massingType: "tower",
      authoritativeFloors: 20,
      roofNodeLabel: "R01",
      roofNodeKind: "router",
      basementLabel: "Basement / aggr switch",
      switchFloorOverrides: { SW01: 0, ...(explicit?.switchFloorOverrides ?? {}) },
    } satisfies BuildingProfile;
  }

  const knownUnits = building.knownUnits ?? [];
  const switches = building.buildingModel?.switches ?? [];
  const exactFloors = [...new Set(knownUnits.map((unit) => floorNumber(unit)).filter((floor) => floor > 0))].sort((a, b) => a - b);
  const inferredFloors = explicit?.authoritativeFloors
    ?? building.buildingModel?.floors_inferred_from_units
    ?? inferFloorsFromUnits(knownUnits);
  const livePortCount = building.buildingCustomerCount?.access_port_count ?? building.buildingCustomerCount?.count ?? building.buildingModel?.live_port_pool.length ?? 0;
  const maxUnitsPerFloor = Math.max(unitsPerFloorEstimate(knownUnits), livePortCount > 0 ? Math.ceil(livePortCount / Math.max(inferredFloors, 1)) : 0);
  const massingType: BuildingProfile["massingType"] =
    explicit?.massingType
      ?? (inferredFloors >= 10 || maxUnitsPerFloor >= 11 ? "tower" : "midrise-slab");

  const switchFloorOverrides: Record<string, number> = { ...(explicit?.switchFloorOverrides ?? {}) };
  for (const entry of switches) {
    const label = entry.identity.split(".").slice(-1)[0];
    if (switchFloorOverrides[label] != null) continue;
    if (entry.served_floors?.length) {
      const floors = [...entry.served_floors].sort((a, b) => a - b);
      switchFloorOverrides[label] = floors[Math.floor(floors.length / 2)] ?? floors[0] ?? 1;
    }
  }

  return {
    massingType,
    authoritativeFloors: explicit?.authoritativeFloors ?? Math.max(inferredFloors, building.floors || 1),
    roofNodeLabel: explicit?.roofNodeLabel ?? (massingType === "tower" ? "R01" : "RFSW01"),
    roofNodeKind: explicit?.roofNodeKind ?? (massingType === "tower" ? "router" : "roof-switch"),
    basementLabel: explicit?.basementLabel ?? "Ground / service level",
    switchFloorOverrides,
  } satisfies BuildingProfile;
}

function hasHydratedBuildingData(live?: {
  buildingHealth?: BuildingHealth;
  buildingCustomerCount?: BuildingCustomerCount;
  buildingModel?: BuildingModel;
}) {
  return Boolean(live?.buildingModel || live?.buildingHealth || live?.buildingCustomerCount);
}

function evidenceBackedOnlineUnitCount(building: Pick<BuildingLive, "buildingModel"> | null | undefined) {
  return (building?.buildingModel?.unit_state_decisions ?? []).filter((row) => row.state === "online").length;
}

function humanStatus(status: Status) {
  return status === "unknown" ? "unverified" : status;
}

function siteIdForAddress(address: string) {
  return `site:${radioIdFromName(address)}`;
}

function inferCambiumRole(name: string, model: string) {
  const joined = `${name} ${model}`.toLowerCase();
  if (joined.includes("v5000")) {
    if (joined.includes("1145 lenox") || joined.includes("725 howard")) return "POP";
    if (joined.includes("728 e new york")) return "DN/POP candidate";
    return "Distribution node";
  }
  if (joined.includes("v3000")) return "Client node";
  if (joined.includes("v2000")) return "Client node";
  if (joined.includes("v1000")) return "Client node";
  return "Radio";
}

function degradeStatus(a: Status, b: Status): Status {
  const rank: Record<Status, number> = {
    online: 0,
    degraded: 1,
    offline: 2,
    unknown: 1,
  };
  return rank[a] >= rank[b] ? a : b;
}

function interpolateLinePosition(coordinates: number[][], progress: number): [number, number] {
  if (coordinates.length <= 1) {
    const point = coordinates[0] ?? [-73.924, 40.666];
    return [point[0], point[1]];
  }

  const clamped = ((progress % 1) + 1) % 1;
  const segmentCount = coordinates.length - 1;
  const scaled = clamped * segmentCount;
  const index = Math.min(segmentCount - 1, Math.floor(scaled));
  const local = scaled - index;
  const start = coordinates[index];
  const end = coordinates[index + 1];
  return [
    start[0] + (end[0] - start[0]) * local,
    start[1] + (end[1] - start[1]) * local,
  ];
}

function arcLineCoordinates(start: [number, number], end: [number, number]): [number, number][] {
  const dx = end[0] - start[0];
  const dy = end[1] - start[1];
  const distance = Math.hypot(dx, dy);
  if (!Number.isFinite(distance) || distance === 0) return [start, end];
  // Short rooftop hops look wrong as arcs; render them as straight segments.
  if (distance < 0.0007) return [start, end];
  const normalX = -dy / distance;
  const normalY = dx / distance;
  const offset = Math.min(distance * 0.65, 0.001);
  const midpoint: [number, number] = [
    (start[0] + end[0]) / 2 + normalX * offset,
    (start[1] + end[1]) / 2 + normalY * offset,
  ];
  return [start, midpoint, end];
}

function alertText(alert: JakeAlert) {
  return [
    alert.annotations?.summary,
    alert.annotations?.description,
    alert.annotations?.location,
    alert.annotations?.device_name,
    alert.labels?.name,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function buildingKeywords(building: BuildingLayout) {
  const canonicalId = canonicalBuildingIdOf(building);
  const lower = `${building.name} ${building.shortLabel} ${building.address}`.toLowerCase();
  const tokens = [lower, building.id.toLowerCase(), canonicalId.toLowerCase()];
  if (canonicalId === "000007.055") tokens.push("728 e new york", "fenimore", "192.168.44.2");
  if (canonicalId === "000007.058") tokens.push("955 rutland");
  if (canonicalId === "000007.004") tokens.push("1145 lenox");
  if (canonicalId === "000007.053") tokens.push("725 howard");
  return tokens;
}

function filterAlertsForBuilding(building: BuildingLayout, alerts: JakeAlert[]) {
  const needles = buildingKeywords(building);
  return alerts.filter((alert) => {
    const text = alertText(alert);
    return needles.some((needle) => text.includes(needle));
  });
}

function filterAlertsForRadio(radio: RadioLayout, alerts: JakeAlert[]) {
  const lowerName = radio.name.toLowerCase();
  const lowerShort = radio.shortLabel.toLowerCase();
  const lowerAddress = radio.address.toLowerCase();
  return alerts.filter((alert) => {
    const text = alertText(alert);
    return text.includes(lowerName) || text.includes(lowerShort) || text.includes(lowerAddress);
  });
}

function radioIdFromName(name: string) {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function shortRadioLabel(name: string) {
  return name
    .replace(/\b(avenue|ave|street|st|road|rd|place|pl)\b/gi, "")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .slice(0, 3)
    .join(" ");
}

function radioStatusFromJake(status: string, alertCount: number): Status {
  if (alertCount > 0) return "offline";
  if (status === "ok") return "online";
  if (status === "auth_failed" || status === "missing_ip" || status === "login_error") return "degraded";
  return "unknown";
}

function getCoord(address: string, siteTopology?: SiteTopology | null, buildingId?: string | null) {
  if (siteTopology) {
    const radioCoord = (siteTopology.radios ?? []).find((radio) => radio.location === address && radio.latitude != null && radio.longitude != null);
    if (radioCoord?.latitude != null && radioCoord.longitude != null) {
      return { lat: radioCoord.latitude, lon: radioCoord.longitude };
    }
    const addressCoord = (siteTopology.addresses ?? []).find((entry) => entry.address === address && entry.latitude != null && entry.longitude != null);
    if (addressCoord?.latitude != null && addressCoord.longitude != null) {
      return { lat: addressCoord.latitude, lon: addressCoord.longitude };
    }
    if (buildingId && siteTopology.buildings?.length) {
      const buildingCoord = siteTopology.buildings.find((entry) => entry.building_id === buildingId && entry.latitude != null && entry.longitude != null);
      if (buildingCoord?.latitude != null && buildingCoord.longitude != null) {
        return { lat: buildingCoord.latitude, lon: buildingCoord.longitude };
      }
      const buildingRadioCoord = (siteTopology.radios ?? []).find(
        (radio) => radio.resolved_building_id === buildingId && radio.latitude != null && radio.longitude != null,
      );
      if (buildingRadioCoord?.latitude != null && buildingRadioCoord.longitude != null) {
        return { lat: buildingRadioCoord.latitude, lon: buildingRadioCoord.longitude };
      }
    }
  }
  const exactStatic = STATIC_SITE_COORDS[address];
  if (exactStatic) return exactStatic;

  const normalizedStatic = STATIC_SITE_COORDS_BY_NORMALIZED.get(normalizeAddressKey(address));
  if (normalizedStatic) return normalizedStatic;

  const stemStatic = STATIC_SITE_COORDS_BY_STEM.get(addressStemKey(address));
  if (stemStatic) return stemStatic;

  if (buildingId) {
    const legacy = LEGACY_LAYOUT_BY_SOURCE_ID.get(buildingId);
    if (legacy) {
      const legacyExact = STATIC_SITE_COORDS[legacy.address];
      if (legacyExact) return legacyExact;
      const legacyNormalized = STATIC_SITE_COORDS_BY_NORMALIZED.get(normalizeAddressKey(legacy.address));
      if (legacyNormalized) return legacyNormalized;
      const legacyStem = STATIC_SITE_COORDS_BY_STEM.get(addressStemKey(legacy.address));
      if (legacyStem) return legacyStem;
    }
  }

  return null;
}

function radioCoord(radio: Pick<RadioLive, "latitude" | "longitude" | "address" | "anchorBuildingId">, siteTopology?: SiteTopology | null) {
  if (radio.latitude != null && radio.longitude != null) {
    return { lat: radio.latitude, lon: radio.longitude };
  }
  return getCoord(radio.address, siteTopology, radio.anchorBuildingId);
}

function distanceBetweenCoords(
  a?: { lat: number; lon: number } | null,
  b?: { lat: number; lon: number } | null,
) {
  if (!a || !b) return Number.POSITIVE_INFINITY;
  const dLat = a.lat - b.lat;
  const dLon = a.lon - b.lon;
  return Math.sqrt(dLat * dLat + dLon * dLon);
}

function colorForStatus(status: Status) {
  return STATUS_COLOR[status];
}

function deriveBuildingStatus(building: BuildingLayout, alertCount: number, outlierCount: number, flapCount: number, rogueCount: number, recoveryCount: number) {
  if (alertCount > 0) return "offline";
  if (rogueCount > 0 || recoveryCount > 0 || outlierCount > 0 || flapCount > 0) return "degraded";
  if (building.id) return "online";
  return "unknown";
}

async function fetchJakeJson<T>(baseUrl: string, path: string, attempts = 3): Promise<T> {
  let lastError: Error | null = null;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const response = await fetch(`${baseUrl}${path}`);
      if (!response.ok) {
        throw new Error(`${path} failed with ${response.status}`);
      }
      return response.json() as Promise<T>;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt < attempts - 1) {
        await new Promise((resolve) => window.setTimeout(resolve, 400 * (attempt + 1)));
        continue;
      }
    }
  }
  throw lastError ?? new Error(`${path} failed`);
}

function Pulse({ x, y, color }: { x: number; y: number; color: string }) {
  return (
    <g>
      <circle cx={x} cy={y} r={6} fill={color} opacity={0.9} />
      <circle cx={x} cy={y} r={10} fill="none" stroke={color} strokeWidth={1} opacity={0.35}>
        <animate attributeName="r" from="6" to="18" dur="2s" repeatCount="indefinite" />
        <animate attributeName="opacity" from="0.5" to="0" dur="2s" repeatCount="indefinite" />
      </circle>
    </g>
  );
}

function RadioGlyph({ x, y, color }: { x: number; y: number; color: string }) {
  return (
    <g>
      <circle cx={x} cy={y} r={10} fill="#020617" stroke={color} strokeWidth={1.6} />
      <circle cx={x} cy={y} r={3} fill={color} />
      <path d={`M ${x - 13} ${y} Q ${x} ${y - 12} ${x + 13} ${y}`} fill="none" stroke={color} strokeWidth={1.1} opacity={0.8} />
      <path d={`M ${x - 9} ${y} Q ${x} ${y - 7} ${x + 9} ${y}`} fill="none" stroke={color} strokeWidth={1.1} opacity={0.8} />
    </g>
  );
}

function StatBar({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <div style={{ marginBottom: 8 }}>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, color: "#64748b", marginBottom: 3 }}>
        <span>{label}</span>
        <span style={{ color }}>{value}%</span>
      </div>
      <div style={{ height: 4, background: "#1e293b", borderRadius: 2 }}>
        <div style={{ height: "100%", width: `${value}%`, background: color, borderRadius: 2, transition: "width 0.8s" }} />
      </div>
    </div>
  );
}

function unitFloor(unit: string) {
  const match = unit.match(/^(\d+)/);
  return match ? Number.parseInt(match[1], 10) : 1;
}

function unitStatusColor(unit: string, buildingStatus: Status) {
  const seed = unit.split("").reduce((acc, char) => acc + char.charCodeAt(0), 0);
  if (buildingStatus === "offline") return STATUS_COLOR.offline;
  if (buildingStatus === "degraded") return seed % 5 === 0 ? STATUS_COLOR.offline : seed % 3 === 0 ? STATUS_COLOR.degraded : STATUS_COLOR.online;
  if (buildingStatus === "unknown") return STATUS_COLOR.unknown;
  return seed % 9 === 0 ? STATUS_COLOR.degraded : STATUS_COLOR.online;
}

function synthesizeUnitLabels(count: number, floors: number) {
  const labels: string[] = [];
  const perFloor = Math.max(1, Math.ceil(count / Math.max(floors, 1)));
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  for (let floor = 1; floor <= floors; floor += 1) {
    for (let slot = 0; slot < perFloor && labels.length < count; slot += 1) {
      labels.push(`${String(floor).padStart(2, "0")}${alphabet[slot] ?? `${slot + 1}`}`);
    }
  }
  return labels;
}

function displayFloorCount(building: BuildingLive) {
  if (building.profile?.authoritativeFloors) return building.profile.authoritativeFloors;
  if (canonicalBuildingIdOf(building) === "000007.055") return 20;
  return Math.max(building.floors, 1);
}

function towerResidentialTemplate() {
  const labels: string[] = [];
  const letters = "ABCDEFGHIJKLM";
  for (let floor = 2; floor <= 20; floor += 1) {
    for (const letter of letters) {
      labels.push(`${String(floor).padStart(2, "0")}${letter}`);
    }
  }
  return labels;
}

function unitSuffixLetter(unit: string) {
  const match = unit.toUpperCase().match(/([A-Z])$/);
  return match?.[1] ?? null;
}

function floorNumber(unit: string) {
  const match = unit.match(/^(\d+)/);
  return match ? Number(match[1]) : 0;
}

function unitFaceColor(unit: Pick<UnitBox, "status" | "port" | "inferred">) {
  if (unit.status === "online" && !unit.port && unit.inferred) return "#60a5fa";
  return STATUS_COLOR[unit.status];
}

function unitStatusLabel(unit: Pick<UnitBox, "status" | "port" | "inferred">) {
  if (unit.status === "online" && !unit.port && unit.inferred) return "inferred online";
  return humanStatus(unit.status);
}

function cpeContextKey(buildingId: string | null | undefined, mac: string | null | undefined) {
  return `${String(buildingId ?? "").trim()}:${normalizeMac(mac)}`;
}

function findViloRowByMac(building: BuildingLive, mac: string | null | undefined) {
  const normalized = normalizeMac(mac);
  if (!normalized) return null;
  return building.cpeIntelligence?.vilo.rows.find((row) => normalizeMac(row.device_mac ?? "") === normalized) ?? null;
}

function findTaucRowByMac(building: BuildingLive, mac: string | null | undefined) {
  const normalized = normalizeMac(mac);
  if (!normalized) return null;
  return building.cpeIntelligence?.tauc.rows.find((row) => normalizeMac(row.mac ?? "") === normalized) ?? null;
}

function formatUptime(value: string | number | null | undefined) {
  if (value == null || value === "") return "Unknown";
  if (typeof value === "number") return `${value}`;
  return String(value);
}

function vendorLabel(vendor: string | null | undefined) {
  switch ((vendor ?? "").toLowerCase()) {
    case "vilo":
      return "Vilo";
    case "tplink":
      return "TP-Link";
    case "unknown":
      return "Unknown";
    default:
      return vendor ? vendor : "Unknown";
  }
}

function renderValue(value: ReactNode) {
  return <div style={{ fontSize: 12, color: "#e2e8f0", fontWeight: 600, marginTop: 2 }}>{value}</div>;
}

function InfoGrid({ items, columns = 2 }: { items: Array<{ label: string; value: ReactNode }>; columns?: number }) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))`, gap: 8 }}>
      {items.map((item) => (
        <div key={item.label} style={{ background: "#0a0f1a", borderRadius: 8, padding: 10, border: "1px solid #0f172a" }}>
          <div style={{ fontSize: 9, color: "#475569", marginBottom: 3 }}>{item.label}</div>
          {renderValue(item.value)}
        </div>
      ))}
    </div>
  );
}

function CpeContextPanel({
  vendor,
  vilo,
  tauc,
  compact = false,
}: {
  vendor: string;
  vilo?: {
    classification?: string;
    inventory_status?: string;
    device_sn?: string;
    subscriber_id?: string;
    subscriber?: { subscriber_id?: string; first_name?: string; last_name?: string; email?: string } | null;
    subscriber_hint?: { source?: string; label?: string; display?: string } | null;
    network?: CpeNetworkContext | null;
    sighting?: { identity?: string; on_interface?: string; port_status?: string; building_id?: string } | null;
    error?: string;
  } | null;
  tauc?: {
    network_name?: string;
    site_id?: string;
    expected_prefix?: string;
    wan_mode?: string;
    mesh_nodes?: number;
    sn?: string;
  } | null;
  compact?: boolean;
}) {
  const title = compact ? "CPE CONTEXT" : "LIVE CPE CONTEXT";
  const boxStyle = { background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 12 } as const;

  if (vilo?.error) {
    return (
      <div style={boxStyle}>
        <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>{title}</div>
        <div style={{ fontSize: 11, color: "#fca5a5" }}>Vilo lookup failed: {vilo.error}</div>
      </div>
    );
  }

  if (vilo) {
    const network = vilo.network;
    const subscriberName = [vilo.subscriber?.first_name, vilo.subscriber?.last_name].filter(Boolean).join(" ").trim();
    return (
      <div style={boxStyle}>
        <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>{title}</div>
        <div style={{ fontSize: 13, color: "#e2e8f0", fontWeight: 700 }}>{network?.network_name ?? "Vilo network"}</div>
        <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 4 }}>
          {vendorLabel(vendor)} · {network?.network_status ?? vilo.inventory_status ?? "unknown"}
        </div>
        <div style={{ marginTop: 10 }}>
          <InfoGrid
            columns={compact ? 1 : 2}
            items={[
              { label: "Uptime", value: formatUptime(network?.uptime) },
              { label: "Clients Online", value: String(network?.device_online_num ?? 0) },
              { label: "Clients Offline", value: String(network?.device_offline_num ?? 0) },
              { label: "Firmware", value: network?.firmware_version ?? "Unknown" },
              { label: "WAN IP", value: network?.wan_ip_address ?? "Unknown" },
              { label: "Public IP", value: network?.public_ip_address ?? "Unknown" },
              { label: "Installer", value: network?.installer ?? "Unknown" },
              { label: "Subscriber", value: subscriberName || vilo.subscriber_hint?.display || vilo.subscriber_id || "Unknown" },
            ]}
          />
        </div>
        {network?.flags?.length ? (
          <div style={{ display: "grid", gap: 6, marginTop: 10 }}>
            {network.flags.map((flag) => (
              <div key={flag} style={{ fontSize: 10, color: "#fbbf24", padding: "7px 9px", borderRadius: 8, border: "1px solid #78350f", background: "#1c1917" }}>
                {flag}
              </div>
            ))}
          </div>
        ) : null}
      </div>
    );
  }

  if (tauc) {
    return (
      <div style={boxStyle}>
        <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>{title}</div>
        <div style={{ fontSize: 13, color: "#e2e8f0", fontWeight: 700 }}>{tauc.network_name ?? "TP-Link network"}</div>
        <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 4 }}>{vendorLabel(vendor)} · ACS-backed context</div>
        <div style={{ marginTop: 10 }}>
          <InfoGrid
            columns={compact ? 1 : 2}
            items={[
              { label: "WAN Mode", value: tauc.wan_mode ?? "Unknown" },
              { label: "Mesh Nodes", value: String(tauc.mesh_nodes ?? 0) },
              { label: "Serial", value: tauc.sn ?? "Unknown" },
              { label: "Expected Prefix", value: tauc.expected_prefix ?? "Unknown" },
            ]}
          />
        </div>
      </div>
    );
  }

  return (
    <div style={boxStyle}>
      <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>{title}</div>
      <div style={{ fontSize: 11, color: "#64748b" }}>No cloud-side Vilo or TAUC context is currently available for this CPE.</div>
    </div>
  );
}

function UnitPrismButton({ unit, selected, onSelectUnit, onInspectPort, compact = false }: UnitPrismProps) {
  const face = unitFaceColor(unit);
  const topFill = `${face}80`;
  const sideFill = `${face}66`;
  const edge = selected ? "#f8fafc" : "rgba(148,163,184,0.72)";
  const frontTop = compact ? 6 : 7;
  const sideWidth = compact ? 5 : 6;
  const topHeight = compact ? 6 : 7;
  return (
    <div style={{ position: "relative", height: compact ? 34 : 38 }}>
      <div
        style={{
          position: "absolute",
          left: compact ? 5 : 6,
          right: sideWidth,
          top: 0,
          height: topHeight,
          clipPath: "polygon(10% 0, 100% 0, 90% 100%, 0 100%)",
          background: `linear-gradient(180deg, rgba(255,255,255,0.18), ${topFill})`,
          border: `1px solid ${edge}`,
          opacity: 0.95,
        }}
      />
      <div
        style={{
          position: "absolute",
          right: 0,
          top: frontTop - 1,
          bottom: 1,
          width: sideWidth,
          clipPath: "polygon(0 0, 100% 12%, 100% 88%, 0 100%)",
          background: `linear-gradient(180deg, ${sideFill}, rgba(15,23,42,0.88))`,
          border: `1px solid ${edge}`,
          opacity: 0.95,
        }}
      />
      <button
        onMouseEnter={() => onSelectUnit(unit)}
        onFocus={() => onSelectUnit(unit)}
        onClick={() => {
          onSelectUnit(unit);
          if (unit.port) onInspectPort(unit.port);
        }}
        title={unit.port ? `${unit.unit} · ${unit.port.mac}` : `${unit.unit} · no live CPE mapped`}
        style={{
          position: "absolute",
          left: 0,
          right: sideWidth,
          top: frontTop,
          bottom: 0,
          cursor: "pointer",
          border: `1px solid ${edge}`,
          clipPath: "polygon(8% 0, 100% 0, 92% 100%, 0 100%)",
          background: `linear-gradient(180deg, ${face}f0 0%, rgba(15,23,42,0.94) 84%)`,
          color: "#f8fafc",
          fontSize: compact ? 9 : 10,
          fontWeight: 800,
          padding: 0,
          boxShadow: selected ? "0 0 0 1px rgba(248,250,252,0.22) inset" : "none",
        }}
      >
        <span style={{ display: "inline-block", transform: "translateY(1px)" }}>{unit.unit}</span>
      </button>
    </div>
  );
}

function buildUnitBoxes(building: BuildingLive, ports: PortWithStatus[]) {
  const sortedPorts = [...ports].sort((a, b) => compareInterfaceLabels(a.on_interface, b.on_interface));
  const sortedKnownUnits = [...building.knownUnits].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  const inferredFloorCount = Math.max(
    displayFloorCount(building),
    building.buildingModel?.floors_inferred_from_units ?? 0,
    1,
  );
  const proxyUnitCount = Math.max(
    sortedPorts.length,
    building.buildingCustomerCount?.access_port_count ?? 0,
    building.buildingCustomerCount?.count ?? 0,
    building.buildingModel?.live_port_pool.length ?? 0,
    building.buildingModel?.coverage?.known_unit_count ?? 0,
    building.buildingModel?.coverage?.live_port_pool_count ?? 0,
    building.buildingHealth?.probable_cpe_count ?? 0,
    8,
  );
  const hasAuthoritativeUnitInventory = Boolean(
    sortedKnownUnits.length
    || (building.buildingModel?.exact_unit_port_matches?.length ?? 0)
    || (building.buildingModel?.coverage?.known_unit_count ?? 0),
  );
  const templateLabels = canonicalBuildingIdOf(building) === "000007.055"
    ? (hasAuthoritativeUnitInventory
        ? (sortedKnownUnits.length ? sortedKnownUnits : towerResidentialTemplate())
        : [])
    : sortedKnownUnits.length
      ? sortedKnownUnits
      : synthesizeUnitLabels(proxyUnitCount, inferredFloorCount);
  const explicitPortByUnit = new Map<string, PortWithStatus>();
  const matchedPortKeys = new Set<string>();
  const accessSwitches = (building.buildingModel?.switches ?? []).map((entry) => entry.identity);
  const switchAnchors = new Map<string, number>();
  for (const identity of accessSwitches) {
    const label = identity.split(".").slice(-1)[0];
    const override = building.profile?.switchFloorOverrides?.[label];
    const modeled = building.buildingModel?.switches.find((entry) => entry.identity === identity);
    if (typeof override === "number") {
      switchAnchors.set(identity, override);
      continue;
    }
    if (modeled?.served_floors?.length) {
      const floors = [...modeled.served_floors].sort((a, b) => a - b);
      switchAnchors.set(identity, floors[Math.floor(floors.length / 2)] ?? floors[0] ?? 1);
    }
  }
  const sortedSwitchAnchors = [...switchAnchors.entries()].sort((a, b) => a[1] - b[1] || a[0].localeCompare(b[0]));
  const assignedUnits = new Set<string>();

  for (const match of building.buildingModel?.exact_unit_port_matches ?? []) {
    const port = sortedPorts.find((entry) => entry.identity === match.switch_identity && ifaceLabel(entry.on_interface) === ifaceLabel(match.interface));
    if (!port) continue;
    explicitPortByUnit.set(match.unit, port);
    matchedPortKeys.add(portRenderKey(port));
    assignedUnits.add(match.unit);
  }

  function nearestSwitchForFloor(floor: number) {
    if (!sortedSwitchAnchors.length) return null;
    return [...sortedSwitchAnchors].sort((a, b) => Math.abs(a[1] - floor) - Math.abs(b[1] - floor) || a[1] - b[1])[0]?.[0] ?? null;
  }

  const remainingPortsBySwitch = new Map<string, PortWithStatus[]>();
  for (const port of sortedPorts) {
    if (matchedPortKeys.has(portRenderKey(port))) continue;
    const row = remainingPortsBySwitch.get(port.identity) ?? [];
    row.push(port);
    remainingPortsBySwitch.set(port.identity, row);
  }

  for (const [switchIdentity, switchPorts] of remainingPortsBySwitch) {
    const modeled = building.buildingModel?.switches.find((entry) => entry.identity === switchIdentity);
    const modeledFloors = new Set((modeled?.served_floors?.length ? modeled.served_floors : templateLabels.map((unit) => floorNumber(unit)).filter((floor) => nearestSwitchForFloor(floor) === switchIdentity)));
    const candidateUnits = templateLabels
      .filter((unit) => !assignedUnits.has(unit))
      .filter((unit) => modeledFloors.size ? modeledFloors.has(floorNumber(unit)) : true)
      .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
    switchPorts.forEach((port, index) => {
      const unit = candidateUnits[index];
      if (!unit) return;
      explicitPortByUnit.set(unit, port);
      assignedUnits.add(unit);
    });
  }

  const unitDecisionMap = new Map((building.buildingModel?.unit_state_decisions ?? []).map((row) => [row.unit, row]));

  return templateLabels.map((unit, index) => {
    const isInferred = !sortedKnownUnits.length || !explicitPortByUnit.has(unit);
    const port = explicitPortByUnit.get(unit) ?? (!sortedKnownUnits.length ? (sortedPorts[index] ?? null) : null);
    const decision = unitDecisionMap.get(unit);
    return {
      unit,
      floor: unitFloor(unit),
      port,
      inferred: isInferred,
      status: (isInferred && !sortedKnownUnits.length) ? "unknown" : port?.status ?? (decision?.state as Status | undefined) ?? (
        sortedKnownUnits.includes(unit)
          ? "unknown"
          : building.status === "offline"
            ? "offline"
            : building.status === "degraded"
              ? "degraded"
              : "unknown"
      ),
    } satisfies UnitBox;
  });
}

function inferSwitchFloor(device: BuildingHealth["devices"][number], building: BuildingLive, units: UnitBox[]) {
  const label = device.identity.split(".").slice(-1)[0];
  const profileFloor = building.profile?.switchFloorOverrides?.[label];
  if (typeof profileFloor === "number") return profileFloor;
  if (canonicalBuildingIdOf(building) === "000007.055" && label === "SW01") return 0;
  const modeledSwitch = building.buildingModel?.switches.find((entry) => entry.identity === device.identity);
  if (modeledSwitch?.served_floors?.length) {
    const modeledFloors = [...modeledSwitch.served_floors].sort((a, b) => a - b);
    return modeledFloors[Math.floor(modeledFloors.length / 2)] ?? modeledFloors[0] ?? 1;
  }
  const servedFloors = units
    .filter((unit) => unit.port?.identity === device.identity)
    .map((unit) => unit.floor)
    .filter((floor) => Number.isFinite(floor));
  if (!servedFloors.length) return 1;
  servedFloors.sort((a, b) => a - b);
  return servedFloors[Math.floor(servedFloors.length / 2)] ?? servedFloors[0] ?? 1;
}

function deviceKind(identity: string) {
  if (identity.includes(".R01")) return "router" as const;
  if (identity.includes(".RFSW")) return "roof-switch" as const;
  if (identity.includes(".SW")) return "switch" as const;
  return "external" as const;
}

function humanizeIdentity(identity: string) {
  const parts = identity.split(".");
  return parts[parts.length - 1] || identity;
}

function buildTopologyGraph(building: BuildingLive, devices: BuildingHealth["devices"], selectedRadio: RadioLive | null) {
  const nodes = new Map<string, TopologyNode>();
  const edges: TopologyEdge[] = [];
  const edgeSeen = new Set<string>();
  const addEdge = (from: string, to: string, label?: string, inferred?: boolean) => {
    const key = `${from}|${to}|${label ?? ""}`;
    if (edgeSeen.has(key)) return;
    edgeSeen.add(key);
    edges.push({ from, to, label, inferred });
  };

  for (const device of devices) {
    nodes.set(device.identity, {
      id: device.identity,
      label: humanizeIdentity(device.identity),
      kind: deviceKind(device.identity),
      status: "online",
      detail: device.ip,
    });
  }

  for (const radio of building.buildingModel?.radios ?? []) {
    nodes.set(radio.name, {
      id: radio.name,
      label: radio.name,
      kind: "radio",
      status: radioStatusFromJake(radio.status, 0),
      detail: `${radio.model} · ${radio.type}`,
    });
  }

  for (const edge of building.buildingModel?.direct_neighbor_edges ?? []) {
    const fromNode = nodes.get(edge.from_identity);
    if (!fromNode) {
      nodes.set(edge.from_identity, {
        id: edge.from_identity,
        label: humanizeIdentity(edge.from_identity),
        kind: deviceKind(edge.from_identity),
        status: "online",
      });
    }
    if (!nodes.has(edge.to_identity)) {
      nodes.set(edge.to_identity, {
        id: edge.to_identity,
        label: edge.to_identity.includes(".") ? humanizeIdentity(edge.to_identity) : edge.to_identity,
        kind: edge.platform === "Cambium" ? "radio" : deviceKind(edge.to_identity),
        status: edge.platform === "Cambium" ? "online" : "unknown",
        detail: edge.platform || edge.neighbor_address,
      });
    }
    addEdge(edge.from_identity, edge.to_identity, edge.from_interface);
  }

  const selectedRadioName = selectedRadio?.name ?? building.buildingModel?.radios?.[0]?.name ?? null;
  const rootInfra =
    devices.find((device) => device.identity.includes(".RFSW"))?.identity
    ?? devices.find((device) => device.identity.includes(".R01"))?.identity
    ?? devices.find((device) => device.identity.endsWith(".SW01"))?.identity
    ?? devices[0]?.identity
    ?? null;

  if (selectedRadioName && !nodes.has(selectedRadioName)) {
    nodes.set(selectedRadioName, {
      id: selectedRadioName,
      label: selectedRadioName,
      kind: "radio",
      status: selectedRadio?.status ?? "unknown",
      detail: selectedRadio ? `${selectedRadio.model} · ${selectedRadio.role}` : undefined,
    });
  }

  if (selectedRadioName) {
    const cambiumNeighbor = (building.buildingModel?.direct_neighbor_edges ?? []).find((edge) =>
      edge.platform === "Cambium" && normalizeSiteToken(edge.to_identity) === normalizeSiteToken(selectedRadioName),
    );
    if (cambiumNeighbor) {
      addEdge(selectedRadioName, cambiumNeighbor.from_identity, cambiumNeighbor.from_interface, true);
    } else if (rootInfra) {
      addEdge(selectedRadioName, rootInfra, "uplink", true);
    }
  }

  const rootId = selectedRadioName ?? rootInfra;
  if (!rootId || !nodes.has(rootId)) return null;

  const children = new Map<string, string[]>();
  for (const edge of edges) {
    const row = children.get(edge.from) ?? [];
    if (!row.includes(edge.to)) row.push(edge.to);
    children.set(edge.from, row);
  }

  const levels: string[][] = [];
  const visited = new Set<string>();
  let frontier = [rootId];
  while (frontier.length && levels.length < 6) {
    const unique = frontier.filter((id, index) => frontier.indexOf(id) === index && !visited.has(id));
    if (!unique.length) break;
    unique.forEach((id) => visited.add(id));
    levels.push(unique);
    frontier = unique.flatMap((id) => children.get(id) ?? []).filter((id) => !visited.has(id));
  }

  return { rootId, nodes, edges, levels };
}

function buildLocalSwitchChain(building: BuildingLive, devices: BuildingHealth["devices"]) {
  const localIds = new Set(
    [
      ...(building.buildingModel?.switches.map((entry) => entry.identity) ?? []),
      ...devices.map((entry) => entry.identity),
    ].filter((identity) => isSwitchLikeIdentity(identity)),
  );
  if (!localIds.size) return [];

  const adjacency = new Map<string, Set<string>>();
  for (const identity of localIds) adjacency.set(identity, new Set<string>());
  for (const edge of building.buildingModel?.direct_neighbor_edges ?? []) {
    if (!localIds.has(edge.from_identity) || !localIds.has(edge.to_identity)) continue;
    adjacency.get(edge.from_identity)?.add(edge.to_identity);
    adjacency.get(edge.to_identity)?.add(edge.from_identity);
  }

  const rank = (identity: string) => {
    if (identity.includes(".RFSW")) return 0;
    if (identity.includes(".R01")) return 1;
    const sw = identity.match(/\.SW(\d+)/i);
    if (sw) return 10 + Number(sw[1]);
    const ag = identity.match(/\.AG(\d+)/i);
    if (ag) return 100 + Number(ag[1]);
    return 1000;
  };

  const nodes = [...localIds].sort((a, b) => rank(a) - rank(b) || a.localeCompare(b));
  const root = nodes[0];
  const chain = [root];
  const visited = new Set(chain);
  let current = root;

  while (true) {
    const next = [...(adjacency.get(current) ?? [])]
      .filter((identity) => !visited.has(identity))
      .sort((a, b) => rank(a) - rank(b) || a.localeCompare(b))[0];
    if (!next) break;
    chain.push(next);
    visited.add(next);
    current = next;
  }

  for (const identity of nodes) {
    if (!visited.has(identity)) chain.push(identity);
  }
  return chain;
}

function parseAddressStem(address: string) {
  const base = shortAddressLabel(address).split(",")[0]?.trim() ?? address;
  const match = base.match(/^(\d+)\s+(.+)$/);
  return {
    number: match ? Number(match[1]) : null,
    street: (match ? match[2] : base).toLowerCase(),
  };
}

function compactPortsForBuilding(building: BuildingLive): PortWithStatus[] {
  return (building.buildingCustomerCount?.results ?? []).map((port) => ({
    ...port,
    status: "online" as const,
    statusLabel: "Healthy",
    notes: [],
  }));
}

function CompactBuildingThumbnail({
  building,
  activeBuildingId,
  onHoverCard,
  onOpenBuilding,
}: {
  building: BuildingLive;
  activeBuildingId: string;
  onHoverCard: (card: HoverCardData | null) => void;
  onOpenBuilding: (buildingId: string) => void;
}) {
  const isLinkedTarget = building.id !== activeBuildingId;
  const units = buildUnitBoxes(building, compactPortsForBuilding(building));
  const floors = [...new Set(units.map((unit) => unit.floor))].sort((a, b) => b - a).slice(0, 4);
  const grouped = new Map<number, UnitBox[]>();
  for (const unit of units) {
    const row = grouped.get(unit.floor) ?? [];
    row.push(unit);
    grouped.set(unit.floor, row);
  }
  const switchPills = (() => {
    const modeled = (building.buildingModel?.switches ?? []).filter((entry) => /\.SW|\.AG/.test(entry.identity));
    const count = Math.max(2, Math.min(2, modeled.length || 2));
    return Array.from({ length: count }, (_, index) => ({
      label: `SW${String(index + 1).padStart(2, "0")}`,
      switchIdentity: modeled[index]?.identity,
      ip: modeled[index]?.ip,
    }));
  })();
  return (
    <div
      onMouseEnter={() => onHoverCard({
        title: building.name,
        subtitle: building.address,
        facts: [
          { label: "Known Units", value: String(building.knownUnits.length) },
          { label: "Live Ports", value: String(building.customerCount) },
          { label: "Switches", value: String(building.buildingModel?.switches.length ?? 0) },
          { label: "MAC", value: "n/a" },
        ],
      })}
      onMouseLeave={() => onHoverCard(null)}
      style={{ border: "1px solid #1e293b", borderRadius: 10, background: "#0b1020", padding: 10 }}
    >
      <div style={{ display: "flex", justifyContent: "center", marginBottom: 8 }}>
        {isLinkedTarget ? (
          <button
            onClick={() => onOpenBuilding(building.id)}
            onMouseEnter={() => onHoverCard({
              title: building.name,
              subtitle: building.address,
              facts: [
                { label: "Known Units", value: String(building.knownUnits.length) },
                { label: "Live Ports", value: String(building.customerCount) },
                { label: "Switches", value: String(building.buildingModel?.switches.length ?? 0) },
                { label: "MAC", value: "n/a" },
              ],
            })}
            style={{
              border: "1px solid rgba(250,204,21,0.9)",
              borderRadius: 999,
              background: "rgba(120,53,15,0.22)",
              color: "#fcd34d",
              fontSize: 11,
              fontWeight: 800,
              padding: "6px 18px",
              cursor: "pointer",
              boxShadow: "0 0 0 1px rgba(250,204,21,0.18) inset, 0 0 18px rgba(250,204,21,0.18)",
            }}
          >
            {building.name}
          </button>
        ) : (
          <div style={{ color: "#e2e8f0", fontSize: 11, fontWeight: 800, letterSpacing: "0.02em" }}>{building.name}</div>
        )}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "24px 1fr", gap: 8, alignItems: "start" }}>
        <div style={{ display: "grid", gap: 12 }}>
          {floors.map((floor) => (
            <div key={floor} style={{ fontSize: 9, color: "#94a3b8", fontWeight: 700 }}>{String(floor).padStart(2, "0")}</div>
          ))}
        </div>
        <div style={{ border: "1px solid #475569", padding: 8, position: "relative", background: "#111827" }}>
          <div style={{ position: "absolute", inset: 0, borderTop: "1px solid #64748b", transform: "translate(10px, -10px) skewX(-35deg)", transformOrigin: "top left" }} />
          <div style={{ display: "grid", gap: 6 }}>
            {floors.map((floor) => (
              <div key={floor} style={{ display: "grid", gridTemplateColumns: "repeat(7, minmax(0, 1fr))", gap: 4 }}>
                {(grouped.get(floor) ?? []).slice(0, 7).map((unit) => (
                  <div
                    key={unit.unit}
                    onMouseEnter={() => onHoverCard({
                      title: `${building.name} · Unit ${unit.unit}`,
                      subtitle: unit.status,
                      facts: [
                        { label: "Switch", value: unit.port?.identity ?? "unmapped" },
                        { label: "Port", value: unit.port ? ifaceLabel(unit.port.on_interface) : "n/a" },
                        { label: "IP", value: unit.port?.ip || "n/a" },
                        { label: "MAC", value: unit.port?.mac || "n/a" },
                      ],
                    })}
                    onMouseLeave={() => onHoverCard(null)}
                    style={{ border: "1px solid #64748b", background: `${unitFaceColor(unit)}55`, color: "#f8fafc", fontSize: 8, fontWeight: 700, textAlign: "center", padding: "6px 0" }}
                  >
                    {unit.unit}
                  </div>
                ))}
              </div>
            ))}
          </div>
        </div>
      </div>
      <div style={{ display: "flex", justifyContent: "flex-end", gap: 6, marginTop: 8 }}>
        {switchPills.map((entry) => (
          <div
            key={entry.label}
            onMouseEnter={() => onHoverCard({
              title: `${building.name} · ${entry.label}`,
              subtitle: entry.switchIdentity ?? "Modeled switch",
              facts: [
                { label: "IP", value: entry.ip || "n/a" },
                { label: "MAC", value: "n/a" },
                { label: "Role", value: "Access switch" },
                { label: "Count", value: String(building.buildingModel?.switches.length ?? 0) },
              ],
            })}
            onMouseLeave={() => onHoverCard(null)}
            style={{ border: "1px solid #22c55e", borderRadius: 999, background: "#16331b", color: "#dcfce7", fontSize: 9, fontWeight: 700, padding: "5px 12px" }}
          >
            {entry.label}
          </div>
        ))}
      </div>
    </div>
  );
}

function CompoundSiteDiagram({
  building,
  allBuildings,
  radios,
  selectedRadio,
  onOpenBuilding,
}: {
  building: BuildingLive;
  allBuildings: BuildingLive[];
  radios: RadioLive[];
  selectedRadio: RadioLive | null;
  onOpenBuilding: (buildingId: string) => void;
}) {
  const [hoverCard, setHoverCard] = useState<HoverCardData | null>(null);
  const context = compoundSiteContext(building, allBuildings);
  const cluster = context?.cluster ?? [building];
  const clusterRoot = cluster[0];
  const agSwitch = context
    ? clusterRoot.buildingModel?.switches.find((entry) => entry.identity === context.rootSwitchIdentity)
      ?? clusterRoot.buildingHealth?.devices.find((entry) => entry.identity === context.rootSwitchIdentity)
    : null;
  const topologyRadio = radios.find((radio) => radio.anchorBuildingId === clusterRoot.id || radio.anchorBuildingId === canonicalBuildingIdOf(clusterRoot));
  const effectiveRadio = selectedRadio
    ? selectedRadio
    : clusterRoot.buildingModel?.radios?.[0]
      ? {
          id: radioIdFromName(clusterRoot.buildingModel.radios[0].name),
          name: clusterRoot.buildingModel.radios[0].name,
          shortLabel: shortRadioLabel(clusterRoot.buildingModel.radios[0].name),
          address: topologyRadio?.address ?? clusterRoot.address,
          ip: topologyRadio?.ip,
          model: topologyRadio?.model ?? clusterRoot.buildingModel.radios[0].model,
          role: topologyRadio?.role ?? clusterRoot.buildingModel.radios[0].type,
          anchorBuildingId: clusterRoot.id,
          x: 0,
          y: 0,
          status: topologyRadio?.status ?? radioStatusFromJake(clusterRoot.buildingModel.radios[0].status, 0),
          knownUnits: clusterRoot.knownUnits,
        }
      : topologyRadio
        ? topologyRadio
      : null;
  if (!effectiveRadio || !agSwitch || cluster.length < 2 || !context) return null;

  return (
    <div style={{ position: "relative", background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14, marginBottom: 16 }}>
      <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>COMPOUND SITE MODEL</div>
      {hoverCard ? (
        <div
          style={{
            position: "absolute",
            right: 14,
            top: 14,
            width: 240,
            zIndex: 8,
            borderRadius: 10,
            border: "1px solid #164e63",
            background: "rgba(2, 6, 23, 0.98)",
            boxShadow: "0 12px 40px rgba(2,6,23,0.45)",
            padding: 12,
          }}
        >
          <div style={{ fontSize: 12, color: "#e2e8f0", fontWeight: 800 }}>{hoverCard.title}</div>
          {hoverCard.subtitle ? <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 3 }}>{hoverCard.subtitle}</div> : null}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginTop: 10 }}>
            {hoverCard.facts.map((fact) => (
              <div key={`${hoverCard.title}-${fact.label}`}>
                <div style={{ fontSize: 9, color: "#475569" }}>{fact.label}</div>
                <div style={{ fontSize: 11, color: "#cbd5e1", marginTop: 2 }}>{fact.value}</div>
              </div>
            ))}
          </div>
        </div>
      ) : null}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 220px 1fr", gap: 14, alignItems: "center" }}>
        <div style={{ display: "grid", gap: 14 }}>
          {cluster.filter((_, index) => index % 2 === 0).map((entry) => <CompactBuildingThumbnail key={entry.id} building={entry} activeBuildingId={building.id} onHoverCard={setHoverCard} onOpenBuilding={onOpenBuilding} />)}
        </div>
          <div style={{ display: "grid", gap: 14, alignContent: "center", justifyItems: "center" }}>
          <div
            onMouseEnter={() => setHoverCard({
              title: effectiveRadio.name,
              subtitle: `${effectiveRadio.model} · ${effectiveRadio.status}`,
              facts: [
                { label: "IP", value: effectiveRadio.ip || "n/a" },
                { label: "MAC", value: "n/a" },
                { label: "Role", value: effectiveRadio.role },
                { label: "Address", value: effectiveRadio.address },
              ],
            })}
            onMouseLeave={() => setHoverCard(null)}
            style={{ border: "1px solid #22c55e", borderRadius: 999, background: "#16331b", color: "#dcfce7", fontSize: 12, fontWeight: 700, padding: "10px 18px" }}
          >
            radio
          </div>
          <div style={{ width: 3, height: 28, background: "#22c55e" }} />
          <div
            onMouseEnter={() => setHoverCard({
              title: agSwitch.identity,
              subtitle: context.mode === "agg-fed branch site" ? "Aggregation switch" : "Distribution switch",
              facts: [
                { label: "IP", value: agSwitch.ip || "n/a" },
                { label: "MAC", value: "n/a" },
                { label: "Downstream sites", value: String(context.downstreamCount) },
                { label: "Units", value: String(clusterRoot.knownUnits.length) },
              ],
            })}
            onMouseLeave={() => setHoverCard(null)}
            style={{ border: "1px solid #22c55e", borderRadius: 999, background: "#16331b", color: "#dcfce7", fontSize: 12, fontWeight: 700, padding: "10px 18px" }}
          >
            {humanizeIdentity(agSwitch.identity)}
          </div>
          <div style={{ width: "100%", height: 3, background: "#22c55e" }} />
          <div style={{ fontSize: 10, color: "#94a3b8", textAlign: "center" }}>
            {effectiveRadio.name}
            <br />
            {context.mode}
          </div>
        </div>
        <div style={{ display: "grid", gap: 14 }}>
          {cluster.filter((_, index) => index % 2 === 1).map((entry) => <CompactBuildingThumbnail key={entry.id} building={entry} activeBuildingId={building.id} onHoverCard={setHoverCard} onOpenBuilding={onOpenBuilding} />)}
        </div>
      </div>
    </div>
  );
}

function TopologyBranchPanel({
  building,
  devices,
  selectedRadio,
}: {
  building: BuildingLive;
  devices: BuildingHealth["devices"];
  selectedRadio: RadioLive | null;
}) {
  const graph = useMemo(() => buildTopologyGraph(building, devices, selectedRadio), [building, devices, selectedRadio]);
  if (!graph) return null;

  return (
    <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14, marginBottom: 16 }}>
      <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>NETWORK BRANCH MODEL</div>
      <div style={{ display: "grid", gap: 14 }}>
        {graph.levels.map((level, levelIndex) => (
          <div key={`level-${levelIndex}`} style={{ display: "grid", gridTemplateColumns: `repeat(${Math.max(level.length, 1)}, minmax(0, 1fr))`, gap: 10 }}>
            {level.map((nodeId) => {
              const node = graph.nodes.get(nodeId)!;
              const outgoing = graph.edges.filter((edge) => edge.from === nodeId);
              return (
                <div key={nodeId} style={{ display: "grid", gap: 8 }}>
                  <div
                    title={[node.label, node.kind, node.detail, `${outgoing.length} downstream links`].filter(Boolean).join(" · ")}
                    style={{
                      border: `1px solid ${STATUS_COLOR[node.status]}55`,
                      background: "#030712",
                      borderRadius: 10,
                      padding: "10px 12px",
                      minHeight: 68,
                    }}
                  >
                    <div style={{ fontSize: 10, color: "#475569", marginBottom: 4 }}>{node.kind.replace("-", " ")}</div>
                    <div style={{ fontSize: 12, color: "#e2e8f0", fontWeight: 700 }}>{node.label}</div>
                    {node.detail ? <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 4 }}>{node.detail}</div> : null}
                  </div>
                  {outgoing.length ? (
                    <div style={{ display: "grid", gap: 6 }}>
                      {outgoing.map((edge) => (
                        <div key={`${edge.from}-${edge.to}-${edge.label ?? ""}`} style={{ fontSize: 10, color: edge.inferred ? "#f59e0b" : "#38bdf8" }}>
                          ↓ {edge.label ? `${edge.label} → ` : ""}{graph.nodes.get(edge.to)?.label ?? edge.to}{edge.inferred ? " · inferred" : ""}
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        ))}
      </div>
    </div>
  );
}

function WireframeTwinView({
  building,
  units,
  selectedUnit,
  selectedUnitVilo,
  selectedUnitTauc,
  onSelectUnit,
  onInspectPort,
  onOpenDevice,
  selectedRadio,
  roofSwitches,
  accessSwitches,
  coreDevices,
}: {
  building: BuildingLive;
  units: UnitBox[];
  selectedUnit: UnitBox | null;
  selectedUnitVilo: BuildingCpeIntelligence["vilo"]["rows"][number] | null;
  selectedUnitTauc: BuildingCpeIntelligence["tauc"]["rows"][number] | null;
  onSelectUnit: (unit: UnitBox) => void;
  onInspectPort: (port: PortWithStatus) => void;
  onOpenDevice: (identity: string) => void;
  selectedRadio: RadioLive | null;
  roofSwitches: BuildingHealth["devices"];
  accessSwitches: BuildingHealth["devices"];
  coreDevices: BuildingHealth["devices"];
}) {
  const [hoveredSwitchId, setHoveredSwitchId] = useState<string | null>(null);
  const [selectedSwitchId, setSelectedSwitchId] = useState<string | null>(null);
  const grouped = new Map<number, UnitBox[]>();
  for (const unit of units) {
    const row = grouped.get(unit.floor) ?? [];
    row.push(unit);
    grouped.set(unit.floor, row);
  }
  const allFloors = new Set<number>(grouped.keys());
  const totalFloors = Math.max(displayFloorCount(building), ...allFloors, 1);
  const floors = Array.from({ length: totalFloors }, (_, index) => totalFloors - index);
  const isTower = building.profile?.massingType === "tower" || canonicalBuildingIdOf(building) === "000007.055";
  const hasUnitInventory = units.length > 0;
  const towerGridCols = hasUnitInventory ? 13 : 4;
  const maxCols = isTower ? towerGridCols : Math.max(...floors.map((floor) => (grouped.get(floor) ?? []).length), 1);
  const towerLetters = "ABCDEFGHIJKLM".split("");
  const router = coreDevices.find((device) => device.identity.endsWith(".R01")) ?? null;
  const roofLabel = isTower && router
    ? router.identity.split(".").slice(-1)[0]
    : building.profile?.roofNodeLabel ?? roofSwitches[0]?.identity.split(".").slice(-1)[0] ?? "RFSW01";
  const switchPlacements = accessSwitches
    .map((device) => ({
      device,
      label: device.identity.split(".").slice(-1)[0],
      floor: inferSwitchFloor(device, building, units),
      count: units.filter((unit) => unit.port?.identity === device.identity).length,
    }))
    .sort((a, b) => a.floor - b.floor || a.label.localeCompare(b.label));
  const switchChain = useMemo(
    () => buildLocalSwitchChain(building, [...coreDevices, ...roofSwitches, ...accessSwitches]),
    [accessSwitches, building, coreDevices, roofSwitches],
  );
  const displaySwitchStack = useMemo(() => {
    const accessByIdentity = new Map(accessSwitches.map((device) => [device.identity, device]));
    const ordered = switchChain
      .filter((identity) => accessByIdentity.has(identity))
      .map((identity) => accessByIdentity.get(identity)!)
      .filter(isPresent);
    const seen = new Set(ordered.map((device) => device.identity));
    const remainder = accessSwitches
      .filter((device) => !seen.has(device.identity))
      .sort((a, b) => humanizeIdentity(a.identity).localeCompare(humanizeIdentity(b.identity), undefined, { numeric: true }));
    return [...ordered, ...remainder].map((device, index) => ({
      device,
      label: device.identity.split(".").slice(-1)[0],
      stackIndex: index,
      count: units.filter((unit) => unit.port?.identity === device.identity).length,
    }));
  }, [accessSwitches, switchChain, units]);
  const switchesByFloor = new Map<number, typeof switchPlacements>();
  for (const placement of switchPlacements) {
    const row = switchesByFloor.get(placement.floor) ?? [];
    row.push(placement);
    switchesByFloor.set(placement.floor, row);
  }
  const switchDetailsByIdentity = useMemo(() => {
    const byIdentity = new Map<string, {
      identity: string;
      label: string;
      ip?: string;
      model?: string;
      version?: string;
      floor: number;
      count: number;
      servedFloors: number[];
      servedUnits: string[];
      directNeighborCount: number;
    }>();
    const modelSwitches = new Map((building.buildingModel?.switches ?? []).map((entry) => [entry.identity, entry]));
    for (const placement of switchPlacements) {
      const modelEntry = modelSwitches.get(placement.device.identity);
      byIdentity.set(placement.device.identity, {
        identity: placement.device.identity,
        label: placement.label,
        ip: placement.device.ip,
        model: placement.device.model || modelEntry?.model,
        version: placement.device.version || modelEntry?.version,
        floor: placement.floor,
        count: placement.count,
        servedFloors: modelEntry?.served_floors ?? [],
        servedUnits: modelEntry?.served_units ?? [],
        directNeighborCount: modelEntry?.direct_neighbors?.length ?? 0,
      });
    }
    return byIdentity;
  }, [building.buildingModel?.switches, switchPlacements]);
  const internalSwitchLinks = useMemo(() => {
    const chainNodes = [
      ...(roofSwitches[0] ? [roofSwitches[0].identity] : []),
      ...(isTower && router ? [router.identity] : []),
      ...displaySwitchStack.map((entry) => entry.device.identity),
    ];
    const ordered = Array.from(new Map(chainNodes.map((identity) => [identity, identity])).values());
    return ordered.slice(0, -1).map((identity, index) => ({
      from: identity,
      to: ordered[index + 1],
    }));
  }, [displaySwitchStack, isTower, roofSwitches, router]);
  const hoveredSwitch = (selectedSwitchId ?? hoveredSwitchId) ? switchDetailsByIdentity.get(selectedSwitchId ?? hoveredSwitchId ?? "") ?? null : null;
  const hasMappedUnits = units.some((unit) => unit.port);
  const genericCellWidth = 32;
  const genericCellHeight = 42;
  const genericCellGapX = 6;
  const genericCellGapY = 8;
  const genericDepthX = 18;
  const genericDepthY = 10;
  const genericUnitWidth = maxCols * genericCellWidth + Math.max(0, maxCols - 1) * genericCellGapX;
  const genericUnitHeight = floors.length * genericCellHeight + Math.max(0, floors.length - 1) * genericCellGapY;
  const genericFacadeLeft = 78;
  const genericFacadeTop = 104;
  const genericFacadeRight = genericFacadeLeft + genericUnitWidth + 28;
  const genericFacadeBottom = genericFacadeTop + genericUnitHeight + 30;
  const genericCanvasWidth = genericFacadeRight + genericDepthX + 180;
  const genericCanvasHeight = genericFacadeBottom + 54;
  const genericRoofLeft = genericFacadeLeft + 16;
  const genericRoofRight = genericFacadeRight - 8;
  const genericRoofFrontY = genericFacadeTop - 8;
  const genericSwitchRailX = genericFacadeRight + 34;
  const genericSwitchLabelX = genericSwitchRailX + 22;
  const genericFloorLabelX = genericFacadeLeft - 28;
  const genericRoofSwitchCenterX = genericFacadeLeft + (genericFacadeRight - genericFacadeLeft) / 2;
  const genericRoofSwitchCenterY = genericRoofFrontY - genericDepthY - 23;
  const genericSwitchCenters = new Map<string, { x: number; y: number }>();
  if (roofSwitches[0] || router) {
    genericSwitchCenters.set((isTower && router ? router.identity : roofSwitches[0]?.identity) ?? roofLabel, {
      x: genericRoofSwitchCenterX,
      y: genericRoofSwitchCenterY,
    });
  }
  displaySwitchStack.forEach((placement, index) => {
    const spacing = displaySwitchStack.length > 1 ? (genericFacadeBottom - genericFacadeTop - 80) / Math.max(displaySwitchStack.length - 1, 1) : 0;
    genericSwitchCenters.set(placement.device.identity, {
      x: genericSwitchLabelX + 32,
      y: genericFacadeTop + 40 + index * spacing,
    });
  });

  useEffect(() => {
    if (!selectedSwitchId) return;
    if (switchDetailsByIdentity.has(selectedSwitchId)) return;
    setSelectedSwitchId(null);
  }, [selectedSwitchId, switchDetailsByIdentity]);

  return (
    <div style={{ display: "grid", gridTemplateColumns: isTower ? "1.45fr 0.75fr" : "1.2fr 0.8fr", gap: 16 }}>
      <div style={{ overflow: "auto", maxHeight: 760, padding: "10px 6px 6px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "baseline", marginBottom: 12 }}>
          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569" }}>UNIT STACK</div>
          <div style={{ fontSize: 10, color: "#64748b" }}>
            {building.knownUnits.length ? "Verified unit labels" : "Port-derived unit proxy layout"}
          </div>
        </div>

        {switchChain.length > 1 ? (
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              flexWrap: "wrap",
              marginBottom: 12,
              padding: "10px 12px",
              borderRadius: 10,
              border: "1px solid #1e293b",
              background: "#030712",
            }}
          >
            <div style={{ fontSize: 10, letterSpacing: "0.08em", color: "#475569", marginRight: 6 }}>SWITCH CHAIN</div>
            {switchChain.map((identity, index) => (
              <Fragment key={identity}>
                <div
                  onMouseEnter={() => setHoveredSwitchId(identity)}
                  onMouseLeave={() => setHoveredSwitchId((current) => (current === identity ? null : current))}
                  onClick={() => setSelectedSwitchId((current) => (current === identity ? null : identity))}
                  style={{
                    padding: "6px 10px",
                    borderRadius: 999,
                    border: `1px solid ${selectedSwitchId === identity ? "#38bdf8" : "#22c55e"}`,
                    background: selectedSwitchId === identity ? "#082f49" : "#052e16",
                    color: "#dcfce7",
                    fontSize: 10,
                    fontWeight: 800,
                    cursor: "pointer",
                  }}
                >
                  {humanizeIdentity(identity)}
                </div>
                {index < switchChain.length - 1 ? <div style={{ color: "#22c55e", fontSize: 12, fontWeight: 800 }}>→</div> : null}
              </Fragment>
            ))}
          </div>
        ) : null}

        <div style={{ display: "flex", justifyContent: "center" }}>
          <div style={{ position: "relative", width: Math.max(isTower ? 620 : 520, maxCols * (isTower ? 34 : 42) + 210), paddingTop: isTower ? 76 : 64, paddingRight: isTower ? 18 : 14 }}>
            {(roofSwitches[0] || (isTower && router)) ? (
              <>
                <div
                  style={{
                    position: "absolute",
                    left: "50%",
                    top: 0,
                    transform: "translateX(-50%)",
                    minWidth: 110,
                    padding: "10px 18px",
                    borderRadius: 10,
                    border: "2px solid #38bdf8",
                    background: "rgba(8,47,73,0.92)",
                    color: "#bae6fd",
                    fontWeight: 800,
                    textAlign: "center",
                    boxShadow: "0 0 0 1px rgba(186,230,253,0.08) inset",
                  }}
                >
                  {roofLabel}
                </div>
                <div
                  style={{
                    position: "absolute",
                    left: "50%",
                    top: 46,
                    transform: "translateX(-50%)",
                    width: 5,
                    height: isTower ? 30 : 24,
                    background: "#38bdf8",
                    boxShadow: "0 0 20px rgba(56,189,248,0.35)",
                  }}
                />
              </>
            ) : null}

            {selectedRadio ? (
              <div
                style={{
                  position: "absolute",
                  right: 8,
                  top: isTower ? 36 : 28,
                  padding: "10px 14px",
                  borderRadius: 999,
                  border: `2px solid ${STATUS_COLOR[selectedRadio.status]}`,
                  background: "#082f49",
                  color: "#e2e8f0",
                  fontSize: 12,
                  fontWeight: 700,
                }}
              >
                {selectedRadio.shortLabel}
              </div>
            ) : null}

            {hoveredSwitch ? (
              <div
                style={{
                  position: "absolute",
                  right: isTower ? 8 : 20,
                  top: isTower ? 84 : 72,
                  width: 220,
                  zIndex: 5,
                  borderRadius: 10,
                  border: "1px solid #164e63",
                  background: "rgba(2, 6, 23, 0.96)",
                  boxShadow: "0 12px 40px rgba(2,6,23,0.4)",
                  padding: 12,
                }}
              >
                <div style={{ fontSize: 12, color: "#e2e8f0", fontWeight: 800 }}>{hoveredSwitch.identity}</div>
                <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 3 }}>
                  {hoveredSwitch.model ?? "Switch"}{hoveredSwitch.ip ? ` · ${hoveredSwitch.ip}` : ""}{hoveredSwitch.version ? ` · ${hoveredSwitch.version}` : ""}
                </div>
                {selectedSwitchId ? (
                  <button
                    onClick={() => setSelectedSwitchId(null)}
                    style={{
                      marginTop: 10,
                      borderRadius: 8,
                      border: "1px solid #164e63",
                      background: "#082f49",
                      color: "#bae6fd",
                      padding: "6px 10px",
                      cursor: "pointer",
                      fontSize: 10,
                      fontWeight: 700,
                    }}
                  >
                    Close switch detail
                  </button>
                ) : null}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginTop: 10 }}>
                  <div>
                    <div style={{ fontSize: 9, color: "#475569" }}>Placement</div>
                    <div style={{ fontSize: 11, color: "#cbd5e1", marginTop: 2 }}>{hoveredSwitch.floor === 0 ? "Basement" : `Floor ${String(hoveredSwitch.floor).padStart(2, "0")}`}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: 9, color: "#475569" }}>Mapped Units</div>
                    <div style={{ fontSize: 11, color: "#cbd5e1", marginTop: 2 }}>{hoveredSwitch.count}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: 9, color: "#475569" }}>Served Floors</div>
                    <div style={{ fontSize: 11, color: "#cbd5e1", marginTop: 2 }}>
                      {hoveredSwitch.servedFloors.length ? hoveredSwitch.servedFloors.map((floor) => String(floor).padStart(2, "0")).join(", ") : "Not pinned"}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: 9, color: "#475569" }}>Neighbors</div>
                    <div style={{ fontSize: 11, color: "#cbd5e1", marginTop: 2 }}>{hoveredSwitch.directNeighborCount}</div>
                  </div>
                </div>
                {hoveredSwitch.servedUnits.length ? (
                  <div style={{ marginTop: 10 }}>
                    <div style={{ fontSize: 9, color: "#475569", marginBottom: 4 }}>Sample Units</div>
                    <div style={{ fontSize: 10, color: "#94a3b8", lineHeight: 1.5 }}>
                      {hoveredSwitch.servedUnits.slice(0, 8).join(", ")}
                      {hoveredSwitch.servedUnits.length > 8 ? " ..." : ""}
                    </div>
                  </div>
                ) : null}
              </div>
            ) : null}

            {isTower ? (
              <div style={{ position: "relative", margin: "0 auto", width: Math.max(520, maxCols * 34 + 140) }}>
                <div
                  style={{
                    position: "absolute",
                    left: "50%",
                    top: 0,
                    transform: "translateX(-50%)",
                    width: 106,
                    height: 62,
                    border: "2px solid #64748b",
                    borderBottom: "none",
                    background: "linear-gradient(180deg, rgba(255,255,255,0.04), rgba(15,23,42,0.58))",
                    zIndex: 2,
                  }}
                />
                <div
                  style={{
                    position: "relative",
                    marginTop: 50,
                    border: "2px solid #64748b",
                    background: "linear-gradient(180deg, rgba(255,255,255,0.02), rgba(15,23,42,0.72))",
                    boxShadow: "22px 0 0 rgba(15,23,42,0.34)",
                    padding: "2px 10px 34px",
                  }}
                >
                  <div style={{ position: "absolute", top: 0, bottom: 34, right: 122, width: 2, background: "rgba(56,189,248,0.55)" }} />
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: `54px repeat(${maxCols}, minmax(28px, 1fr)) 126px`,
                      gridAutoRows: "34px",
                      gap: 1,
                      position: "relative",
                      zIndex: 1,
                    }}
                    >
                    {floors.map((floor) => {
                      const row = grouped.get(floor) ?? [];
                      const paddedRow = isTower
                        ? (hasUnitInventory
                            ? towerLetters.slice(0, maxCols).map((letter) => row.find((unit) => unitSuffixLetter(unit.unit) === letter) ?? null)
                            : Array.from({ length: maxCols }, () => null))
                        : [...row, ...Array.from({ length: Math.max(0, maxCols - row.length) }, () => null)];
                      return (
                        <Fragment key={floor}>
                          <div style={{ display: "grid", placeItems: "center", color: "#64748b", fontSize: 10, fontWeight: 800, borderRight: "1px solid rgba(100,116,139,0.7)" }}>
                            {String(floor).padStart(2, "0")}
                          </div>
                        {paddedRow.map((unit, index) =>
                          unit ? (
                              <UnitPrismButton
                                key={unit.unit}
                                unit={unit}
                                selected={selectedUnit?.unit === unit.unit}
                                onSelectUnit={onSelectUnit}
                                onInspectPort={onInspectPort}
                                compact
                              />
                            ) : (
                              <div key={`empty-${floor}-${index}`} style={{ border: "1px solid rgba(100,116,139,0.42)", background: "rgba(255,255,255,0.015)" }} />
                            )
                          )}
                          <div style={{ position: "relative", display: "flex", alignItems: "center", gap: 6, paddingLeft: 12 }}>
                            {floor === 1 ? (
                              <div style={{ fontSize: 10, color: "#94a3b8", fontWeight: 700 }}>
                                {hasUnitInventory ? "Offices / no customer CPEs" : "Infrastructure riser / unit map pending"}
                              </div>
                            ) : !hasUnitInventory && floor === totalFloors ? (
                              <div style={{ fontSize: 10, color: "#64748b", fontWeight: 700 }}>3D tower shell restored</div>
                            ) : (
                              <>
                                <div style={{ position: "absolute", left: 0, top: "50%", width: 12, height: 2, background: "rgba(56,189,248,0.55)", transform: "translateY(-50%)" }} />
                              </>
                            )}
                          </div>
                        </Fragment>
                      );
                    })}
                  </div>
                  <div
                    style={{
                      position: "absolute",
                      left: 0,
                      right: 0,
                      bottom: 0,
                      height: 32,
                      borderTop: "2px solid #64748b",
                      background: "rgba(226,232,240,0.08)",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      padding: "0 14px",
                    }}
                  >
                    <div style={{ color: "#64748b", fontSize: 10, fontWeight: 800 }}>BASEMENT</div>
                    <div style={{ color: "#475569", fontSize: 10, fontWeight: 700 }}>{building.profile?.basementLabel ?? "Basement"}</div>
                    <div style={{ fontSize: 10, color: "#475569", fontWeight: 700 }}>
                      {hasUnitInventory ? "Switch stack at right" : "Infrastructure-only tower view"}
                    </div>
                  </div>
                </div>
                <div
                  style={{
                    position: "absolute",
                    right: 18,
                    top: 108,
                    display: "flex",
                    flexDirection: "column",
                    gap: 14,
                    alignItems: "stretch",
                    zIndex: 3,
                  }}
                >
                  {displaySwitchStack.map((placement) => (
                    <div
                      key={placement.device.identity}
                      onMouseEnter={() => setHoveredSwitchId(placement.device.identity)}
                      onMouseLeave={() => setHoveredSwitchId((current) => (current === placement.device.identity ? null : current))}
                      onClick={() => setSelectedSwitchId((current) => (current === placement.device.identity ? null : placement.device.identity))}
                      style={{
                        minWidth: 64,
                        padding: "6px 10px",
                        borderRadius: 8,
                        border: `1px solid ${selectedSwitchId === placement.device.identity ? "#38bdf8" : "#22c55e"}`,
                        background: selectedSwitchId === placement.device.identity ? "#082f49" : "#052e16",
                        color: "#dcfce7",
                        fontSize: 10,
                        fontWeight: 800,
                        textAlign: "center",
                        cursor: "pointer",
                      }}
                    >
                      {placement.label}
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <div style={{ position: "relative", margin: "0 auto", width: "100%", maxWidth: 960 }}>
                {/* Blueprint dot-grid background */}
                <svg
                  viewBox={`0 0 ${genericCanvasWidth} ${genericCanvasHeight}`}
                  style={{ width: "100%", height: "auto", display: "block", overflow: "visible" }}
                >
                  {/* Blueprint grid background */}
                  <defs>
                    <pattern id="blueprint-dots" x="0" y="0" width="20" height="20" patternUnits="userSpaceOnUse">
                      <circle cx="1" cy="1" r="0.8" fill="rgba(56,189,248,0.18)" />
                    </pattern>
                  </defs>
                  <rect x="0" y="0" width={genericCanvasWidth} height={genericCanvasHeight} fill="url(#blueprint-dots)" />

                  {/* Building facade */}
                  <polygon
                    points={[
                      `${genericFacadeLeft},${genericFacadeTop}`,
                      `${genericFacadeRight},${genericFacadeTop}`,
                      `${genericFacadeRight},${genericFacadeBottom}`,
                      `${genericFacadeLeft},${genericFacadeBottom}`,
                    ].join(" ")}
                    fill="rgba(2,6,23,0.82)"
                    stroke="#38bdf8"
                    strokeWidth="1.5"
                  />
                  {/* Roof face */}
                  <polygon
                    points={[
                      `${genericRoofLeft},${genericRoofFrontY}`,
                      `${genericRoofRight},${genericRoofFrontY}`,
                      `${genericRoofRight + genericDepthX},${genericRoofFrontY - genericDepthY}`,
                      `${genericRoofLeft + genericDepthX},${genericRoofFrontY - genericDepthY}`,
                    ].join(" ")}
                    fill="rgba(8,47,73,0.72)"
                    stroke="#38bdf8"
                    strokeWidth="1.5"
                  />
                  {/* Side face */}
                  <polygon
                    points={[
                      `${genericFacadeRight},${genericFacadeTop}`,
                      `${genericFacadeRight + genericDepthX},${genericFacadeTop - genericDepthY}`,
                      `${genericFacadeRight + genericDepthX},${genericFacadeBottom - genericDepthY}`,
                      `${genericFacadeRight},${genericFacadeBottom}`,
                    ].join(" ")}
                    fill="rgba(4,20,40,0.72)"
                    stroke="#38bdf8"
                    strokeWidth="1.2"
                  />

                  {/* ROOF RADIO badge */}
                  {(roofSwitches[0] || router) ? (
                    <>
                      <rect
                        x={genericFacadeLeft + (genericFacadeRight - genericFacadeLeft) / 2 - 52}
                        y={genericRoofFrontY - genericDepthY - 36}
                        width={104}
                        height={26}
                        rx="4"
                        fill="rgba(8,47,73,0.92)"
                        stroke="#38bdf8"
                        strokeWidth="1.5"
                      />
                      <text
                        x={genericFacadeLeft + (genericFacadeRight - genericFacadeLeft) / 2}
                        y={genericRoofFrontY - genericDepthY - 18}
                        fill="#7dd3fc"
                        fontSize="10"
                        fontWeight="700"
                        textAnchor="middle"
                        letterSpacing="0.12em"
                      >
                        ROOF RADIO
                      </text>
                      {/* Connector line from badge to roof */}
                      <line
                        x1={genericFacadeLeft + (genericFacadeRight - genericFacadeLeft) / 2}
                        y1={genericRoofFrontY - genericDepthY - 10}
                        x2={genericFacadeLeft + (genericFacadeRight - genericFacadeLeft) / 2}
                        y2={genericRoofFrontY - genericDepthY}
                        stroke="#38bdf8"
                        strokeWidth="1.2"
                        strokeDasharray="3 2"
                      />
                    </>
                  ) : null}

                  {/* Building height dimension — left rail */}
                  <line x1={genericFacadeLeft - 18} y1={genericFacadeTop} x2={genericFacadeLeft - 18} y2={genericFacadeBottom} stroke="rgba(56,189,248,0.5)" strokeWidth="1" />
                  <line x1={genericFacadeLeft - 22} y1={genericFacadeTop} x2={genericFacadeLeft - 14} y2={genericFacadeTop} stroke="rgba(56,189,248,0.5)" strokeWidth="1" />
                  <line x1={genericFacadeLeft - 22} y1={genericFacadeBottom} x2={genericFacadeLeft - 14} y2={genericFacadeBottom} stroke="rgba(56,189,248,0.5)" strokeWidth="1" />
                  <text
                    x={genericFacadeLeft - 26}
                    y={(genericFacadeTop + genericFacadeBottom) / 2}
                    fill="rgba(56,189,248,0.7)"
                    fontSize="9"
                    fontWeight="600"
                    textAnchor="middle"
                    transform={`rotate(-90, ${genericFacadeLeft - 26}, ${(genericFacadeTop + genericFacadeBottom) / 2})`}
                  >
                    {floors.length * 12}'-0"
                  </text>

                  {/* Switch rail */}
                  <line x1={genericSwitchRailX} y1={genericFacadeTop + 24} x2={genericSwitchRailX} y2={genericFacadeBottom - 22} stroke="rgba(56,189,248,0.75)" strokeWidth="3" />

                  {/* Internal switch backbone / daisy chains */}
                  {internalSwitchLinks.map((link) => {
                    const from = genericSwitchCenters.get(link.from);
                    const to = genericSwitchCenters.get(link.to);
                    if (!from || !to) return null;
                    return (
                      <g key={`${link.from}-${link.to}`}>
                        <line
                          x1={from.x}
                          y1={from.y}
                          x2={to.x}
                          y2={to.y}
                          stroke="#22c55e"
                          strokeWidth="3"
                          strokeLinecap="round"
                          opacity="0.9"
                        />
                        <circle cx={from.x} cy={from.y} r="2.5" fill="#22c55e" />
                        <circle cx={to.x} cy={to.y} r="2.5" fill="#22c55e" />
                      </g>
                    );
                  })}

                  {/* Floor rows */}
                  {floors.map((floor, floorIndex) => {
                    const row = grouped.get(floor) ?? [];
                    const paddedRow = [...row, ...Array.from({ length: Math.max(0, maxCols - row.length) }, () => null)];
                    const rowTop = genericFacadeTop + 20 + floorIndex * (genericCellHeight + genericCellGapY);
                    const rowBottom = rowTop + genericCellHeight;
                    return (
                      <Fragment key={floor}>
                        {/* Floor label — "F1" style */}
                        <text x={genericFloorLabelX} y={rowTop + genericCellHeight * 0.66} fill="#38bdf8" fontSize="13" fontWeight="700" textAnchor="middle" letterSpacing="0.05em">
                          F{floor}
                        </text>
                        {paddedRow.map((unit, colIndex) => {
                          if (!unit) return null;
                          const x = genericFacadeLeft + 18 + colIndex * (genericCellWidth + genericCellGapX);
                          const y = rowTop;
                          const unitColor = unitFaceColor(unit);
                          const frontFill = `${unitColor}cc`;
                          const topFill = `${unitColor}88`;
                          const sideFill = `${unitColor}66`;
                          const edge = selectedUnit?.unit === unit.unit ? "#f8fafc" : "rgba(148,163,184,0.88)";
                          return (
                            <g
                              key={unit.unit}
                              onMouseEnter={() => onSelectUnit(unit)}
                              onClick={() => { onSelectUnit(unit); if (unit.port) onInspectPort(unit.port); }}
                              style={{ cursor: "pointer" }}
                            >
                              <polygon
                                points={[`${x + 5},${y}`, `${x + genericCellWidth},${y}`, `${x + genericCellWidth + genericDepthX * 0.45},${y - genericDepthY}`, `${x + 5 + genericDepthX * 0.45},${y - genericDepthY}`].join(" ")}
                                fill={topFill} stroke={edge} strokeWidth="1.2"
                              />
                              <polygon
                                points={[`${x + genericCellWidth},${y}`, `${x + genericCellWidth + genericDepthX * 0.45},${y - genericDepthY}`, `${x + genericCellWidth + genericDepthX * 0.45},${y + genericCellHeight - genericDepthY}`, `${x + genericCellWidth},${y + genericCellHeight}`].join(" ")}
                                fill={sideFill} stroke={edge} strokeWidth="1.1"
                              />
                              <rect x={x} y={y} width={genericCellWidth} height={genericCellHeight} rx={0} fill={frontFill} stroke={edge} strokeWidth="1.2" />
                              <text x={x + genericCellWidth / 2} y={y + genericCellHeight / 2 + 6} fill="#f8fafc" fontSize="11" fontWeight="800" textAnchor="middle">
                                {unit.unit}
                              </text>
                            </g>
                          );
                        })}
                        {/* Switch connector */}
                        <line x1={genericSwitchRailX - 14} y1={rowTop + genericCellHeight / 2} x2={genericSwitchRailX} y2={rowTop + genericCellHeight / 2} stroke="rgba(56,189,248,0.75)" strokeWidth="2" />
                        {/* Floor divider */}
                        <line x1={genericFacadeLeft} y1={rowBottom} x2={genericFacadeRight} y2={rowBottom} stroke="rgba(56,189,248,0.18)" strokeWidth="1" />
                      </Fragment>
                    );
                  })}

                  {displaySwitchStack.map((placement) => {
                    const center = genericSwitchCenters.get(placement.device.identity);
                    if (!center) return null;
                    return (
                      <g
                        key={placement.device.identity}
                        transform={`translate(${center.x - 32}, ${center.y - 12})`}
                        onMouseEnter={() => setHoveredSwitchId(placement.device.identity)}
                        onMouseLeave={() => setHoveredSwitchId((cur) => cur === placement.device.identity ? null : cur)}
                        onClick={() => setSelectedSwitchId((current) => (current === placement.device.identity ? null : placement.device.identity))}
                        style={{ cursor: "pointer" }}
                      >
                        <rect x="0" y="0" width="64" height="24" rx="6" fill={selectedSwitchId === placement.device.identity ? "#082f49" : "#052e16"} stroke={selectedSwitchId === placement.device.identity ? "#38bdf8" : "#22c55e"} />
                        <text x="32" y="16" fill="#dcfce7" fontSize="10" fontWeight="800" textAnchor="middle">{placement.label}</text>
                      </g>
                    );
                  })}

                  {/* Footer watermark */}
                  <text
                    x={genericFacadeRight - 4}
                    y={genericCanvasHeight - 8}
                    fill="rgba(56,189,248,0.3)"
                    fontSize="9"
                    fontWeight="600"
                    textAnchor="end"
                    letterSpacing="0.15em"
                  >
                    NYCHA NOC · {building.shortLabel.toUpperCase()} · SHEET 01
                  </text>
                </svg>

                {/* RF LINKS ACTIVE panel — right side */}
                {selectedRadio ? (
                  <div style={{
                    position: "absolute",
                    left: genericFacadeLeft + 20,
                    top: 10,
                    width: Math.min(220, genericFacadeRight - genericFacadeLeft - 40),
                    background: "rgba(2,6,23,0.92)",
                    border: "1px solid rgba(56,189,248,0.35)",
                    borderRadius: 8,
                    padding: "10px 12px",
                    boxShadow: "0 10px 24px rgba(2,6,23,0.32)",
                  }}>
                    <div style={{ fontSize: 9, letterSpacing: "0.14em", color: "#38bdf8", marginBottom: 10, fontWeight: 700 }}>RF LINKS ACTIVE</div>
                    {[
                      { name: selectedRadio.name, freq: "5.8GHz", model: selectedRadio.model, status: selectedRadio.status },
                    ].map((link) => (
                      <div key={link.name} style={{ marginBottom: 10 }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 2 }}>
                          <div style={{ width: 7, height: 7, borderRadius: 2, background: STATUS_COLOR[link.status], flexShrink: 0 }} />
                          <div style={{ fontSize: 10, color: "#e2e8f0", fontWeight: 700, lineHeight: 1.35, whiteSpace: "normal", wordBreak: "break-word" }}>{link.name}</div>
                        </div>
                        <div style={{ fontSize: 9, color: "#475569", paddingLeft: 13, lineHeight: 1.35 }}>{link.freq} · {link.model} · {link.status}</div>
                      </div>
                    ))}
                  </div>
                ) : null}

                {/* Bottom status bar */}
                <div style={{
                  marginTop: 6,
                  padding: "7px 14px",
                  background: "rgba(8,47,73,0.6)",
                  border: "1px solid rgba(56,189,248,0.2)",
                  borderRadius: 6,
                  fontSize: 10,
                  color: "#7dd3fc",
                  letterSpacing: "0.04em",
                  textAlign: "center",
                }}>
                  Click a unit to trace its network path
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      <div style={{ display: "grid", gap: 12 }}>
        <div style={{ background: "#030712", border: "1px solid #0f172a", borderRadius: 10, padding: 14 }}>
          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>SELECTED UNIT</div>
          {selectedUnit ? (
            <>
              <div style={{ fontSize: 18, fontWeight: 700, color: "#f8fafc" }}>{selectedUnit.unit}</div>
              <div style={{ fontSize: 11, color: unitFaceColor(selectedUnit), marginTop: 4 }}>{unitStatusLabel(selectedUnit)}</div>
              <div style={{ display: "grid", gap: 8, marginTop: 14 }}>
                {[
                  { label: "CPE MAC", value: selectedUnit.port?.mac ?? "Unknown" },
                  {
                    label: "Switch",
                    value: selectedUnit.port?.identity ? (
                      <button
                        onClick={() => onOpenDevice(selectedUnit.port!.identity)}
                        style={{ background: "none", border: "none", padding: 0, color: "#93c5fd", cursor: "pointer", font: "inherit", textDecoration: "underline" }}
                      >
                        {selectedUnit.port.identity}
                      </button>
                    ) : "Unknown",
                  },
                  { label: "Port", value: ifaceLabel(selectedUnit.port?.on_interface) },
                  { label: "VLAN", value: selectedUnit.port ? String(selectedUnit.port.vid) : "Unknown" },
                  { label: "Vendor", value: selectedUnit.port ? vendorFromMac(selectedUnit.port.mac) : "Unknown" },
                  { label: "Evidence", value: selectedUnit.port ? "Port-verified live client" : selectedUnit.inferred ? "Inventory/evidence-backed; no exact live port match" : "Unknown" },
                ].map((item) => (
                  <div key={item.label} style={{ borderRadius: 8, border: "1px solid #1e293b", background: "#020617", padding: "8px 10px" }}>
                    <div style={{ fontSize: 9, color: "#475569" }}>{item.label}</div>
                    <div style={{ fontSize: 12, color: "#e2e8f0", fontWeight: 600, marginTop: 2 }}>{item.value}</div>
                  </div>
                ))}
              </div>
              {selectedUnit.port ? (
                <button
                  onClick={() => onInspectPort(selectedUnit.port as PortWithStatus)}
                  style={{
                    marginTop: 12,
                    width: "100%",
                    borderRadius: 8,
                    border: "1px solid #164e63",
                    background: "#082f49",
                    color: "#bae6fd",
                    padding: "10px 12px",
                    cursor: "pointer",
                    fontSize: 12,
                    fontWeight: 700,
                  }}
                >
                  Open CPE Port Detail
                </button>
              ) : null}
              {selectedUnit.port ? (
                <div style={{ marginTop: 12 }}>
                  <CpeContextPanel
                    compact
                    vendor={vendorFromMac(selectedUnit.port.mac)}
                    vilo={selectedUnitVilo ? {
                      classification: selectedUnitVilo.classification,
                      inventory_status: selectedUnitVilo.inventory_status,
                      device_sn: selectedUnitVilo.device_sn,
                      subscriber_id: selectedUnitVilo.subscriber_id,
                      subscriber: selectedUnitVilo.subscriber,
                      subscriber_hint: selectedUnitVilo.subscriber_hint,
                      network: selectedUnitVilo.network,
                      sighting: selectedUnitVilo.sighting,
                    } : null}
                    tauc={selectedUnitTauc ? {
                      network_name: selectedUnitTauc.network_name,
                      site_id: selectedUnitTauc.site_id,
                      expected_prefix: selectedUnitTauc.expected_prefix,
                      wan_mode: selectedUnitTauc.wan_mode,
                      mesh_nodes: selectedUnitTauc.mesh_nodes,
                      sn: selectedUnitTauc.sn,
                    } : null}
                  />
                </div>
              ) : null}
              <div style={{ fontSize: 10, color: "#64748b", marginTop: 12 }}>
                {selectedUnit.port
                  ? (selectedUnit.inferred ? "Port-to-unit mapping is currently inferred from switch port order." : "Unit label came from address inventory.")
                  : "No customer CPE is currently mapped to this unit."}
              </div>
            </>
          ) : (
            <div style={{ fontSize: 11, color: "#64748b" }}>
              {units.length
                ? "Select a unit in the building wireframe to inspect its CPE."
                : "No customer CPEs are currently mapped for this site, so there is no unit-level drilldown yet."}
            </div>
          )}
        </div>

        <div style={{ background: "#030712", border: "1px solid #0f172a", borderRadius: 10, padding: 14 }}>
          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>RISER MODEL</div>
          <div style={{ display: "grid", gap: 8 }}>
            {router ? (
              <div style={{ padding: "8px 10px", borderRadius: 8, border: "1px solid #78350f", background: "#1c1917" }}>
                <div style={{ fontSize: 11, color: "#fde68a", fontWeight: 700 }}>{isTower ? "Roof Router" : "Core Router"}</div>
                <button
                  onClick={() => onOpenDevice(router.identity)}
                  style={{ background: "none", border: "none", padding: 0, color: "#93c5fd", cursor: "pointer", fontSize: 11, marginTop: 3, textDecoration: "underline", textAlign: "left" }}
                >
                  {router.identity}
                </button>
              </div>
            ) : null}
            {switchPlacements.map((placement) => (
              <div key={placement.device.identity} style={{ padding: "8px 10px", borderRadius: 8, border: "1px solid #0f172a", background: "#020617" }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                  <button
                    onClick={() => onOpenDevice(placement.device.identity)}
                    style={{ background: "none", border: "none", padding: 0, color: "#93c5fd", cursor: "pointer", fontSize: 11, fontWeight: 700, textDecoration: "underline", textAlign: "left" }}
                  >
                    {placement.device.identity}
                  </button>
                  <span style={{ fontSize: 10, color: "#94a3b8" }}>{placement.floor === 0 ? "Basement" : `Floor ${String(placement.floor).padStart(2, "0")}`}</span>
                </div>
                <div style={{ fontSize: 10, color: "#64748b", marginTop: 4 }}>{placement.count} mapped units · inferred from port-to-unit mapping</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function MapView({
  buildings,
  radios,
  radioLinks,
  siteTopology,
  selectedId,
  selectedRadioId,
  onSelect,
  onSelectRadio,
}: {
  buildings: BuildingLive[];
  radios: RadioLive[];
  radioLinks: Array<{ fromRadioId?: string; toRadioId?: string; fromBuildingId?: string; toBuildingId?: string; strength: "strong" | "medium" | "weak"; kind: string }>;
  siteTopology: SiteTopology | null;
  selectedId: string | null;
  selectedRadioId: string | null;
  onSelect: (building: BuildingLive) => void;
  onSelectRadio: (radio: RadioLive) => void;
}) {
  const frameRef = useRef<HTMLDivElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const fitDoneRef = useRef(false);
  const mapReadyRef = useRef(false);
  const buildingsRef = useRef(buildings);
  const radiosRef = useRef(radios);

  useEffect(() => {
    buildingsRef.current = buildings;
    radiosRef.current = radios;
  }, [buildings, radios]);

  const buildingFeatures = useMemo(
    () =>
      buildings
        .map((building) => {
          const coord = getCoord(building.address, siteTopology, canonicalBuildingIdOf(building));
          if (!coord) return null;
          return {
            type: "Feature" as const,
            properties: {
              nodeKey: `building:${building.id}`,
              id: building.id,
              kind: "building",
              label: building.shortLabel,
              status: building.status,
              customerCount: building.customerCount,
            },
            geometry: {
              type: "Point" as const,
              coordinates: [coord.lon, coord.lat],
            },
          };
        })
        .filter(isPresent),
    [buildings, siteTopology],
  );

  const radioFeatures = useMemo(
    () =>
      radios
        .map((radio) => {
          const coord = radioCoord(radio, siteTopology);
          if (!coord) return null;
          return {
            type: "Feature" as const,
            properties: {
              nodeKey: `radio:${radio.id}`,
              id: radio.id,
              kind: "radio",
              label: radio.shortLabel,
              status: radio.status,
              model: radio.model,
            },
            geometry: {
              type: "Point" as const,
              coordinates: [coord.lon, coord.lat],
            },
          };
        })
        .filter(isPresent),
    [radios, siteTopology],
  );

  const lineFeatures = useMemo(() => {
    const byBuildingId = new Map(buildings.map((building) => [building.id, building]));
    const byRadioId = new Map(radios.map((radio) => [radio.id, radio]));
    const features: Array<{
      type: "Feature";
      properties: Record<string, string>;
      geometry: { type: "LineString"; coordinates: number[][] };
    }> = [];

    for (const link of radioLinks) {
      const fromBuilding = link.fromBuildingId ? byBuildingId.get(link.fromBuildingId) : null;
      const toBuilding = link.toBuildingId ? byBuildingId.get(link.toBuildingId) : null;
      const fromRadio = link.fromRadioId ? byRadioId.get(link.fromRadioId) : null;
      const toRadio = link.toRadioId ? byRadioId.get(link.toRadioId) : null;
      const preferRadioEndpoints = link.kind !== "Siklu transport" && !(fromBuilding && toBuilding);
      const fromCoord = preferRadioEndpoints
        ? fromRadio ? radioCoord(fromRadio, siteTopology) : fromBuilding ? getCoord(fromBuilding.address, siteTopology, canonicalBuildingIdOf(fromBuilding)) : null
        : fromBuilding ? getCoord(fromBuilding.address, siteTopology, canonicalBuildingIdOf(fromBuilding)) : fromRadio ? radioCoord(fromRadio, siteTopology) : null;
      const toCoord = preferRadioEndpoints
        ? toRadio ? radioCoord(toRadio, siteTopology) : toBuilding ? getCoord(toBuilding.address, siteTopology, canonicalBuildingIdOf(toBuilding)) : null
        : toBuilding ? getCoord(toBuilding.address, siteTopology, canonicalBuildingIdOf(toBuilding)) : toRadio ? radioCoord(toRadio, siteTopology) : null;
      const fromStatus = preferRadioEndpoints ? fromRadio?.status ?? fromBuilding?.status : fromBuilding?.status ?? fromRadio?.status;
      const toStatus = preferRadioEndpoints ? toRadio?.status ?? toBuilding?.status : toBuilding?.status ?? toRadio?.status;
      if (!fromCoord || !toCoord || !fromStatus || !toStatus) continue;
      features.push({
        type: "Feature",
        properties: {
          kind: link.kind === "Siklu transport" ? "transport" : "radio-link",
          family: link.kind === "Siklu transport" ? "siklu" : "cambium",
          strength: link.strength,
          label: link.kind,
          status: degradeStatus(fromStatus, toStatus),
        },
        geometry: {
          type: "LineString",
          coordinates: [
            [fromCoord.lon, fromCoord.lat],
            [toCoord.lon, toCoord.lat],
          ],
        },
      });
    }

    return features;
  }, [buildings, radioLinks, radios]);

  const cambiumLineFeatures = useMemo(() => {
    const byBuildingId = new Map(buildings.map((building) => [canonicalBuildingIdOf(building), building]));
    const byRadioId = new Map(radios.map((radio) => [radio.id, radio]));
    const sikluBuildingPairs = new Set(
      (siteTopology?.radio_links ?? [])
        .filter((link) => (link.kind ?? "").toLowerCase() === "siklu" && link.from_building_id && link.to_building_id)
        .map((link) =>
          [String(link.from_building_id), String(link.to_building_id)].sort().join("::"),
        ),
    );
    return (siteTopology?.radio_links ?? [])
      .filter((link) => (link.kind ?? "").toLowerCase() === "cambium")
      .filter((link) => {
        const fromBuildingId = link.from_building_id ? String(link.from_building_id) : "";
        const toBuildingId = link.to_building_id ? String(link.to_building_id) : "";
        return !(fromBuildingId && toBuildingId && sikluBuildingPairs.has([fromBuildingId, toBuildingId].sort().join("::")));
      })
      .map((link) => {
        const fromBuilding =
          (link.from_building_id ? byBuildingId.get(link.from_building_id) : null) ??
          findMatchingBuilding(buildings, link.from_label, link.from_building_id, link.location);
        const toBuilding =
          (link.to_building_id ? byBuildingId.get(link.to_building_id) : null) ??
          findMatchingBuilding(buildings, link.to_label, link.to_building_id, null);
        const fromRadio = link.from_radio_id
          ? byRadioId.get(link.from_radio_id)
          : radios.find((radio) => radioMatchesEndpoint(radio, link.from_label, link.location));
        const toRadio = link.to_radio_id
          ? byRadioId.get(link.to_radio_id)
          : radios.find((radio) => radioMatchesEndpoint(radio, link.to_label, null));
        const preferRadioEndpoints = Boolean(fromRadio || toRadio);
        const fromCoord = preferRadioEndpoints
          ? fromRadio ? radioCoord(fromRadio, siteTopology) : fromBuilding ? getCoord(fromBuilding.address, siteTopology, canonicalBuildingIdOf(fromBuilding)) : null
          : fromBuilding ? getCoord(fromBuilding.address, siteTopology, canonicalBuildingIdOf(fromBuilding)) : fromRadio ? radioCoord(fromRadio, siteTopology) : null;
        const toCoord = preferRadioEndpoints
          ? toRadio ? radioCoord(toRadio, siteTopology) : toBuilding ? getCoord(toBuilding.address, siteTopology, canonicalBuildingIdOf(toBuilding)) : null
          : toBuilding ? getCoord(toBuilding.address, siteTopology, canonicalBuildingIdOf(toBuilding)) : toRadio ? radioCoord(toRadio, siteTopology) : null;
        const fromStatus = preferRadioEndpoints ? fromRadio?.status ?? fromBuilding?.status : fromBuilding?.status ?? fromRadio?.status;
        const toStatus = preferRadioEndpoints ? toRadio?.status ?? toBuilding?.status : toBuilding?.status ?? toRadio?.status;
        if (!fromCoord || !toCoord || !fromStatus || !toStatus) return null;
        return {
          type: "Feature" as const,
          properties: {
            kind: "radio-link",
            family: "cambium",
            strength: link.status === "ok" ? "strong" : "weak",
            label: link.name,
            status: degradeStatus(fromStatus, toStatus),
          },
          geometry: {
            type: "LineString" as const,
            coordinates: [
              [fromCoord.lon, fromCoord.lat],
              [toCoord.lon, toCoord.lat],
            ],
          },
        };
      })
      .filter(isPresent);
  }, [buildings, radios, siteTopology]);

  const packetFeatures = useMemo(() => {
    return lineFeatures.map((feature) => {
      const coords = feature.geometry.coordinates;
      const status = (feature.properties.status as Status | undefined) ?? "unknown";
      return {
        type: "Feature" as const,
        properties: {
          status,
          size: feature.properties.kind === "transport" ? 6.2 : 5,
        },
        geometry: {
          type: "Point" as const,
          coordinates: interpolateLinePosition(coords, 0.5),
        },
      };
    });
  }, [lineFeatures]);

  const cambiumPacketFeatures = useMemo(() => {
    return cambiumLineFeatures.map((feature) => ({
      type: "Feature" as const,
      properties: {
        status: feature.properties.status,
      },
      geometry: {
        type: "Point" as const,
        coordinates: interpolateLinePosition(feature.geometry.coordinates, 0.5),
      },
    }));
  }, [cambiumLineFeatures]);

  const syncMapData = (
    map: maplibregl.Map,
    siteFeatures: typeof buildingFeatures,
    transportRadioFeatures: typeof radioFeatures,
    networkLineFeatures: typeof lineFeatures,
    networkCambiumFeatures: typeof cambiumLineFeatures,
    networkPacketFeatures: typeof packetFeatures,
    cambiumPackets: typeof cambiumPacketFeatures,
    activeBuildingId: string | null,
    activeRadioId: string | null,
  ) => {
    const buildingSource = map.getSource("building-nodes") as maplibregl.GeoJSONSource | undefined;
    if (buildingSource) {
      buildingSource.setData({
        type: "FeatureCollection",
        features: siteFeatures,
      });
    }

    const radioSource = map.getSource("radio-nodes") as maplibregl.GeoJSONSource | undefined;
    if (radioSource) {
      radioSource.setData({
        type: "FeatureCollection",
        features: transportRadioFeatures,
      });
    }

    const lineSource = map.getSource("network-lines") as maplibregl.GeoJSONSource | undefined;
    if (lineSource) {
      lineSource.setData({
        type: "FeatureCollection",
        features: networkLineFeatures,
      });
    }

    const cambiumSource = map.getSource("cambium-lines") as maplibregl.GeoJSONSource | undefined;
    if (cambiumSource) {
      cambiumSource.setData({
        type: "FeatureCollection",
        features: networkCambiumFeatures,
      });
    }

    const packetSource = map.getSource("network-packets") as maplibregl.GeoJSONSource | undefined;
    if (packetSource) {
      packetSource.setData({
        type: "FeatureCollection",
        features: networkPacketFeatures,
      });
    }

    const cambiumPacketSource = map.getSource("cambium-packets") as maplibregl.GeoJSONSource | undefined;
    if (cambiumPacketSource) {
      cambiumPacketSource.setData({
        type: "FeatureCollection",
        features: cambiumPackets,
      });
    }

    if (map.getLayer("selected-building")) {
      map.setFilter("selected-building", ["==", ["get", "nodeKey"], activeBuildingId ? `building:${activeBuildingId}` : ""]);
    }

    if (map.getLayer("selected-radio")) {
      map.setFilter("selected-radio", ["==", ["get", "nodeKey"], activeRadioId ? `radio:${activeRadioId}` : ""]);
    }

    if (!fitDoneRef.current && siteFeatures.length) {
      const bounds = new maplibregl.LngLatBounds();
      for (const feature of siteFeatures) {
        bounds.extend(feature.geometry.coordinates as [number, number]);
      }
      map.fitBounds(bounds, { padding: 56, maxZoom: 15.4, duration: 0 });
      fitDoneRef.current = true;
    }
  };

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: MAP_STYLE as never,
      center: [-73.924, 40.666],
      zoom: 13.4,
      maxZoom: 19,
    });
    mapRef.current = map;
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");

    map.on("load", () => {
      mapReadyRef.current = true;
      map.addSource("building-nodes", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      map.addSource("radio-nodes", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
        cluster: true,
        clusterRadius: 50,
        clusterMaxZoom: 13,
      });
      map.addSource("network-lines", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      map.addSource("cambium-lines", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      map.addSource("network-packets", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      map.addSource("cambium-packets", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });

      map.addLayer({
        id: "network-lines",
        type: "line",
        source: "network-lines",
        paint: {
          "line-color": [
            "case",
            ["==", ["get", "family"], "cambium"], "#38bdf8",
            ["match",
              ["get", "status"],
              "online", STATUS_COLOR.online,
              "degraded", STATUS_COLOR.degraded,
              "offline", STATUS_COLOR.offline,
              STATUS_COLOR.unknown,
            ],
          ],
          "line-width": [
            "match",
            ["get", "kind"],
            "transport", 4.2,
            "radio-link", 3.6,
            2.1,
          ],
          "line-opacity": [
            "case",
            ["==", ["get", "family"], "cambium"], 0.95,
            0.9,
          ],
          "line-blur": [
            "case",
            ["==", ["get", "family"], "cambium"], 0.05,
            0.15,
          ],
        },
      });

      map.addLayer({
        id: "network-packet-glow",
        type: "circle",
        source: "network-packets",
        paint: {
          "circle-color": [
            "match",
            ["get", "status"],
            "online", STATUS_COLOR.online,
            "degraded", STATUS_COLOR.degraded,
            "offline", STATUS_COLOR.offline,
            STATUS_COLOR.unknown,
          ],
          "circle-radius": ["get", "size"],
          "circle-opacity": 0.18,
          "circle-blur": 0.7,
        },
      });

      map.addLayer({
        id: "cambium-line-glow",
        type: "line",
        source: "cambium-lines",
        paint: {
          "line-color": "#38bdf8",
          "line-width": 10,
          "line-opacity": 0.28,
          "line-blur": 1.1,
        },
        layout: {
          "line-cap": "round",
          "line-join": "round",
        },
      });

      map.addLayer({
        id: "cambium-lines",
        type: "line",
        source: "cambium-lines",
        paint: {
          "line-color": "#67e8f9",
          "line-width": 5.2,
          "line-dasharray": [4, 3],
          "line-opacity": 1,
          "line-blur": 0,
        },
        layout: {
          "line-cap": "round",
          "line-join": "round",
        },
      });

      map.addLayer({
        id: "cambium-packets",
        type: "circle",
        source: "cambium-packets",
        paint: {
          "circle-color": "#67e8f9",
          "circle-stroke-color": "#082f49",
          "circle-stroke-width": 1.2,
          "circle-radius": 5,
          "circle-opacity": 1,
        },
      });

      map.addLayer({
        id: "network-packets",
        type: "circle",
        source: "network-packets",
        paint: {
          "circle-color": [
            "match",
            ["get", "status"],
            "online", "#dcfce7",
            "degraded", "#fef3c7",
            "offline", "#fecdd3",
            "#e2e8f0",
          ],
          "circle-stroke-color": [
            "match",
            ["get", "status"],
            "online", STATUS_COLOR.online,
            "degraded", STATUS_COLOR.degraded,
            "offline", STATUS_COLOR.offline,
            STATUS_COLOR.unknown,
          ],
          "circle-stroke-width": 1.25,
          "circle-radius": ["*", ["get", "size"], 0.5],
          "circle-opacity": 0.88,
        },
      });

      map.addLayer({
        id: "radio-clusters",
        type: "circle",
        source: "radio-nodes",
        filter: ["has", "point_count"],
        paint: {
          "circle-color": "#0f172a",
          "circle-stroke-color": "#7dd3fc",
          "circle-stroke-width": 2,
          "circle-radius": ["step", ["get", "point_count"], 16, 10, 20, 25, 26],
          "circle-opacity": 0.92,
        },
      });

      map.addLayer({
        id: "building-nodes",
        type: "circle",
        source: "building-nodes",
        paint: {
          "circle-color": [
            "match",
            ["get", "status"],
            "online", STATUS_COLOR.online,
            "degraded", STATUS_COLOR.degraded,
            "offline", STATUS_COLOR.offline,
            STATUS_COLOR.unknown,
          ],
          "circle-radius": 8.5,
          "circle-stroke-color": "#020617",
          "circle-stroke-width": 1.6,
        },
      });

      map.addLayer({
        id: "radio-unclustered-nodes",
        type: "circle",
        source: "radio-nodes",
        filter: ["!", ["has", "point_count"]],
        paint: {
          "circle-color": [
            "match",
            ["get", "status"],
            "online", STATUS_COLOR.online,
            "degraded", STATUS_COLOR.degraded,
            "offline", STATUS_COLOR.offline,
            STATUS_COLOR.unknown,
          ],
          "circle-radius": 6,
          "circle-stroke-color": "#f8fafc",
          "circle-stroke-width": 2,
        },
      });

      map.addLayer({
        id: "selected-building",
        type: "circle",
        source: "building-nodes",
        filter: ["==", ["get", "nodeKey"], ""],
        paint: {
          "circle-radius": 13,
          "circle-color": "rgba(0,0,0,0)",
          "circle-stroke-color": "#67e8f9",
          "circle-stroke-width": 2,
        },
      });

      map.addLayer({
        id: "selected-radio",
        type: "circle",
        source: "radio-nodes",
        filter: ["==", ["get", "nodeKey"], ""],
        paint: {
          "circle-radius": 10,
          "circle-color": "rgba(0,0,0,0)",
          "circle-stroke-color": "#67e8f9",
          "circle-stroke-width": 2,
        },
      });


      map.on("click", "radio-clusters", (event) => {
        const feature = event.features?.[0];
        const clusterId = feature?.properties?.cluster_id;
        const source = map.getSource("radio-nodes") as maplibregl.GeoJSONSource & {
          getClusterExpansionZoom?: (clusterId: number, callback: (error: Error | null, zoom: number) => void) => void;
        };
        if (!clusterId || !source.getClusterExpansionZoom) return;
        source.getClusterExpansionZoom(Number(clusterId), (error, zoom) => {
          if (error) return;
          map.easeTo({
            center: (feature.geometry as GeoJSON.Point).coordinates as [number, number],
            zoom,
          });
        });
      });

      map.on("click", "building-nodes", (event) => {
        const feature = event.features?.[0];
        if (!feature || feature.geometry.type !== "Point") return;
        const id = String(feature.properties?.id || "");
        const match = buildingsRef.current.find((building) => building.id === id);
        if (match) onSelect(match);
      });

      map.on("click", "radio-unclustered-nodes", (event) => {
        const feature = event.features?.[0];
        if (!feature || feature.geometry.type !== "Point") return;
        const id = String(feature.properties?.id || "");
        const match = radiosRef.current.find((radio) => radio.id === id);
        if (match) onSelectRadio(match);
      });

      for (const layerId of ["radio-clusters", "building-nodes", "radio-unclustered-nodes"]) {
        map.on("mouseenter", layerId, () => {
          map.getCanvas().style.cursor = "pointer";
        });
        map.on("mouseleave", layerId, () => {
          map.getCanvas().style.cursor = "";
        });
      }

      syncMapData(map, buildingFeatures, radioFeatures, lineFeatures, cambiumLineFeatures, packetFeatures, cambiumPacketFeatures, selectedId, selectedRadioId);
    });

    return () => {
      mapReadyRef.current = false;
      map.remove();
      mapRef.current = null;
    };
  }, [buildingFeatures, cambiumLineFeatures, cambiumPacketFeatures, lineFeatures, onSelect, onSelectRadio, packetFeatures, radioFeatures, selectedId, selectedRadioId]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReadyRef.current) return;
    syncMapData(map, buildingFeatures, radioFeatures, lineFeatures, cambiumLineFeatures, packetFeatures, cambiumPacketFeatures, selectedId, selectedRadioId);
  }, [buildingFeatures, cambiumLineFeatures, cambiumPacketFeatures, lineFeatures, packetFeatures, radioFeatures, selectedId, selectedRadioId]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReadyRef.current) return;
    const targetKey = selectedRadioId ? `radio:${selectedRadioId}` : selectedId ? `building:${selectedId}` : "";
    if (!targetKey) return;
    const feature = [...buildingFeatures, ...radioFeatures].find((entry) => entry.properties.nodeKey === targetKey);
    if (!feature) return;
    map.easeTo({
      center: feature.geometry.coordinates as [number, number],
      zoom: feature.properties.kind === "building" ? 15.4 : 15,
      duration: 600,
    });
  }, [buildingFeatures, radioFeatures, selectedId, selectedRadioId]);

  return (
    <div ref={frameRef} style={{ position: "relative", height: 680, width: "100%", borderRadius: 18, overflow: "hidden", border: "1px solid rgba(125,211,252,0.2)" }}>
      <div ref={containerRef} style={{ position: "absolute", inset: 0 }} />
    </div>
  );
}

function BuildingView({
  building,
  allBuildings,
  ports,
  loadingModel,
  radios,
  selectedRadio,
  onBack,
  onSelectPort,
  onOpenBuilding,
}: {
  building: BuildingLive;
  allBuildings: BuildingLive[];
  ports: PortWithStatus[];
  loadingModel: boolean;
  radios: RadioLive[];
  selectedRadio: RadioLive | null;
  onBack: () => void;
  onSelectPort: (port: PortWithStatus) => void;
  onOpenBuilding: (buildingId: string) => void;
}) {
  const devices = building.buildingHealth?.devices ?? [];
  const roofSwitches = devices.filter((device) => device.identity.includes("RFSW"));
  const accessSwitches = devices.filter((device) => device.identity.includes(".SW"));
  const unitBoxes = useMemo(() => buildUnitBoxes(building, ports), [building, ports]);
  const exactCoverage = building.buildingModel?.coverage?.exact_unit_port_coverage_pct ?? 0;
  const evidenceOnlineCount = evidenceBackedOnlineUnitCount(building);
  const summaryPortLabel = loadingModel ? "Loading evidence" : building.customerCount > 0 ? "Live ports" : evidenceOnlineCount > 0 ? "Online units" : "Live ports";
  const summaryPortValue = loadingModel ? "..." : building.customerCount > 0 ? String(building.customerCount) : evidenceOnlineCount > 0 ? String(evidenceOnlineCount) : "0";
  const [selectedUnitId, setSelectedUnitId] = useState<string | null>(null);
  const [summaryFocus, setSummaryFocus] = useState<SummaryFocus>(null);
  const [selectedDeviceIdentity, setSelectedDeviceIdentity] = useState<string | null>(null);
  const selectedUnit = unitBoxes.find((unit) => unit.unit === selectedUnitId) ?? unitBoxes[0] ?? null;
  const selectedUnitVilo = selectedUnit?.port ? findViloRowByMac(building, selectedUnit.port.mac) : null;
  const selectedUnitTauc = selectedUnit?.port ? findTaucRowByMac(building, selectedUnit.port.mac) : null;
  const portsByIssueKey = useMemo(() => new Map(ports.map((port) => [portKey(port.identity, port.on_interface), port])), [ports]);
  const sortedFlapPorts = useMemo(
    () => [...(building.flapHistory?.ports ?? [])].sort((a, b) => a.identity.localeCompare(b.identity) || compareInterfaceLabels(a.interface, b.interface)),
    [building.flapHistory?.ports],
  );
  const sortedExactMatches = useMemo(
    () => [...(building.buildingModel?.exact_unit_port_matches ?? [])].sort(
      (a, b) => a.switch_identity.localeCompare(b.switch_identity) || compareInterfaceLabels(a.interface, b.interface) || a.unit.localeCompare(b.unit, undefined, { numeric: true }),
    ),
    [building.buildingModel?.exact_unit_port_matches],
  );
  const deviceDetailMap = useMemo(() => {
    const modeledSwitches = new Map((building.buildingModel?.switches ?? []).map((entry) => [entry.identity, entry]));
    const neighborEdges = building.buildingModel?.direct_neighbor_edges ?? [];
    const byIdentity = new Map<string, {
      identity: string;
      ip?: string;
      model?: string;
      version?: string;
      servedUnits: string[];
      servedFloors: number[];
      neighborCount: number;
      kind: string;
    }>();
    for (const device of devices) {
      const modeled = modeledSwitches.get(device.identity);
      byIdentity.set(device.identity, {
        identity: device.identity,
        ip: device.ip,
        model: device.model || modeled?.model,
        version: device.version || modeled?.version,
        servedUnits: modeled?.served_units ?? [],
        servedFloors: modeled?.served_floors ?? [],
        neighborCount: neighborEdges.filter((edge) => edge.from_identity === device.identity || edge.to_identity === device.identity).length,
        kind: deviceKind(device.identity),
      });
    }
    for (const modeled of building.buildingModel?.switches ?? []) {
      if (byIdentity.has(modeled.identity)) continue;
      byIdentity.set(modeled.identity, {
        identity: modeled.identity,
        ip: modeled.ip,
        model: modeled.model,
        version: modeled.version,
        servedUnits: modeled.served_units ?? [],
        servedFloors: modeled.served_floors ?? [],
        neighborCount: neighborEdges.filter((edge) => edge.from_identity === modeled.identity || edge.to_identity === modeled.identity).length,
        kind: deviceKind(modeled.identity),
      });
    }
    return byIdentity;
  }, [building.buildingModel?.direct_neighbor_edges, building.buildingModel?.switches, building.buildingHealth?.devices]);
  const selectedDevice = selectedDeviceIdentity ? deviceDetailMap.get(selectedDeviceIdentity) ?? null : null;
  const openDeviceDetails = (identity: string) => {
    setSelectedDeviceIdentity(identity);
    setSummaryFocus("devices");
  };

  useEffect(() => {
    setSelectedUnitId(unitBoxes[0]?.unit ?? null);
  }, [building.id, unitBoxes]);

  useEffect(() => {
    setSummaryFocus(null);
  }, [building.id]);

  useEffect(() => {
    setSelectedDeviceIdentity(null);
  }, [building.id]);

  return (
    <div style={{ padding: "0 4px" }}>
      <button
        onClick={onBack}
        style={{ background: "none", border: "none", color: "#60a5fa", cursor: "pointer", fontSize: 13, padding: "0 0 12px", display: "flex", alignItems: "center", gap: 6 }}
      >
        ← back to map
      </button>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 16 }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 18, fontWeight: 600, color: "#f1f5f9" }}>{building.name}</h2>
          <p style={{ margin: "3px 0 0", fontSize: 12, color: "#64748b" }}>
            {building.development} · Block {building.id} · {loadingModel ? "loading live unit evidence" : `${building.customerCount} live customer-facing ports`}
            {building.knownUnits.length ? ` · ${building.knownUnits.length} known units` : ""}
            {!building.customerCount && evidenceOnlineCount ? ` · ${evidenceOnlineCount} evidence-backed online units` : ""}
          </p>
        </div>
        <span
          style={{
            fontSize: 11,
            padding: "3px 10px",
            borderRadius: 12,
            background: STATUS_BG[building.status],
            color: STATUS_COLOR[building.status],
            border: `1px solid ${STATUS_COLOR[building.status]}40`,
          }}
        >
          {building.status}
        </span>
      </div>

      {loadingModel ? (
        <div style={{ marginBottom: 16, padding: "10px 12px", borderRadius: 10, border: "1px solid #164e63", background: "#082f49", color: "#bae6fd", fontSize: 11, lineHeight: 1.5 }}>
          Live unit evidence for this building is still loading. Neutral boxes here mean unverified right now, not confirmed down.
        </div>
      ) : null}

      <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14, marginBottom: 16 }}>
        <WireframeTwinView
        building={building}
        units={unitBoxes}
        selectedUnit={selectedUnit}
        selectedUnitVilo={selectedUnitVilo}
        selectedUnitTauc={selectedUnitTauc}
        onSelectUnit={(unit) => setSelectedUnitId(unit.unit)}
        onInspectPort={onSelectPort}
        onOpenDevice={openDeviceDetails}
        selectedRadio={selectedRadio}
        roofSwitches={roofSwitches}
        accessSwitches={accessSwitches}
        coreDevices={building.buildingHealth?.devices ?? []}
        />
      </div>

      <CompoundSiteDiagram building={building} allBuildings={allBuildings} radios={radios} selectedRadio={selectedRadio} onOpenBuilding={onOpenBuilding} />

      <div style={{ display: "grid", gridTemplateColumns: selectedRadio ? "1fr 1fr" : "1fr", gap: 14, marginBottom: 16 }}>
        {selectedRadio ? (
          <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14 }}>
            <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 10 }}>ACTIVE RADIO CONTEXT</div>
            <div style={{ fontSize: 13, color: "#e2e8f0", fontWeight: 700 }}>{selectedRadio.name}</div>
            <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 4 }}>{selectedRadio.model} · {selectedRadio.role}</div>
            <div style={{ fontSize: 11, color: STATUS_COLOR[selectedRadio.status], marginTop: 8 }}>{selectedRadio.status}</div>
            <div style={{ fontSize: 10, color: "#64748b", marginTop: 8 }}>
              {selectedRadio.alert?.annotations?.summary ?? "No active alert on this radio."}
            </div>
          </div>
        ) : null}
        <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14 }}>
          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 10 }}>ASSUMPTION NOTES</div>
          <div style={{ fontSize: 11, color: "#94a3b8", lineHeight: 1.5 }}>
            {canonicalBuildingIdOf(building) === "000007.055"
              ? "728 E NY uses a 20-floor wireframe with 13 units per residential floor, floor 01 reserved for offices, SW01 modeled in the basement, and router R01 modeled in the roof parapet."
              : building.buildingModel
                ? `${building.buildingModel.coverage.exact_unit_port_match_count} exact unit-port matches out of ${building.buildingModel.coverage.known_unit_count} units for this building (${exactCoverage}% coverage). ${building.buildingModel.data_gaps.switch_floor_placement}`
                : "Switch floor placement is inferred from the unit floors served by each access switch. Unit-to-port mappings stay tied to verified inventory when unit labels exist and fall back to deterministic port-order inference otherwise."}
          </div>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 16 }}>
        <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14 }}>
          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>CPE DEPLOYMENT</div>
          <InfoGrid
            items={[
              { label: "Live CPEs", value: String(building.cpeIntelligence?.customer_count ?? building.customerCount) },
              { label: "Access Ports", value: String(building.cpeIntelligence?.access_port_count ?? building.buildingCustomerCount?.access_port_count ?? 0) },
              { label: "Vilo", value: String(building.cpeIntelligence?.vendor_summary?.vilo ?? 0) },
              { label: "TP-Link", value: String(building.cpeIntelligence?.vendor_summary?.tplink ?? 0) },
              { label: "Dark Building", value: building.cpeIntelligence?.dark_building ? "Yes" : "No" },
              { label: "Top Firmware", value: Object.entries(building.cpeIntelligence?.vilo.firmware_versions ?? {})[0]?.join(" · ") ?? "Unknown" },
            ]}
          />
        </div>
        <CpeContextPanel
          vendor={selectedUnit?.port ? vendorFromMac(selectedUnit.port.mac) : "unknown"}
          vilo={selectedUnitVilo ? {
            classification: selectedUnitVilo.classification,
            inventory_status: selectedUnitVilo.inventory_status,
            device_sn: selectedUnitVilo.device_sn,
            subscriber_id: selectedUnitVilo.subscriber_id,
            subscriber: selectedUnitVilo.subscriber,
            subscriber_hint: selectedUnitVilo.subscriber_hint,
            network: selectedUnitVilo.network,
            sighting: selectedUnitVilo.sighting,
          } : null}
          tauc={selectedUnitTauc ? {
            network_name: selectedUnitTauc.network_name,
            site_id: selectedUnitTauc.site_id,
            expected_prefix: selectedUnitTauc.expected_prefix,
            wan_mode: selectedUnitTauc.wan_mode,
            mesh_nodes: selectedUnitTauc.mesh_nodes,
            sn: selectedUnitTauc.sn,
          } : null}
        />
      </div>

      <TopologyBranchPanel building={building} devices={devices} selectedRadio={selectedRadio} />

      {selectedDevice ? (
        <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14, marginBottom: 16 }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "start" }}>
            <div>
              <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 10 }}>SELECTED DEVICE</div>
              <div style={{ fontSize: 18, color: "#e2e8f0", fontWeight: 700 }}>{selectedDevice.identity}</div>
              <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 4 }}>
                {selectedDevice.model ?? "Unknown model"}{selectedDevice.ip ? ` · ${selectedDevice.ip}` : ""}{selectedDevice.version ? ` · ${selectedDevice.version}` : ""}
              </div>
            </div>
            <button
              onClick={() => setSelectedDeviceIdentity(null)}
              style={{ border: "1px solid #164e63", background: "#082f49", color: "#bae6fd", borderRadius: 8, padding: "8px 10px", cursor: "pointer", fontSize: 11, fontWeight: 700 }}
            >
              Close device detail
            </button>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 8, marginTop: 12 }}>
            {[
              { label: "Role", value: selectedDevice.kind },
              { label: "Neighbors", value: String(selectedDevice.neighborCount) },
              { label: "Served floors", value: selectedDevice.servedFloors.length ? selectedDevice.servedFloors.map((floor) => String(floor).padStart(2, "0")).join(", ") : "n/a" },
              { label: "Served units", value: selectedDevice.servedUnits.length ? String(selectedDevice.servedUnits.length) : "0" },
            ].map((item) => (
              <div key={item.label} style={{ borderRadius: 8, border: "1px solid #1e293b", background: "#020617", padding: "8px 10px" }}>
                <div style={{ fontSize: 9, color: "#475569" }}>{item.label}</div>
                <div style={{ fontSize: 12, color: "#e2e8f0", fontWeight: 600, marginTop: 2 }}>{item.value}</div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      <div style={{ display: "grid", gridTemplateColumns: "1.15fr 0.85fr", gap: 14, marginBottom: 16 }}>
        <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14 }}>
          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>LIVE DEVICES</div>
          <div style={{ display: "grid", gap: 8 }}>
            {devices.map((device) => (
              <div key={device.identity} style={{ border: "1px solid #1e293b", borderRadius: 8, padding: "10px 12px", background: "#030712" }}>
                <button
                  onClick={() => openDeviceDetails(device.identity)}
                  style={{ background: "none", border: "none", padding: 0, color: "#93c5fd", cursor: "pointer", fontSize: 12, fontWeight: 600, textDecoration: "underline", textAlign: "left" }}
                >
                  {device.identity}
                </button>
                <div style={{ fontSize: 10, color: "#64748b", marginTop: 4 }}>
                  {device.model} · {device.ip} · {device.version}
                </div>
              </div>
            ))}
          </div>
        </div>

        <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14 }}>
          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>BLOCK SUMMARY</div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
              {[
                { id: "devices" as const, label: "Devices", value: String(building.deviceCount), color: "#60a5fa" },
                { id: "ports" as const, label: summaryPortLabel, value: summaryPortValue, color: "#22c55e" },
                { id: "flaps" as const, label: "Flap ports", value: String(building.flapHistory?.count ?? 0), color: "#f59e0b" },
                { id: "units" as const, label: "Known units", value: String(building.knownUnits.length), color: "#c084fc" },
                { id: "matches" as const, label: "Exact matches", value: String(building.buildingModel?.coverage.exact_unit_port_match_count ?? 0), color: "#38bdf8" },
                { id: "coverage" as const, label: "Model coverage", value: `${exactCoverage}%`, color: "#f97316" },
              ].map((item) => (
                <button
                  key={item.label}
                  onClick={() => setSummaryFocus((current) => current === item.id ? null : item.id)}
                  style={{
                    background: summaryFocus === item.id ? "#111827" : "#0a0f1a",
                    borderRadius: 8,
                    padding: 10,
                    border: `1px solid ${summaryFocus === item.id ? item.color : "#0f172a"}`,
                    cursor: "pointer",
                    textAlign: "left",
                  }}
                >
                  <div style={{ fontSize: 9, color: "#475569", marginBottom: 3 }}>{item.label}</div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: item.color }}>{item.value}</div>
                </button>
              ))}
            </div>
          </div>
        </div>

      {summaryFocus ? (
        <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14, marginBottom: 16 }}>
          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>
            {summaryFocus === "devices" ? "DEVICE DETAIL" :
              summaryFocus === "ports" ? (building.customerCount > 0 ? "LIVE PORT DETAIL" : "ONLINE UNIT EVIDENCE") :
              summaryFocus === "flaps" ? "FLAP PORT DETAIL" :
              summaryFocus === "units" ? "UNIT INVENTORY" :
              summaryFocus === "matches" ? "EXACT UNIT MATCHES" :
              "MODEL COVERAGE DETAIL"}
          </div>
          {summaryFocus === "devices" ? (
            <div style={{ display: "grid", gap: 8 }}>
              {devices.map((device) => (
                <div key={device.identity} style={{ border: "1px solid #0f172a", borderRadius: 8, padding: "10px 12px", background: "#030712" }}>
                  <button
                    onClick={() => openDeviceDetails(device.identity)}
                    style={{ background: "none", border: "none", padding: 0, color: "#93c5fd", cursor: "pointer", fontSize: 12, fontWeight: 700, textDecoration: "underline", textAlign: "left" }}
                  >
                    {device.identity}
                  </button>
                  <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 4 }}>{device.model} · {device.ip} · {device.version}</div>
                </div>
              ))}
            </div>
          ) : null}
          {summaryFocus === "ports" ? (
            building.customerCount > 0 ? (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 8 }}>
                {ports.map((port) => (
                  <button
                    key={portRenderKey(port)}
                    onClick={() => onSelectPort(port)}
                    style={{ textAlign: "left", border: `1px solid ${STATUS_COLOR[port.status]}55`, borderRadius: 8, padding: "10px 12px", background: "#030712", cursor: "pointer" }}
                  >
                    <div style={{ fontSize: 12, color: "#e2e8f0", fontWeight: 700 }}>{ifaceLabel(port.on_interface)}</div>
                    <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 4 }}>{port.identity}</div>
                    <div style={{ fontSize: 10, color: STATUS_COLOR[port.status], marginTop: 4 }}>{port.statusLabel}</div>
                  </button>
                ))}
              </div>
            ) : evidenceOnlineCount > 0 ? (
              <div style={{ display: "grid", gap: 8 }}>
                {unitBoxes.filter((unit) => unit.status === "online").map((unit) => (
                  <button
                    key={unit.unit}
                    onClick={() => setSelectedUnitId(unit.unit)}
                    style={{ textAlign: "left", border: "1px solid #166534", borderRadius: 8, padding: "10px 12px", background: "#03120a", cursor: "pointer" }}
                  >
                    <div style={{ fontSize: 12, color: "#dcfce7", fontWeight: 700 }}>{unit.unit}</div>
                    <div style={{ fontSize: 10, color: "#86efac", marginTop: 4 }}>Evidence-backed online unit</div>
                    <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 4 }}>
                      {building.buildingModel?.unit_state_decisions.find((row) => row.unit === unit.unit)?.sources.join(", ") ?? "inventory"}
                    </div>
                  </button>
                ))}
              </div>
            ) : (
              <div style={{ fontSize: 11, color: "#64748b" }}>No live port or unit-online evidence is currently available for this building.</div>
            )
          ) : null}
          {summaryFocus === "flaps" ? (
            sortedFlapPorts.length ? (
              <div style={{ display: "grid", gap: 8 }}>
                {sortedFlapPorts.map((issue, index) => {
                  const matchedPort = portsByIssueKey.get(portKey(issue.identity, issue.interface));
                  return (
                    <div key={`${issue.identity}-${issue.interface}-${index}`} style={{ border: "1px solid #0f172a", borderRadius: 8, padding: "10px 12px", background: "#030712" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
                        <div>
                          <div style={{ fontSize: 12, color: "#e2e8f0", fontWeight: 700 }}>
                            <button
                              onClick={() => openDeviceDetails(issue.identity)}
                              style={{ background: "none", border: "none", padding: 0, color: "#93c5fd", cursor: "pointer", font: "inherit", textDecoration: "underline" }}
                            >
                              {issue.identity}
                            </button>{" "}
                            · {ifaceLabel(issue.interface)}
                          </div>
                          <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 4 }}>
                            {(issue.issues ?? []).join(" · ") || issue.comment || "Flap history detected"}
                          </div>
                        </div>
                        {matchedPort ? (
                          <button
                            onClick={() => onSelectPort(matchedPort)}
                            style={{ border: "1px solid #164e63", background: "#082f49", color: "#bae6fd", borderRadius: 8, padding: "8px 10px", cursor: "pointer", fontSize: 11, fontWeight: 700 }}
                          >
                            Open Port
                          </button>
                        ) : null}
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <div style={{ fontSize: 11, color: "#64748b" }}>No flap ports are currently recorded for this building.</div>
            )
          ) : null}
          {summaryFocus === "units" ? (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              {building.knownUnits.map((unit) => (
                <button
                  key={unit}
                  onClick={() => setSelectedUnitId(unit)}
                  style={{
                    border: `1px solid ${selectedUnitId === unit ? "#38bdf8" : "#1e293b"}`,
                    background: selectedUnitId === unit ? "#082f49" : "#030712",
                    color: "#e2e8f0",
                    borderRadius: 8,
                    padding: "8px 10px",
                    cursor: "pointer",
                    fontSize: 11,
                    fontWeight: 700,
                  }}
                >
                  {unit}
                </button>
              ))}
            </div>
          ) : null}
          {summaryFocus === "matches" ? (
            sortedExactMatches.length ? (
              <div style={{ display: "grid", gap: 8 }}>
                {sortedExactMatches.map((match) => {
                  const matchedPort = portsByIssueKey.get(portKey(match.switch_identity, match.interface));
                  return (
                    <div key={`${match.unit}-${match.switch_identity}-${match.interface}`} style={{ border: "1px solid #0f172a", borderRadius: 8, padding: "10px 12px", background: "#030712" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                        <div>
                          <div style={{ fontSize: 12, color: "#e2e8f0", fontWeight: 700 }}>{match.unit}</div>
                          <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 4 }}>
                            <button
                              onClick={() => openDeviceDetails(match.switch_identity)}
                              style={{ background: "none", border: "none", padding: 0, color: "#93c5fd", cursor: "pointer", font: "inherit", textDecoration: "underline" }}
                            >
                              {match.switch_identity}
                            </button>{" "}
                            · {match.interface} · {match.mac}
                          </div>
                        </div>
                        {matchedPort ? (
                          <button
                            onClick={() => onSelectPort(matchedPort)}
                            style={{ border: "1px solid #164e63", background: "#082f49", color: "#bae6fd", borderRadius: 8, padding: "8px 10px", cursor: "pointer", fontSize: 11, fontWeight: 700 }}
                          >
                            Open Port
                          </button>
                        ) : null}
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <div style={{ fontSize: 11, color: "#64748b" }}>No exact unit-port matches are available yet.</div>
            )
          ) : null}
          {summaryFocus === "coverage" ? (
            <div style={{ display: "grid", gap: 10 }}>
              <div style={{ border: "1px solid #0f172a", borderRadius: 8, padding: "10px 12px", background: "#030712" }}>
                <div style={{ fontSize: 11, color: "#e2e8f0", fontWeight: 700 }}>{exactCoverage}% exact model coverage</div>
                <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 4 }}>
                  {building.buildingModel?.coverage.exact_unit_port_match_count ?? 0} exact unit-port matches out of {building.buildingModel?.coverage.known_unit_count ?? 0} known units.
                </div>
              </div>
              <div style={{ border: "1px solid #0f172a", borderRadius: 8, padding: "10px 12px", background: "#030712" }}>
                <div style={{ fontSize: 10, color: "#475569", marginBottom: 4 }}>Switch placement</div>
                <div style={{ fontSize: 11, color: "#cbd5e1" }}>{building.buildingModel?.data_gaps.switch_floor_placement ?? "No note available."}</div>
              </div>
              <div style={{ border: "1px solid #0f172a", borderRadius: 8, padding: "10px 12px", background: "#030712" }}>
                <div style={{ fontSize: 10, color: "#475569", marginBottom: 4 }}>Geometry</div>
                <div style={{ fontSize: 11, color: "#cbd5e1" }}>{building.buildingModel?.data_gaps.building_geometry ?? "No note available."}</div>
              </div>
            </div>
          ) : null}
        </div>
      ) : null}

      {building.buildingModel ? (
        <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14, marginBottom: 16 }}>
          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>BUILDING MODEL</div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
            <div style={{ display: "grid", gap: 8 }}>
              {building.buildingModel.switches.map((entry) => (
                <div key={entry.identity} style={{ border: "1px solid #0f172a", borderRadius: 8, padding: "10px 12px", background: "#030712" }}>
                  <button
                    onClick={() => openDeviceDetails(entry.identity)}
                    style={{ background: "none", border: "none", padding: 0, color: "#93c5fd", cursor: "pointer", fontSize: 12, fontWeight: 700, textDecoration: "underline", textAlign: "left" }}
                  >
                    {entry.identity}
                  </button>
                  <div style={{ fontSize: 10, color: "#64748b", marginTop: 4 }}>
                    Exact floors: {entry.served_floors.length ? entry.served_floors.map((floor) => String(floor).padStart(2, "0")).join(", ") : "none"} · exact units: {entry.exact_match_count}
                  </div>
                </div>
              ))}
            </div>
            <div style={{ display: "grid", gap: 8 }}>
              {(building.buildingModel.direct_neighbor_edges.slice(0, 6)).map((edge, index) => (
                <div key={`${edge.from_identity}-${edge.from_interface}-${edge.to_identity}-${index}`} style={{ border: "1px solid #0f172a", borderRadius: 8, padding: "10px 12px", background: "#030712" }}>
                  <div style={{ fontSize: 11, color: "#e2e8f0", fontWeight: 700 }}>
                    <button
                      onClick={() => openDeviceDetails(edge.from_identity)}
                      style={{ background: "none", border: "none", padding: 0, color: "#93c5fd", cursor: "pointer", font: "inherit", textDecoration: "underline" }}
                    >
                      {edge.from_identity}
                    </button>{" "}
                    · {edge.from_interface}
                  </div>
                  <div style={{ fontSize: 10, color: "#64748b", marginTop: 4 }}>
                    →{" "}
                    {edge.to_identity.includes(".") ? (
                      <button
                        onClick={() => openDeviceDetails(edge.to_identity)}
                        style={{ background: "none", border: "none", padding: 0, color: "#93c5fd", cursor: "pointer", font: "inherit", textDecoration: "underline" }}
                      >
                        {edge.to_identity}
                      </button>
                    ) : edge.to_identity}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      ) : null}

      <div style={{ fontSize: 12, color: "#64748b", marginBottom: 10 }}>Customer-facing access ports</div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 10 }}>
        {ports.map((port) => (
          <button
            key={portRenderKey(port)}
            onClick={() => onSelectPort(port)}
            style={{
              textAlign: "left",
              background: "#020617",
              border: `1px solid ${STATUS_COLOR[port.status]}55`,
              borderRadius: 10,
              padding: 12,
              cursor: "pointer",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
              <span style={{ fontSize: 11, fontWeight: 700, color: "#e2e8f0" }}>{ifaceLabel(port.on_interface)}</span>
              <span style={{ fontSize: 9, color: STATUS_COLOR[port.status] }}>{port.statusLabel}</span>
            </div>
            <div style={{ fontSize: 11, color: "#cbd5e1", marginTop: 6, fontWeight: 600 }}>{port.mac}</div>
            <div style={{ fontSize: 10, color: "#64748b", marginTop: 6 }}>{port.identity}</div>
            <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 6 }}>{vendorFromMac(port.mac)} · VLAN {port.vid}</div>
          </button>
        ))}
      </div>
    </div>
  );
}

function PortView({
  building,
  port,
  path,
  cpeContext,
  onBack,
}: {
  building: BuildingLive;
  port: PortWithStatus;
  path: Array<{ label: string; status: Status }>;
  cpeContext: CpeContext | null;
  onBack: () => void;
}) {
  return (
    <div style={{ padding: "0 4px" }}>
      <button
        onClick={onBack}
        style={{ background: "none", border: "none", color: "#60a5fa", cursor: "pointer", fontSize: 13, padding: "0 0 12px", display: "flex", alignItems: "center", gap: 6 }}
      >
        ← back to {building.shortLabel}
      </button>

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 16 }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 18, fontWeight: 600, color: "#f1f5f9" }}>
            {port.mac} · {ifaceLabel(port.on_interface)}
          </h2>
          <p style={{ margin: "3px 0 0", fontSize: 12, color: "#64748b" }}>
            {port.identity} · {building.address}
          </p>
        </div>
        <span
          style={{
            fontSize: 11,
            padding: "3px 10px",
            borderRadius: 12,
            background: STATUS_BG[port.status],
            color: STATUS_COLOR[port.status],
            border: `1px solid ${STATUS_COLOR[port.status]}40`,
          }}
        >
          {port.statusLabel}
        </span>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 320px", gap: 16 }}>
        <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14 }}>
          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>NETWORK PATH</div>
          {path.map((hop, index) => (
            <div key={hop.label}>
              <div style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 12px", borderRadius: 8, border: "1px solid #0f172a", background: "#030712" }}>
                <div style={{ width: 10, height: 10, borderRadius: "50%", background: STATUS_COLOR[hop.status] }} />
                <div style={{ color: "#e2e8f0", fontSize: 12, flex: 1 }}>{hop.label}</div>
                <div style={{ color: STATUS_COLOR[hop.status], fontSize: 10 }}>{hop.status}</div>
              </div>
              {index < path.length - 1 ? <div style={{ width: 2, height: 10, background: "#1e293b", margin: "0 0 0 16px" }} /> : null}
            </div>
          ))}
        </div>

        <div>
          <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14, marginBottom: 12 }}>
            <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>PORT FACTS</div>
            <InfoGrid
              items={[
                { label: "Switch IP", value: port.ip },
                { label: "MAC", value: port.mac },
                { label: "Vendor", value: vendorLabel(cpeContext?.vendor ?? vendorFromMac(port.mac)) },
                { label: "VLAN", value: String(port.vid) },
              ]}
            />
          </div>

          <div style={{ marginBottom: 12 }}>
            <CpeContextPanel vendor={cpeContext?.vendor ?? vendorFromMac(port.mac)} vilo={cpeContext?.vilo ?? null} tauc={cpeContext?.tauc ?? null} />
          </div>

          <div style={{ background: "#020617", border: "1px solid #1e293b", borderRadius: 10, padding: 14 }}>
            <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>DIAGNOSTIC NOTES</div>
            {port.notes.length ? (
              port.notes.map((note) => (
                <div key={note} style={{ fontSize: 11, color: "#cbd5e1", padding: "8px 10px", background: "#030712", borderRadius: 8, border: "1px solid #0f172a", marginBottom: 8 }}>
                  {note}
                </div>
              ))
            ) : (
              <div style={{ fontSize: 11, color: "#64748b" }}>No open flags for this access port in the latest pull.</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default function NychaNoc() {
  const [view, setView] = useState<"map" | "building" | "port" | "radio">("map");
  const [selectedBuildingId, setSelectedBuildingId] = useState<string | null>(null);
  const [selectedPortKey, setSelectedPortKey] = useState<string | null>(null);
  const [selectedRadioId, setSelectedRadioId] = useState<string | null>(null);
  const [buildingBlocksOpen, setBuildingBlocksOpen] = useState(false);
  const [siteSummary, setSiteSummary] = useState<SiteSummary | null>(null);
  const [siteTopology, setSiteTopology] = useState<SiteTopology | null>(null);
  const [buildingData, setBuildingData] = useState<Record<string, BuildingDataEntry>>({});
  const [cpeContextData, setCpeContextData] = useState<Record<string, CpeContext>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const restoredUiStateRef = useRef(false);

  const legacyLayoutById = useMemo(() => new Map(BUILDING_LAYOUTS.map((layout) => [layout.id, layout])), []);

  const buildingLayouts = useMemo<BuildingLayout[]>(() => {
    const entries = new Map<string, BuildingLayout>();
    const topologyAddressesByBuilding = new Map<string, Array<SiteTopology["addresses"][number]>>();
    const topologyRadiosByBuilding = new Map<string, Array<SiteTopology["radios"][number]>>();
    const handledBuildingIds = new Set<string>();

    for (const address of siteTopology?.addresses ?? []) {
      if (!address.building_id) continue;
      const rows = topologyAddressesByBuilding.get(address.building_id) ?? [];
      rows.push(address);
      topologyAddressesByBuilding.set(address.building_id, rows);
    }

    for (const radio of siteTopology?.radios ?? []) {
      if (!radio.resolved_building_id) continue;
      const rows = topologyRadiosByBuilding.get(radio.resolved_building_id) ?? [];
      rows.push(radio);
      topologyRadiosByBuilding.set(radio.resolved_building_id, rows);
    }

    for (const layout of BUILDING_LAYOUTS) {
      entries.set(layout.id, layout);
    }

    for (const building of siteTopology?.buildings ?? []) {
      handledBuildingIds.add(building.building_id);
      const legacy = legacyLayoutById.get(building.building_id);
      const relatedAddresses = topologyAddressesByBuilding.get(building.building_id) ?? [];
      const profile = BUILDING_PROFILES[building.building_id];
      const relatedRadios = topologyRadiosByBuilding.get(building.building_id) ?? [];
      if (!relatedAddresses.length) {
        const displayAddress = legacy?.address ?? relatedRadios[0]?.location ?? building.building_id;
        entries.set(building.building_id, {
          id: building.building_id,
          sourceBuildingId: building.building_id,
          name: legacy?.name ?? deriveBuildingName(displayAddress),
          shortLabel: legacy?.shortLabel ?? shortAddressLabel(displayAddress),
          address: displayAddress,
          development: legacy?.development ?? "Network topology site",
          floors: profile?.authoritativeFloors ?? legacy?.floors ?? inferFloorsFromUnits(building.known_units ?? []),
          x: legacy?.x ?? 0,
          y: legacy?.y ?? 0,
        });
        continue;
      }
      const duplicateCount = relatedAddresses.length;
      for (const addressEntry of relatedAddresses) {
        const address = legacy?.address === addressEntry.address ? legacy.address : addressEntry.address;
        entries.set(addressLayoutId(address, building.building_id, duplicateCount), {
          id: addressLayoutId(address, building.building_id, duplicateCount),
          sourceBuildingId: building.building_id,
          name: legacy?.address === addressEntry.address && legacy?.name ? legacy.name : deriveBuildingName(address),
          shortLabel: legacy?.address === addressEntry.address && legacy?.shortLabel ? legacy.shortLabel : shortAddressLabel(address),
          address,
          development: legacy?.address === addressEntry.address && legacy?.development ? legacy.development : "Network topology site",
          floors: profile?.authoritativeFloors ?? legacy?.floors ?? inferFloorsFromUnits(addressEntry.units ?? building.known_units ?? []),
          x: legacy?.x ?? 0,
          y: legacy?.y ?? 0,
        });
      }
    }

    for (const [buildingId, relatedAddresses] of topologyAddressesByBuilding.entries()) {
      if (handledBuildingIds.has(buildingId)) continue;
      const legacy = legacyLayoutById.get(buildingId);
      const profile = BUILDING_PROFILES[buildingId];
      const duplicateCount = relatedAddresses.length;
      for (const addressEntry of relatedAddresses) {
        const address = legacy?.address === addressEntry.address ? legacy.address : addressEntry.address;
        entries.set(addressLayoutId(address, buildingId, duplicateCount), {
          id: addressLayoutId(address, buildingId, duplicateCount),
          sourceBuildingId: buildingId,
          name: legacy?.address === addressEntry.address && legacy?.name ? legacy.name : deriveBuildingName(address),
          shortLabel: legacy?.address === addressEntry.address && legacy?.shortLabel ? legacy.shortLabel : shortAddressLabel(address),
          address,
          development: legacy?.address === addressEntry.address && legacy?.development ? legacy.development : "Network topology site",
          floors: profile?.authoritativeFloors ?? legacy?.floors ?? inferFloorsFromUnits(addressEntry.units ?? []),
          x: legacy?.x ?? 0,
          y: legacy?.y ?? 0,
        });
      }
    }

    for (const addressEntry of siteTopology?.addresses ?? []) {
      if (addressEntry.building_id) continue;
      const derivedId = siteIdForAddress(addressEntry.address);
      if (entries.has(derivedId)) continue;
      entries.set(derivedId, {
        id: derivedId,
        sourceBuildingId: derivedId,
        name: deriveBuildingName(addressEntry.address),
        shortLabel: shortAddressLabel(addressEntry.address),
        address: addressEntry.address,
        development: "Network topology site",
        floors: inferFloorsFromUnits(addressEntry.units ?? []),
        x: 0,
        y: 0,
      });
    }

    for (const radio of siteTopology?.radios ?? []) {
      if (radio.resolved_building_id) continue;
      const coord = getCoord(radio.location, siteTopology, null);
      if (!coord) continue;
      const derivedId = siteIdForAddress(radio.location);
      entries.set(derivedId, {
        id: derivedId,
        sourceBuildingId: derivedId,
        name: deriveBuildingName(radio.location),
        shortLabel: shortAddressLabel(radio.location),
        address: radio.location,
        development: "Radio-attached site",
        floors: inferFloorsFromUnits(radio.address_units ?? []),
        x: 0,
        y: 0,
      });
    }

    return [...entries.values()].sort((a, b) => a.name.localeCompare(b.name));
  }, [legacyLayoutById, siteTopology]);

  useEffect(() => {
    let active = true;
    let retryTimer: number | null = null;

    async function load() {
      try {
        const baseUrl = DEFAULT_JAKE_BASE_URL;
        const [site, topology] = await Promise.all([
          fetchJakeJson<SiteSummary>(baseUrl, "/api/site-summary?site_id=000007&include_alerts=false"),
          fetchJakeJson<SiteTopology>(baseUrl, "/api/site-topology?site_id=000007"),
        ]);
        setSiteSummary(site);
        setSiteTopology(topology);
        if (!active) return;
        setError(null);
      } catch (loadError) {
        if (!active) return;
        setError(loadError instanceof Error ? loadError.message : "Data fetch failed");
        if (retryTimer === null) {
          retryTimer = window.setTimeout(() => {
            retryTimer = null;
            if (active) void load();
          }, 5000);
        }
      } finally {
        if (active) setLoading(false);
      }
    }

    load();
    const timer = window.setInterval(load, 30000);
    return () => {
      active = false;
      if (retryTimer !== null) window.clearTimeout(retryTimer);
      window.clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    if (!buildingLayouts.length) return;
    if (!restoredUiStateRef.current && typeof window !== "undefined") {
      restoredUiStateRef.current = true;
      try {
        const raw = window.localStorage.getItem(UI_STATE_STORAGE_KEY);
        if (raw) {
          const saved = JSON.parse(raw) as {
            view?: "map" | "building" | "port" | "radio";
            selectedBuildingId?: string | null;
            selectedPortKey?: string | null;
            selectedRadioId?: string | null;
          };
          if (saved.selectedBuildingId && buildingLayouts.some((building) => building.id === saved.selectedBuildingId)) {
            setSelectedBuildingId(saved.selectedBuildingId);
          }
          if (saved.view) setView(saved.view);
          if (saved.selectedPortKey) setSelectedPortKey(saved.selectedPortKey);
          if (saved.selectedRadioId) setSelectedRadioId(saved.selectedRadioId);
        }
      } catch {
        // ignore invalid persisted state and fall back to default selection
      }
    }
    if (selectedBuildingId && buildingLayouts.some((building) => building.id === selectedBuildingId)) return;
    setSelectedBuildingId(buildingLayouts.find((building) => building.id === "000007.004")?.id ?? buildingLayouts[0]?.id ?? null);
  }, [buildingLayouts, selectedBuildingId]);

  useEffect(() => {
    if (typeof window === "undefined" || !restoredUiStateRef.current) return;
    window.localStorage.setItem(
      UI_STATE_STORAGE_KEY,
      JSON.stringify({
        view,
        selectedBuildingId,
        selectedPortKey,
        selectedRadioId,
      }),
    );
  }, [selectedBuildingId, selectedPortKey, selectedRadioId, view]);

  const buildings = useMemo<BuildingLive[]>(() => {
    const alerts = siteSummary?.active_alerts ?? [];
    return buildingLayouts.map((layout) => {
      const live = buildingData[canonicalBuildingIdOf(layout)];
      const topologyBuilding = (siteTopology?.buildings ?? []).find((entry) => entry.building_id === canonicalBuildingIdOf(layout));
      const addressRecord = siteTopology?.addresses.find((entry) => entry.address === layout.address)
        ?? siteTopology?.addresses.find((entry) => entry.building_id === canonicalBuildingIdOf(layout));
      const matchingAlerts = filterAlertsForBuilding(layout, alerts);
      const status = deriveBuildingStatus(
        layout,
        matchingAlerts.length,
        live?.buildingHealth?.outlier_count ?? topologyBuilding?.health?.outlier_count ?? 0,
        live?.flapHistory?.count ?? 0,
        live?.rogueDhcp?.count ?? 0,
        live?.recoveryReady?.count ?? 0,
      );
      return {
        ...layout,
        status,
        customerCount: live?.buildingCustomerCount?.count ?? topologyBuilding?.customer_count ?? 0,
        deviceCount: live?.buildingHealth?.device_count ?? topologyBuilding?.health?.device_count ?? 0,
        alertCount: matchingAlerts.length,
        outlierCount: live?.buildingHealth?.outlier_count ?? topologyBuilding?.health?.outlier_count ?? 0,
        knownUnits: addressRecord?.units?.length ? addressRecord.units : (live?.buildingModel?.known_units ?? topologyBuilding?.known_units ?? []),
        profile: synthesizeProfile({
          id: canonicalBuildingIdOf(layout),
          sourceBuildingId: canonicalBuildingIdOf(layout),
          floors: layout.floors,
          knownUnits: addressRecord?.units?.length ? addressRecord.units : (live?.buildingModel?.known_units ?? topologyBuilding?.known_units ?? []),
          buildingModel: live?.buildingModel,
          buildingCustomerCount: live?.buildingCustomerCount,
        }),
        buildingHealth: live?.buildingHealth ?? topologyBuilding?.health,
        buildingCustomerCount: live?.buildingCustomerCount ?? (topologyBuilding ? {
          building_id: topologyBuilding.building_id,
          count: topologyBuilding.customer_count,
          access_port_count: topologyBuilding.customer_count,
          switch_count: topologyBuilding.health?.device_count ?? 0,
          vendor_summary: {},
          results: [],
        } : undefined),
        buildingModel: live?.buildingModel,
        flapHistory: live?.flapHistory,
        rogueDhcp: live?.rogueDhcp,
        recoveryReady: live?.recoveryReady,
        cpeIntelligence: live?.cpeIntelligence,
      };
    });
  }, [buildingData, buildingLayouts, siteSummary, siteTopology]);

  useEffect(() => {
    if (!selectedBuildingId) return;
    const selectedLayout = buildingLayouts.find((building) => building.id === selectedBuildingId);
    if (!selectedLayout) return;

    const current = parseAddressStem(selectedLayout.address);
    const nearbyIds = buildingLayouts
      .filter((candidate) => {
        const parsed = parseAddressStem(candidate.address);
        if (!current.street || parsed.street !== current.street) return false;
        if (current.number == null || parsed.number == null) return false;
        return Math.abs(parsed.number - current.number) <= 30;
      })
      .map((candidate) => canonicalBuildingIdOf(candidate));
    const ids = [...new Set([canonicalBuildingIdOf(selectedLayout), ...nearbyIds])]
      .filter((buildingId) => /^\d{6}\.\d{3}$/.test(buildingId))
      .filter((buildingId) => !hasHydratedBuildingData(buildingData[buildingId]));
    if (!ids.length) return;

    let active = true;
    void (async () => {
      const baseUrl = DEFAULT_JAKE_BASE_URL;
      await Promise.allSettled(
        ids.map(async (buildingId) => {
          const [
            health,
            customerCount,
            buildingModel,
            flapHistory,
            rogueDhcp,
            recoveryReady,
          ] = await Promise.all([
            fetchJakeJson<BuildingHealth>(baseUrl, `/api/building-health?building_id=${buildingId}&include_alerts=false`),
            fetchJakeJson<BuildingCustomerCount>(baseUrl, `/api/building-customer-count?building_id=${buildingId}`),
            fetchJakeJson<BuildingModel>(baseUrl, `/api/building-model?building_id=${buildingId}`),
            fetchJakeJson<IssueResponse>(baseUrl, `/api/building-flap-history?building_id=${buildingId}`),
            fetchJakeJson<IssueResponse>(baseUrl, `/api/rogue-dhcp-suspects?building_id=${buildingId}`),
            fetchJakeJson<IssueResponse>(baseUrl, `/api/recovery-ready-cpes?building_id=${buildingId}`),
          ]);
          if (!active) return;
          setBuildingData((prev) => ({
            ...prev,
            [buildingId]: {
              buildingHealth: health,
              buildingCustomerCount: customerCount,
              buildingModel,
              flapHistory,
              rogueDhcp,
              recoveryReady,
            },
          }));
        }),
      );
    })();

    return () => {
      active = false;
    };
  }, [buildingData, buildingLayouts, selectedBuildingId]);

  const selectedBuilding = buildings.find((building) => building.id === selectedBuildingId) ?? buildings[0] ?? null;
  const selectedBuildingLoadingModel = selectedBuilding ? !hasHydratedBuildingData(buildingData[canonicalBuildingIdOf(selectedBuilding)]) : false;
  useEffect(() => {
    if (!selectedBuilding) return;
    const buildingId = canonicalBuildingIdOf(selectedBuilding);
    if (!/^\d{6}\.\d{3}$/.test(buildingId)) return;
    if (buildingData[buildingId]?.cpeIntelligence) return;
    let active = true;
    void (async () => {
      try {
        const baseUrl = DEFAULT_JAKE_BASE_URL;
        const cpeIntelligence = await fetchJakeJson<BuildingCpeIntelligence>(baseUrl, `/v1/jake/buildings/${buildingId}/cpes?limit=200`);
        if (!active) return;
        setBuildingData((prev) => ({
          ...prev,
          [buildingId]: {
            ...prev[buildingId],
            cpeIntelligence,
          },
        }));
      } catch {
        // leave CPE intelligence absent when upstream systems are unavailable
      }
    })();
    return () => {
      active = false;
    };
  }, [buildingData, selectedBuilding]);
  const radios = useMemo<RadioLive[]>(() => {
    if (siteTopology?.radios?.length) {
      const grouped = new Map<string, SiteTopology["radios"]>();
      for (const radio of siteTopology.radios) {
        const key = radio.resolved_building_id || "unresolved";
        const rows = grouped.get(key) ?? [];
        rows.push(radio);
        grouped.set(key, rows);
      }
      const findAnchorBuilding = (radio: SiteTopology["radios"][number]) => {
        const exactAddressMatch = buildings.find((building) => building.address === radio.location);
        if (exactAddressMatch) return exactAddressMatch;
        const candidates = radio.resolved_building_id
          ? buildings.filter((building) => canonicalBuildingIdOf(building) === radio.resolved_building_id)
          : [];
        if (!candidates.length) return null;
        const normalizedMatch = candidates.find((building) => buildingMatchesEndpoint(building, radio.name, radio.location));
        if (normalizedMatch) return normalizedMatch;
        const radioCoord = getCoord(radio.location, siteTopology, radio.resolved_building_id);
        return [...candidates].sort((a, b) => {
          const aCoord = getCoord(a.address, siteTopology, canonicalBuildingIdOf(a));
          const bCoord = getCoord(b.address, siteTopology, canonicalBuildingIdOf(b));
          return distanceBetweenCoords(radioCoord, aCoord) - distanceBetweenCoords(radioCoord, bCoord);
        })[0] ?? null;
      };
      const unresolvedBaseX = 615;
      const unresolvedBaseY = 120;
      return siteTopology.radios.map((radio) => {
        const anchor = findAnchorBuilding(radio);
        const addressRecord = siteTopology.addresses.find((address) => address.address === radio.location);
        const siblings = grouped.get(radio.resolved_building_id || "unresolved") ?? [radio];
        const index = siblings.findIndex((entry) => entry.name === radio.name);
        const step = siblings.length <= 1 ? 0 : index - (siblings.length - 1) / 2;
        const x = anchor ? anchor.x + 52 + step * 28 : unresolvedBaseX;
        const y = anchor ? anchor.y - 62 - Math.abs(step) * 6 : unresolvedBaseY + index * 28;
        const alert = radio.alerts?.[0] ?? null;
        return {
          id: radioIdFromName(radio.name),
          name: radio.name,
          shortLabel: shortRadioLabel(radio.name),
          address: radio.location,
          model: radio.model,
          role: radio.type === "cambium" ? inferCambiumRole(radio.name, radio.model) : radio.type === "siklu" ? "Siklu link" : "Radio",
          anchorBuildingId: anchor?.id || radio.resolved_building_id || siteIdForAddress(radio.location),
          x,
          y,
          status: radioStatusFromJake(radio.status, radio.alerts?.length ?? 0),
          alert,
          knownUnits: addressRecord?.units ?? radio.address_units ?? [],
          latitude: radio.latitude,
          longitude: radio.longitude,
        };
      });
    }

    return BUILDING_LAYOUTS.map((layout) => ({
      id: radioIdFromName(`${layout.sourceBuildingId}-fallback-radio`),
      name: `${layout.shortLabel} fallback transport`,
      shortLabel: `${layout.shortLabel} link`,
      address: layout.address,
      model: "Legacy transport",
      role: "Fallback transport node",
      anchorBuildingId: layout.id,
      x: layout.x + 44,
      y: layout.y - 48,
      status: "unknown" as const,
      knownUnits: [],
      latitude: null,
      longitude: null,
    }));
  }, [buildingLayouts, buildings, siteTopology]);
  const selectedRadio = radios.find((radio) => radio.id === selectedRadioId) ?? null;

  const radioLinks = useMemo(() => {
    const findRadioEndpoint = (label?: string | null, location?: string | null) =>
      radios.find((radio) => radioMatchesEndpoint(radio, label, location)) ?? null;

    const liveLinks = (siteTopology?.radio_links ?? [])
      .filter((link) => (link.kind ?? "").toLowerCase() === "siklu")
      .filter((link) => Boolean(link.from_building_id && link.to_building_id))
      .map((link) => {
        const fromBuilding = findMatchingBuilding(buildings, link.from_label, link.from_building_id, link.location);
        const toBuilding = findMatchingBuilding(buildings, link.to_label, link.to_building_id, null);
        const fromRadio = link.from_radio_id
          ? radios.find((radio) => radio.id === link.from_radio_id)
          : findRadioEndpoint(link.from_label, link.location);
        const toRadio = link.to_radio_id
          ? radios.find((radio) => radio.id === link.to_radio_id)
          : findRadioEndpoint(link.to_label, null);
        if (!fromBuilding && !fromRadio) return null;
        if (!toBuilding && !toRadio) return null;
        return {
          fromRadioId: fromRadio?.id,
          toRadioId: toRadio?.id,
          fromBuildingId: fromBuilding?.id,
          toBuildingId: toBuilding?.id,
          strength: link.status === "ok" ? "strong" as const : "weak" as const,
          kind: link.kind === "siklu" ? "Siklu transport" : link.kind,
        };
      })
      .filter(Boolean) as Array<{ fromRadioId?: string; toRadioId?: string; fromBuildingId?: string; toBuildingId?: string; strength: "strong" | "medium" | "weak"; kind: string }>;

    if (liveLinks.length) return liveLinks;

    return LEGACY_TRANSPORT_LINKS.map((link) => {
      const fromBuilding = buildings.find((building) => canonicalBuildingIdOf(building) === link.from);
      const toBuilding = buildings.find((building) => canonicalBuildingIdOf(building) === link.to);
      const fromRadio = fromBuilding
        ? radios.find((radio) => radio.anchorBuildingId === fromBuilding.id)
        : null;
      const toRadio = toBuilding
        ? radios.find((radio) => radio.anchorBuildingId === toBuilding.id)
        : null;
      if (!fromBuilding || !toBuilding) return null;
      return {
        fromRadioId: fromRadio?.id,
        toRadioId: toRadio?.id,
        fromBuildingId: fromBuilding.id,
        toBuildingId: toBuilding.id,
        strength: link.strength,
        kind: link.kind,
      };
    }).filter(Boolean) as Array<{ fromRadioId?: string; toRadioId?: string; fromBuildingId?: string; toBuildingId?: string; strength: "strong" | "medium" | "weak"; kind: string }>;
  }, [buildings, radios, siteTopology]);

  const selectedBuildingPorts = useMemo<PortWithStatus[]>(() => {
    if (!selectedBuilding) return [];

    const flapMap = new Map((selectedBuilding.flapHistory?.ports ?? []).map((port) => [portKey(port.identity, port.interface), port]));
    const rogueMap = new Map((selectedBuilding.rogueDhcp?.ports ?? []).map((port) => [portKey(port.identity, port.interface), port]));
    const recoveryMap = new Map((selectedBuilding.recoveryReady?.ports ?? []).map((port) => [portKey(port.identity, port.interface), port]));
    const issueMaps = [flapMap, rogueMap, recoveryMap];

    const issueNotes = (key: string) => {
      const flap = flapMap.get(key);
      const rogue = rogueMap.get(key);
      const recovery = recoveryMap.get(key);
      return [
        ...(rogue?.issues ?? []),
        ...(recovery?.issues ?? []),
        ...(flap?.issues ?? []),
        ...(rogue?.comment ? [rogue.comment] : []),
        ...(recovery?.comment ? [recovery.comment] : []),
        ...(flap?.comment ? [flap.comment] : []),
        ...(rogue?.fixes ?? []),
        ...(recovery?.fixes ?? []),
        ...(flap?.fixes ?? []),
      ].filter(Boolean);
    };

    const issueStatus = (key: string): Pick<PortWithStatus, "status" | "statusLabel"> => {
      const flap = flapMap.get(key);
      const rogue = rogueMap.get(key);
      const recovery = recoveryMap.get(key);
      if (rogue) return { status: "offline", statusLabel: "Rogue DHCP" };
      if (recovery) return { status: "degraded", statusLabel: recovery.status ?? "Recovery" };
      if (flap) return { status: "degraded", statusLabel: "Flapping" };
      return { status: "online", statusLabel: "Healthy" };
    };

    const hydrated = (selectedBuilding.buildingCustomerCount?.results ?? [])
      .map((port) => {
        const key = portKey(port.identity, port.on_interface);
        return { ...port, ...issueStatus(key), notes: issueNotes(key) };
      });

    const issueOnlyPorts: PortWithStatus[] = [];
    for (const issueMap of issueMaps) {
      for (const issue of issueMap.values()) {
        const key = portKey(issue.identity, issue.interface);
        if (hydrated.some((port) => portKey(port.identity, port.on_interface) === key)) continue;
        const status = issueStatus(key);
        issueOnlyPorts.push({
          identity: issue.identity,
          ip: "",
          mac: "",
          on_interface: issue.interface,
          vid: Number.parseInt(String((issue as { pvid?: string | number }).pvid ?? 0), 10) || 0,
          local: 0,
          external: 0,
          ...status,
          notes: issueNotes(key),
        });
      }
    }

    const combined = [...hydrated, ...issueOnlyPorts].sort(
      (a, b) => a.identity.localeCompare(b.identity) || compareInterfaceLabels(a.on_interface, b.on_interface),
    );

    return Array.from(new Map(combined.map((port) => [portRenderKey(port), port])).values());
  }, [selectedBuilding]);

  const selectedPort =
    selectedBuildingPorts.find((port) => portRenderKey(port) === selectedPortKey) ?? selectedBuildingPorts[0] ?? null;
  const selectedPortCpeKey = selectedBuilding && selectedPort ? cpeContextKey(canonicalBuildingIdOf(selectedBuilding), selectedPort.mac) : null;
  const selectedPortCpeContext =
    selectedPortCpeKey
      ? cpeContextData[selectedPortCpeKey]
        ?? {
          mac: normalizeMac(selectedPort?.mac ?? ""),
          vendor: vendorFromMac(selectedPort?.mac ?? ""),
          building_id: canonicalBuildingIdOf(selectedBuilding!),
          vilo: selectedBuilding ? (() => {
            const row = findViloRowByMac(selectedBuilding, selectedPort?.mac);
            return row ? {
              classification: row.classification,
              inventory_status: row.inventory_status,
              device_sn: row.device_sn,
              subscriber_id: row.subscriber_id,
              subscriber: row.subscriber,
              subscriber_hint: row.subscriber_hint,
              network: row.network,
              sighting: row.sighting,
            } : null;
          })() : null,
          tauc: selectedBuilding ? (() => {
            const row = findTaucRowByMac(selectedBuilding, selectedPort?.mac);
            return row ? {
              network_name: row.network_name,
              site_id: row.site_id,
              expected_prefix: row.expected_prefix,
              wan_mode: row.wan_mode,
              mesh_nodes: row.mesh_nodes,
              sn: row.sn,
            } : null;
          })() : null,
        }
      : null;

  useEffect(() => {
    if (!selectedBuilding || !selectedPort) return;
    const buildingId = canonicalBuildingIdOf(selectedBuilding);
    const mac = normalizeMac(selectedPort.mac);
    if (!buildingId || !mac) return;
    const key = cpeContextKey(buildingId, mac);
    if (cpeContextData[key]) return;
    let active = true;
    void (async () => {
      try {
        const baseUrl = DEFAULT_JAKE_BASE_URL;
        const result = await fetchJakeJson<CpeContext>(baseUrl, `/v1/jake/cpe-context?mac=${encodeURIComponent(mac)}&building_id=${encodeURIComponent(buildingId)}`);
        if (!active) return;
        setCpeContextData((prev) => ({
          ...prev,
          [key]: result,
        }));
      } catch {
        // keep local building-derived fallback if cloud context is unavailable
      }
    })();
    return () => {
      active = false;
    };
  }, [cpeContextData, selectedBuilding, selectedPort]);

  const breadcrumb = [
    { label: "NYCHA Map", view: "map" as const },
    ...(selectedBuilding ? [{ label: selectedBuilding.shortLabel, view: "building" as const }] : []),
    ...(selectedRadio && view !== "map" ? [{ label: selectedRadio.shortLabel, view: "building" as const }] : []),
    ...(view === "port" && selectedPort ? [{ label: selectedPort.on_interface, view: "port" as const }] : []),
  ];

  const onlineBuildings = buildings.filter((building) => building.status === "online").length;
  const degradedBuildings = buildings.filter((building) => building.status === "degraded").length;
  const offlineBuildings = buildings.filter((building) => building.status === "offline").length;
  const sidebarBuildings = useMemo(() => {
    const byId = new Map<string, BuildingLive>();
    for (const building of buildings) {
      const key = canonicalBuildingIdOf(building);
      if (!byId.has(key)) {
        byId.set(key, building);
      }
    }
    return [...byId.values()].sort((a, b) => a.shortLabel.localeCompare(b.shortLabel, undefined, { numeric: true }));
  }, [buildings]);
  const siteAlerts = siteSummary?.active_alerts ?? [];
  const selectedPortPath: Array<{ label: string; status: Status }> =
    selectedBuilding && selectedPort
      ? [
          { label: siteSummary?.online_customers?.matched_routers?.[0]?.identity ?? "Core router", status: offlineBuildings ? "degraded" : "online" },
          { label: selectedBuilding.buildingHealth?.devices.find((device) => device.identity.includes("RFSW"))?.identity ?? "Roof switch", status: selectedBuilding.status },
          { label: selectedPort.identity, status: selectedPort.status },
          { label: `${selectedPort.on_interface} · ${selectedPort.mac}`, status: selectedPort.status },
        ]
      : [];

  return (
    <div style={{ background: "#020617", minHeight: "100vh", color: "#f1f5f9", fontFamily: "ui-monospace, 'Cascadia Code', 'Fira Code', monospace", fontSize: 13 }}>
      <div style={{ background: "#020617", borderBottom: "1px solid #0f172a", padding: "12px 20px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{ width: 8, height: 8, borderRadius: "50%", background: loading ? "#f59e0b" : "#22c55e", boxShadow: `0 0 6px ${loading ? "#f59e0b" : "#22c55e"}` }} />
          <span style={{ fontSize: 14, fontWeight: 700, letterSpacing: "0.08em", color: "#60a5fa" }}>NYCHA NOC</span>
          <span style={{ fontSize: 10, color: "#334155" }}>NETWORK DIGITAL TWIN</span>
        </div>
        <div style={{ display: "flex", gap: 20, fontSize: 11, color: "#64748b" }}>
          <span>SITE 000007</span>
          <span style={{ color: "#22c55e" }}>● {onlineBuildings} online</span>
          <span style={{ color: "#f59e0b" }}>● {degradedBuildings} degraded</span>
          <span style={{ color: "#ef4444" }}>● {offlineBuildings} offline</span>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 320px", height: "calc(100vh - 53px)" }}>
        <div style={{ overflow: "auto", padding: "16px 20px" }}>
          <div style={{ display: "flex", gap: 6, alignItems: "center", marginBottom: 14, fontSize: 11, color: "#475569" }}>
            {breadcrumb.map((crumb, index) => (
              <span key={crumb.label} style={{ display: "flex", alignItems: "center", gap: 6 }}>
                {index > 0 ? <span style={{ color: "#1e293b" }}>›</span> : null}
                <button
                  onClick={() => {
                    if (crumb.view === "map") {
                      setView("map");
                      setSelectedPortKey(null);
                      setSelectedRadioId(null);
                    }
                    if (crumb.view === "building") {
                      setView("building");
                      setSelectedPortKey(null);
                      setSelectedRadioId(null);
                    }
                  }}
                  style={{
                    background: "none",
                    border: "none",
                    color: index === breadcrumb.length - 1 ? "#94a3b8" : "#60a5fa",
                    cursor: index < breadcrumb.length - 1 ? "pointer" : "default",
                    fontSize: 11,
                    padding: 0,
                    fontFamily: "inherit",
                  }}
                >
                  {crumb.label}
                </button>
              </span>
            ))}
          </div>

          {error ? (
            <div style={{ marginBottom: 14, padding: "10px 12px", borderRadius: 8, border: "1px solid #7f1d1d", background: "#450a0a", color: "#fecaca" }}>
              Data fetch error: {error}
            </div>
          ) : null}

          {view === "map" ? (
            <MapView
              buildings={buildings}
              radios={radios}
              radioLinks={radioLinks}
              siteTopology={siteTopology}
              selectedId={selectedBuilding?.id ?? null}
              selectedRadioId={selectedRadioId}
              onSelect={(building) => {
                setSelectedBuildingId(building.id);
                setSelectedPortKey(null);
                setSelectedRadioId(null);
                setView("building");
              }}
              onSelectRadio={(radio) => {
                const anchorBuilding = buildings.find((building) => building.id === radio.anchorBuildingId) ?? null;
                if (anchorBuilding) setSelectedBuildingId(anchorBuilding.id);
                setSelectedRadioId(radio.id);
                setSelectedPortKey(null);
                setView("building");
              }}
            />
          ) : null}

          {view === "building" && selectedBuilding ? (
            <BuildingView
              building={selectedBuilding}
              allBuildings={buildings}
              ports={selectedBuildingPorts}
              loadingModel={selectedBuildingLoadingModel}
              radios={radios.filter((radio) => radio.anchorBuildingId === selectedBuilding.id || radio.address === selectedBuilding.address)}
              selectedRadio={selectedRadio && (selectedRadio.anchorBuildingId === selectedBuilding.id || selectedRadio.address === selectedBuilding.address) ? selectedRadio : null}
              onBack={() => {
                setView("map");
                setSelectedPortKey(null);
                setSelectedRadioId(null);
              }}
              onSelectPort={(port) => {
                setSelectedPortKey(portRenderKey(port));
                setView("port");
              }}
              onOpenBuilding={(buildingId) => {
                setSelectedBuildingId(buildingId);
                setSelectedPortKey(null);
                setSelectedRadioId(null);
                setView("building");
              }}
            />
          ) : null}

          {view === "port" && selectedBuilding && selectedPort ? (
            <PortView
              building={selectedBuilding}
              port={selectedPort}
              path={selectedPortPath}
              cpeContext={selectedPortCpeContext}
              onBack={() => setView("building")}
            />
          ) : null}

        </div>

        <div style={{ borderLeft: "1px solid #0f172a", padding: "16px", overflow: "auto", background: "#030712" }}>
          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 12 }}>SITE SUMMARY</div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6, marginBottom: 16 }}>
            {[
              { label: "Devices", value: siteSummary?.devices_total ?? 0, color: "#60a5fa" },
              { label: "Online CPEs", value: siteSummary?.online_customers.count ?? 0, color: "#22c55e" },
              { label: "Switches", value: siteSummary?.switches_count ?? 0, color: "#94a3b8" },
              { label: "Outliers", value: siteSummary?.outlier_count ?? 0, color: "#f59e0b" },
            ].map((item) => (
              <div key={item.label} style={{ background: "#0a0f1a", borderRadius: 8, padding: 10, border: "1px solid #0f172a" }}>
                <div style={{ fontSize: 9, color: "#475569", marginBottom: 3 }}>{item.label}</div>
                <div style={{ fontSize: 18, fontWeight: 700, color: item.color }}>{item.value}</div>
              </div>
            ))}
          </div>

          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", marginBottom: 10 }}>LIVE RATIOS</div>
          <StatBar label="API reachable" value={Math.round((((siteSummary?.scan?.api_reachable ?? 0) / 66) || 0) * 100)} color="#60a5fa" />
          <StatBar label="Customer session health" value={Math.min(100, Math.round((((siteSummary?.online_customers.count ?? 0) / 148) || 0) * 100))} color="#22c55e" />
          <StatBar label="TP-Link share" value={Math.min(100, Math.round((((siteSummary?.bridge_host_summary?.tplink ?? 0) / Math.max(siteSummary?.bridge_host_summary?.total ?? 1, 1)) * 100) || 0))} color="#f59e0b" />
          <StatBar label="Vilo share" value={Math.min(100, Math.round((((siteSummary?.bridge_host_summary?.vilo ?? 0) / Math.max(siteSummary?.bridge_host_summary?.total ?? 1, 1)) * 100) || 0))} color="#a78bfa" />

          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", margin: "16px 0 10px" }}>MAP KEY</div>
          <div style={{ background: "#0a0f1a", borderRadius: 8, padding: 10, border: "1px solid #0f172a", marginBottom: 16 }}>
            {[
              { swatch: "#22c55e", label: "Green", detail: "Healthy network state" },
              { swatch: "#f59e0b", label: "Amber", detail: "Degraded network state" },
              { swatch: "#ef4444", label: "Red", detail: "Offline or alert-driven state" },
              { swatch: "#334155", label: "Dark solid", detail: "Siklu backhaul / transport" },
              { swatch: "#38bdf8", label: "Cyan dashed", detail: "Explicit Cambium radio link" },
            ].map((item) => (
              <div key={item.label} style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                <span style={{ width: 18, height: 3, borderRadius: 999, background: item.swatch, display: "inline-block" }} />
                <div>
                  <div style={{ fontSize: 11, color: "#cbd5e1" }}>{item.label}</div>
                  <div style={{ fontSize: 9, color: "#64748b" }}>{item.detail}</div>
                </div>
              </div>
            ))}
            <div style={{ fontSize: 9, color: "#64748b", marginTop: 6 }}>
              Colors are health/status based. They are not RSSI.
            </div>
          </div>

          <button
            onClick={() => setBuildingBlocksOpen((open) => !open)}
            style={{
              width: "100%",
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: 12,
              background: "#0a0f1a",
              border: "1px solid #0f172a",
              borderRadius: 8,
              padding: "10px 12px",
              cursor: "pointer",
              margin: "16px 0 10px",
              color: "#cbd5e1",
              fontFamily: "inherit",
            }}
          >
            <span style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569" }}>BUILDING BLOCKS</span>
            <span style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <span style={{ fontSize: 10, color: "#64748b" }}>{sidebarBuildings.length}</span>
              <span style={{ fontSize: 12, color: "#94a3b8" }}>{buildingBlocksOpen ? "▾" : "▸"}</span>
            </span>
          </button>
          {buildingBlocksOpen ? sidebarBuildings.map((building) => (
            <div
              key={canonicalBuildingIdOf(building)}
              onClick={() => {
                setSelectedBuildingId(building.id);
                setSelectedPortKey(null);
                setSelectedRadioId(null);
                setView("building");
              }}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 8,
                padding: "8px 10px",
                borderRadius: 6,
                marginBottom: 4,
                cursor: "pointer",
                background: selectedBuilding?.id === building.id ? "#0f172a" : "transparent",
                border: `1px solid ${selectedBuilding?.id === building.id ? "#1e3a5f" : "transparent"}`,
              }}
            >
              <div style={{ width: 6, height: 6, borderRadius: "50%", background: STATUS_COLOR[building.status], flexShrink: 0 }} />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 11, color: "#cbd5e1", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{building.shortLabel}</div>
                <div style={{ fontSize: 9, color: "#475569" }}>{building.customerCount} live ports · {building.deviceCount} devices</div>
              </div>
              <span style={{ fontSize: 9, color: STATUS_COLOR[building.status] }}>{building.status}</span>
            </div>
          )) : null}

          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", margin: "16px 0 10px" }}>CAMBIUM RADIOS</div>
          {radios.map((radio) => (
            <div
              key={radio.id}
              onClick={() => {
                const anchorBuilding = buildings.find((building) => building.id === radio.anchorBuildingId) ?? null;
                if (anchorBuilding) setSelectedBuildingId(anchorBuilding.id);
                setSelectedRadioId(radio.id);
                setSelectedPortKey(null);
                setView("building");
              }}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 8,
                padding: "8px 10px",
                borderRadius: 6,
                marginBottom: 4,
                cursor: "pointer",
                background: selectedRadio?.id === radio.id ? "#0f172a" : "transparent",
                border: `1px solid ${selectedRadio?.id === radio.id ? "#1e3a5f" : "transparent"}`,
              }}
            >
              <div style={{ width: 6, height: 6, borderRadius: "50%", background: STATUS_COLOR[radio.status], flexShrink: 0 }} />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 11, color: "#cbd5e1", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{radio.shortLabel}</div>
                <div style={{ fontSize: 9, color: "#475569" }}>{radio.role} · {radio.model}</div>
              </div>
              <span style={{ fontSize: 9, color: STATUS_COLOR[radio.status] }}>{radio.status}</span>
            </div>
          ))}

          <div style={{ fontSize: 10, letterSpacing: "0.1em", color: "#475569", margin: "16px 0 10px" }}>ACTIVE ALERTS</div>
          {siteAlerts.length ? (
            siteAlerts.map((alert) => (
              <div
                key={`${alert.annotations?.summary ?? "alert"}-${alert.labels?.name ?? ""}`}
                style={{ fontSize: 10, color: "#fecaca", padding: "7px 8px", borderRadius: 4, borderLeft: "2px solid #ef4444", background: "#450a0a", marginBottom: 6 }}
              >
                {alert.annotations?.summary ?? alert.labels?.name ?? "Unnamed alert"}
              </div>
            ))
          ) : (
            <div style={{ fontSize: 10, color: "#475569" }}>No active alerts returned.</div>
          )}

          <div style={{ fontSize: 9, color: "#1e293b", marginTop: 20, textAlign: "center" }}>
            Site {siteSummary?.site_id ?? "000007"} · last scan {siteSummary?.scan?.id ?? "n/a"} · polls every 30s
          </div>
        </div>
      </div>
    </div>
  );
}
