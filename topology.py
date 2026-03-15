"""
Jake topology graph — built from live LLDP edges in network_map.db.

Node types:  site | building | router | switch | port | cpe
Edge types:  UPLINKS_TO | CONNECTS_TO | MEMBER_OF | SERVES

Stored as a NetworkX DiGraph, serialised to Postgres (or a JSON sidecar
when Postgres is not yet configured).  The graph is rebuilt on demand by
calling sync_from_ops(ops) and cached in memory for the process lifetime.
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import networkx as nx

# ---------------------------------------------------------------------------
# Node / edge helpers
# ---------------------------------------------------------------------------

def _sw_to_building(identity: str) -> str | None:
    """'000007.055.SW04' → '000007.055'"""
    parts = identity.split(".")
    return ".".join(parts[:2]) if len(parts) >= 3 else None


def _sw_to_site(identity: str) -> str | None:
    """'000007.055.SW04' → '000007'"""
    parts = identity.split(".")
    return parts[0] if parts else None


def _node_type(identity: str) -> str:
    parts = identity.split(".")
    if len(parts) == 1:
        return "site"
    if len(parts) == 2:
        return "building"
    seg = parts[2].upper()
    if seg.startswith("R"):
        return "router"
    if seg.startswith("SW"):
        return "switch"
    return "device"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------

class TopologyGraph:
    """
    Thin wrapper around a NetworkX DiGraph.

    Nodes carry:  type, identity, site_id, building_id, address, ip, platform
    Edges carry:  type (UPLINKS_TO | CONNECTS_TO | MEMBER_OF | SERVES),
                  from_interface, to_interface, media
    """

    def __init__(self) -> None:
        self._g: nx.DiGraph = nx.DiGraph()
        self._lock = threading.Lock()
        self._built = False

    # ------------------------------------------------------------------
    # Build / sync
    # ------------------------------------------------------------------

    def sync_from_ops(self, ops: Any) -> dict[str, int]:
        """
        Pull all available topology data from a live JakeOps instance and
        rebuild the in-memory graph.  Returns a summary dict.
        """
        g = nx.DiGraph()

        # 1. seed the site node
        site_id = "000007"  # TODO: make multi-site when needed
        g.add_node(site_id, type="site", identity=site_id)

        buildings_seen: set[str] = set()
        switches_seen: set[str] = set()
        edges_added = 0

        # 2. collect all switch identities from the scan
        try:
            server_info = ops.get_server_info()
            scan_meta = server_info.get("latest_scan") or {}
        except Exception:
            scan_meta = {}

        # 3. walk buildings via get_building_model for every known building
        #    We derive building list from switch identities in the scan DB
        try:
            from packages.jake.connectors.mcp.jake_ops_mcp import JakeOps  # noqa
            scan_id = ops.latest_scan_id()
            rows = ops.db.execute(
                "SELECT DISTINCT identity FROM devices WHERE scan_id=? AND identity IS NOT NULL",
                (scan_id,),
            ).fetchall()
            all_identities = [r[0] for r in rows if r[0]]
        except Exception:
            all_identities = []

        for identity in all_identities:
            ntype = _node_type(identity)
            building_id = _sw_to_building(identity)
            site = _sw_to_site(identity)

            # add device node
            g.add_node(
                identity,
                type=ntype,
                identity=identity,
                site_id=site,
                building_id=building_id,
            )

            # add building node + edge
            if building_id and building_id not in buildings_seen:
                buildings_seen.add(building_id)
                g.add_node(building_id, type="building", identity=building_id, site_id=site)
                g.add_edge(building_id, site_id, type="MEMBER_OF")

            # device → building
            if building_id:
                g.add_edge(identity, building_id, type="MEMBER_OF")

        # 4. enrich with LLDP edges from get_building_model
        for building_id in buildings_seen:
            try:
                model = ops.get_building_model(building_id)
            except Exception:
                continue

            # add address
            if model.get("address"):
                g.nodes[building_id]["address"] = model["address"]

            # LLDP direct_neighbor_edges → topology edges
            for edge in model.get("direct_neighbor_edges") or []:
                src = edge.get("from_identity")
                dst = edge.get("to_identity")
                src_iface = edge.get("from_interface", "")
                platform = edge.get("platform", "")
                neighbor_addr = edge.get("neighbor_address")

                if not src or not dst:
                    continue

                # add destination node if not yet seen
                if dst not in g:
                    dst_type = _node_type(dst)
                    dst_building = _sw_to_building(dst) if "." in dst else None
                    dst_site = _sw_to_site(dst) if "." in dst else None
                    g.add_node(
                        dst,
                        type=dst_type,
                        identity=dst,
                        site_id=dst_site,
                        building_id=dst_building,
                        platform=platform,
                    )
                    if neighbor_addr:
                        g.nodes[dst]["ip"] = neighbor_addr

                # classify edge type
                src_type = g.nodes.get(src, {}).get("type", "")
                dst_type = g.nodes.get(dst, {}).get("type", "")

                if dst_type == "router" or (
                    dst_type not in ("switch", "router")
                    and src_type == "switch"
                ):
                    edge_type = "UPLINKS_TO"
                else:
                    edge_type = "CONNECTS_TO"

                g.add_edge(
                    src, dst,
                    type=edge_type,
                    from_interface=src_iface,
                    platform=platform,
                )
                edges_added += 1

        with self._lock:
            self._g = g
            self._built = True

        return {
            "nodes": g.number_of_nodes(),
            "edges": g.number_of_edges(),
            "buildings": len(buildings_seen),
            "switches": len(switches_seen),
            "edges_from_lldp": edges_added,
        }

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    @property
    def g(self) -> nx.DiGraph:
        return self._g

    def node(self, identity: str) -> dict[str, Any] | None:
        return dict(self._g.nodes[identity]) if identity in self._g else None

    def neighbors_of(self, identity: str) -> list[dict]:
        return [
            {"identity": n, **self._g.nodes[n], "edge": dict(self._g.edges[identity, n])}
            for n in self._g.successors(identity)
            if (identity, n) in self._g.edges
        ]

    def uplinks_of(self, identity: str) -> list[dict]:
        return [
            n for n in self.neighbors_of(identity)
            if n.get("edge", {}).get("type") == "UPLINKS_TO"
        ]

    def blast_radius(self, identity: str) -> dict[str, Any]:
        """
        What stops working if this node goes offline?
        Returns all nodes downstream (reachable FROM this node in reverse graph).
        """
        g = self._g
        if identity not in g:
            return {"identity": identity, "error": "node not found"}

        # nodes that depend on `identity` = ancestors in the directed graph
        # (things that route THROUGH this node)
        # We walk the reverse: what does `identity` serve?
        served = set()
        for node in g.nodes:
            if node == identity:
                continue
            # if `identity` is on any path from node to the "outside"
            # (a router or the site root), then node is affected
            try:
                paths = list(nx.all_simple_paths(g, node, identity, cutoff=6))
                if paths:
                    served.add(node)
            except nx.NetworkXNoPath:
                pass

        switches = [n for n in served if g.nodes[n].get("type") == "switch"]
        buildings = list({g.nodes[n].get("building_id") for n in served if g.nodes[n].get("building_id")})
        routers = [n for n in served if g.nodes[n].get("type") == "router"]

        return {
            "identity": identity,
            "type": g.nodes.get(identity, {}).get("type"),
            "affected_nodes": len(served),
            "affected_switches": switches,
            "affected_buildings": buildings,
            "affected_routers": routers,
            "total_downstream": len(served),
        }

    def redundancy_check(self, identity: str) -> dict[str, Any]:
        """
        How many independent uplink paths does this node have?
        A node with only one uplink is a single point of failure.
        """
        uplinks = self.uplinks_of(identity)
        return {
            "identity": identity,
            "uplink_count": len(uplinks),
            "is_single_point_of_failure": len(uplinks) <= 1,
            "uplinks": [u["identity"] for u in uplinks],
        }

    def path_between(self, src: str, dst: str) -> dict[str, Any]:
        """Shortest path between two nodes."""
        try:
            path = nx.shortest_path(self._g, src, dst)
            return {"src": src, "dst": dst, "path": path, "hops": len(path) - 1}
        except (nx.NetworkXNoPath, nx.NodeNotFound) as e:
            return {"src": src, "dst": dst, "error": str(e)}

    def summary(self) -> dict[str, Any]:
        g = self._g
        type_counts: dict[str, int] = {}
        for _, data in g.nodes(data=True):
            t = data.get("type", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1

        spofs = [
            n for n in g.nodes
            if g.nodes[n].get("type") in ("switch", "router")
            and self.redundancy_check(n)["is_single_point_of_failure"]
        ]

        return {
            "built": self._built,
            "total_nodes": g.number_of_nodes(),
            "total_edges": g.number_of_edges(),
            "by_type": type_counts,
            "single_points_of_failure": len(spofs),
            "spof_identities": spofs[:20],
        }

    def to_json(self) -> dict:
        """Serialise graph for storage or transport."""
        return nx.node_link_data(self._g)

    @classmethod
    def from_json(cls, data: dict) -> "TopologyGraph":
        inst = cls()
        inst._g = nx.node_link_graph(data)
        inst._built = True
        return inst


# ---------------------------------------------------------------------------
# Process-level singleton
# ---------------------------------------------------------------------------

_graph: TopologyGraph | None = None
_graph_lock = threading.Lock()


def get_graph() -> TopologyGraph:
    global _graph
    if _graph is None:
        with _graph_lock:
            if _graph is None:
                _graph = TopologyGraph()
    return _graph


def rebuild_graph(ops: Any) -> dict[str, int]:
    """Rebuild the singleton graph from a live JakeOps instance."""
    g = get_graph()
    return g.sync_from_ops(ops)
