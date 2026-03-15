"""Run from pypr-core root: python3 tools/write_topology.py"""
from pathlib import Path

# ── write directly ─────────────────────────────────────────────────────────────
src = r'''from __future__ import annotations

import threading
from typing import Any

import networkx as nx


def _sw_to_building(identity: str) -> str | None:
    parts = identity.split(".")
    return ".".join(parts[:2]) if len(parts) >= 3 else None


def _sw_to_site(identity: str) -> str | None:
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


class TopologyGraph:
    def __init__(self) -> None:
        self._g: nx.DiGraph = nx.DiGraph()
        self._lock = threading.Lock()
        self._built = False

    def sync_from_ops(self, ops: Any) -> dict[str, int]:
        g = nx.DiGraph()
        site_id = "000007"
        g.add_node(site_id, type="site", identity=site_id)

        buildings_seen: set[str] = set()
        edges_added = 0

        try:
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

            g.add_node(identity, type=ntype, identity=identity,
                       site_id=site, building_id=building_id)

            if building_id and building_id not in buildings_seen:
                buildings_seen.add(building_id)
                g.add_node(building_id, type="building",
                           identity=building_id, site_id=site)
                g.add_edge(building_id, site_id, type="MEMBER_OF")

            if building_id:
                g.add_edge(identity, building_id, type="MEMBER_OF")

        for building_id in buildings_seen:
            try:
                model = ops.get_building_model(building_id)
            except Exception:
                continue

            if model.get("address"):
                g.nodes[building_id]["address"] = model["address"]

            for edge in model.get("direct_neighbor_edges") or []:
                src = edge.get("from_identity")
                dst = edge.get("to_identity")
                src_iface = edge.get("from_interface", "")
                platform = edge.get("platform", "")
                neighbor_addr = edge.get("neighbor_address")

                if not src or not dst:
                    continue

                if dst not in g:
                    dst_type = _node_type(dst)
                    dst_building = _sw_to_building(dst) if "." in dst else None
                    dst_site = _sw_to_site(dst) if "." in dst else None
                    g.add_node(dst, type=dst_type, identity=dst,
                               site_id=dst_site, building_id=dst_building,
                               platform=platform)
                    if neighbor_addr:
                        g.nodes[dst]["ip"] = neighbor_addr

                dst_type_val = g.nodes.get(dst, {}).get("type", "")
                edge_type = (
                    "UPLINKS_TO"
                    if dst_type_val == "router" or dst_type_val not in ("switch", "router")
                    else "CONNECTS_TO"
                )
                g.add_edge(src, dst, type=edge_type,
                           from_interface=src_iface, platform=platform)
                edges_added += 1

        with self._lock:
            self._g = g
            self._built = True

        return {
            "nodes": g.number_of_nodes(),
            "edges": g.number_of_edges(),
            "buildings": len(buildings_seen),
            "edges_from_lldp": edges_added,
        }

    @property
    def g(self) -> nx.DiGraph:
        return self._g

    def node(self, identity: str) -> dict[str, Any] | None:
        return dict(self._g.nodes[identity]) if identity in self._g else None

    def neighbors_of(self, identity: str) -> list[dict]:
        return [
            {"identity": n, **self._g.nodes[n],
             "edge": dict(self._g.edges[identity, n])}
            for n in self._g.successors(identity)
            if (identity, n) in self._g.edges
        ]

    def uplinks_of(self, identity: str) -> list[dict]:
        return [
            n for n in self.neighbors_of(identity)
            if n.get("edge", {}).get("type") == "UPLINKS_TO"
        ]

    def blast_radius(self, identity: str) -> dict[str, Any]:
        g = self._g
        if identity not in g:
            return {"identity": identity, "error": "node not found"}

        served = set()
        for node in g.nodes:
            if node == identity:
                continue
            try:
                if nx.has_path(g, node, identity):
                    served.add(node)
            except Exception:
                pass

        switches = [n for n in served if g.nodes[n].get("type") == "switch"]
        buildings = list({
            g.nodes[n].get("building_id")
            for n in served if g.nodes[n].get("building_id")
        })
        routers = [n for n in served if g.nodes[n].get("type") == "router"]

        return {
            "identity": identity,
            "type": g.nodes.get(identity, {}).get("type"),
            "affected_switches": switches,
            "affected_buildings": buildings,
            "affected_routers": routers,
            "total_downstream": len(served),
        }

    def redundancy_check(self, identity: str) -> dict[str, Any]:
        uplinks = self.uplinks_of(identity)
        return {
            "identity": identity,
            "uplink_count": len(uplinks),
            "is_single_point_of_failure": len(uplinks) <= 1,
            "uplinks": [u["identity"] for u in uplinks],
        }

    def path_between(self, src: str, dst: str) -> dict[str, Any]:
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
            and len(self.uplinks_of(n)) <= 1
        ]
        return {
            "built": self._built,
            "total_nodes": g.number_of_nodes(),
            "total_edges": g.number_of_edges(),
            "by_type": type_counts,
            "single_points_of_failure": len(spofs),
            "spof_identities": spofs[:20],
        }


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
    return get_graph().sync_from_ops(ops)
'''

Path("packages/jake/graph").mkdir(parents=True, exist_ok=True)
Path("packages/jake/graph/__init__.py").write_text("")

topology_src_path = Path(__file__).with_name("_topology_src.py")
topology_src = topology_src_path.read_text() if topology_src_path.exists() else src

Path("packages/jake/graph/topology.py").write_text(topology_src)
print("wrote packages/jake/graph/topology.py")
