from __future__ import annotations
import re

import threading
from typing import Any

import networkx as nx


def _sw_to_building(identity):
    parts = identity.split(".")
    return ".".join(parts[:2]) if len(parts) >= 3 else None


def _sw_to_site(identity):
    parts = identity.split(".")
    return parts[0] if parts else None


def _node_type(identity):
    parts = identity.split(".")
    if len(parts) == 1:
        return "site"
    if len(parts) == 2:
        return "building"
    seg = parts[2].upper()
    if re.match(r"^R\d{2}$", seg):
        return "router"
    if "SW" in seg:
        return "switch"
    if seg.startswith("AG"):
        return "switch"
    return "device"


class TopologyGraph:
    def __init__(self):
        self._g = nx.DiGraph()
        self._lock = threading.Lock()
        self._built = False

    def sync_from_ops(self, ops):
        g = nx.DiGraph()
        site_id = "000007"
        g.add_node(site_id, type="site", identity=site_id)
        buildings_seen = set()
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
            g.add_node(identity, type=ntype, identity=identity, site_id=site, building_id=building_id)
            if building_id and building_id not in buildings_seen:
                buildings_seen.add(building_id)
                g.add_node(building_id, type="building", identity=building_id, site_id=site)
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
                if not src or not dst:
                    continue
                if dst not in g:
                    dt = _node_type(dst)
                    g.add_node(
                        dst,
                        type=dt,
                        identity=dst,
                        site_id=_sw_to_site(dst) if "." in dst else None,
                        building_id=_sw_to_building(dst) if "." in dst else None,
                        platform=edge.get("platform", ""),
                    )
                    if edge.get("neighbor_address"):
                        g.nodes[dst]["ip"] = edge["neighbor_address"]
                dt = g.nodes.get(dst, {}).get("type", "")
                etype = "UPLINKS_TO" if dt == "router" or dt not in ("switch", "router") else "CONNECTS_TO"
                g.add_edge(
                    src,
                    dst,
                    type=etype,
                    from_interface=edge.get("from_interface", ""),
                    platform=edge.get("platform", ""),
                )
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
    def g(self):
        return self._g

    def node(self, identity):
        return dict(self._g.nodes[identity]) if identity in self._g else None

    def neighbors_of(self, identity):
        return [
            {"identity": n, **self._g.nodes[n], "edge": dict(self._g.edges[identity, n])}
            for n in self._g.successors(identity)
            if (identity, n) in self._g.edges
        ]

    def uplinks_of(self, identity):
        return [n for n in self.neighbors_of(identity) if n.get("edge", {}).get("type") == "UPLINKS_TO"]

    def blast_radius(self, identity):
        g = self._g
        if identity not in g:
            return {"identity": identity, "error": "node not found"}
        served = {n for n in g.nodes if n != identity and nx.has_path(g, n, identity)}
        return {
            "identity": identity,
            "type": g.nodes.get(identity, {}).get("type"),
            "affected_switches": [n for n in served if g.nodes[n].get("type") == "switch"],
            "affected_buildings": list(
                {g.nodes[n].get("building_id") for n in served if g.nodes[n].get("building_id")}
            ),
            "affected_routers": [n for n in served if g.nodes[n].get("type") == "router"],
            "total_downstream": len(served),
        }

    def redundancy_check(self, identity):
        uplinks = self.uplinks_of(identity)
        return {
            "identity": identity,
            "uplink_count": len(uplinks),
            "is_single_point_of_failure": len(uplinks) <= 1,
            "uplinks": [u["identity"] for u in uplinks],
        }

    def path_between(self, src, dst):
        try:
            path = nx.shortest_path(self._g, src, dst)
            return {"src": src, "dst": dst, "path": path, "hops": len(path) - 1}
        except (nx.NetworkXNoPath, nx.NodeNotFound) as e:
            return {"src": src, "dst": dst, "error": str(e)}

    def summary(self):
        g = self._g
        type_counts = {}
        for _, data in g.nodes(data=True):
            t = data.get("type", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1
        spofs = [
            n
            for n in g.nodes
            if g.nodes[n].get("type") in ("switch", "router") and len(self.uplinks_of(n)) <= 1
        ]
        return {
            "built": self._built,
            "total_nodes": g.number_of_nodes(),
            "total_edges": g.number_of_edges(),
            "by_type": type_counts,
            "single_points_of_failure": len(spofs),
            "spof_identities": spofs[:20],
        }


_graph = None
_graph_lock = threading.Lock()


def get_graph():
    global _graph
    if _graph is None:
        with _graph_lock:
            if _graph is None:
                _graph = TopologyGraph()
    return _graph


def rebuild_graph(ops):
    return get_graph().sync_from_ops(ops)
