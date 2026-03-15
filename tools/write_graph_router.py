"""Run from pypr-core root: python3 tools/write_graph_router.py"""
from pathlib import Path

src = '''from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from apps.api.jake_router import _ops
from packages.jake.graph.topology import get_graph, rebuild_graph

router = APIRouter(prefix="/v1/graph", tags=["graph"])


@router.post("/sync", summary="Rebuild topology graph from live network data")
def sync_graph() -> dict:
    try:
        result = rebuild_graph(_ops())
        return {"status": "ok", **result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/summary", summary="Node counts, edge counts, SPOFs")
def graph_summary() -> dict:
    g = get_graph()
    if not g._built:
        return {"built": False, "message": "POST /v1/graph/sync first"}
    return g.summary()


@router.get("/nodes/{identity}", summary="Node + immediate neighbors + redundancy")
def get_node(identity: str) -> dict:
    g = get_graph()
    node = g.node(identity)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Node {identity!r} not in graph")
    return {
        "node": node,
        "neighbors": g.neighbors_of(identity),
        "uplinks": g.uplinks_of(identity),
        "redundancy": g.redundancy_check(identity),
    }


@router.get("/blast-radius/{identity}", summary="What breaks if this node goes offline?")
def blast_radius(identity: str) -> dict:
    g = get_graph()
    if not g._built:
        raise HTTPException(status_code=503, detail="POST /v1/graph/sync first")
    return g.blast_radius(identity)


@router.get("/path", summary="Shortest path between two nodes")
def path(src: str = Query(...), dst: str = Query(...)) -> dict:
    g = get_graph()
    if not g._built:
        raise HTTPException(status_code=503, detail="POST /v1/graph/sync first")
    return g.path_between(src, dst)


@router.get("/redundancy/{identity}", summary="Is this node a single point of failure?")
def redundancy(identity: str) -> dict:
    g = get_graph()
    if not g._built:
        raise HTTPException(status_code=503, detail="POST /v1/graph/sync first")
    return g.redundancy_check(identity)


@router.get("/single-points-of-failure", summary="All SPOFs in the network")
def spofs() -> dict:
    g = get_graph()
    if not g._built:
        raise HTTPException(status_code=503, detail="POST /v1/graph/sync first")
    result = [
        {**g.redundancy_check(n), "type": (g.node(n) or {}).get("type")}
        for n in g.g.nodes
        if (g.node(n) or {}).get("type") in ("switch", "router")
        and len(g.uplinks_of(n)) <= 1
    ]
    return {"count": len(result), "spofs": result}
'''

Path("apps/api/graph_router.py").write_text(src)
print("wrote apps/api/graph_router.py")
