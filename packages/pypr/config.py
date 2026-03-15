from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

import yaml


DEFAULT_POLICY_PATH = "packages/pypr/config/constitution.yaml"


@lru_cache(maxsize=1)
def load_policy() -> dict[str, Any]:
    path = os.getenv("PYPR_POLICY_PATH", DEFAULT_POLICY_PATH)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_threshold(path: list[str], fallback: float) -> float:
    node: Any = load_policy()
    for p in path:
        if not isinstance(node, dict) or p not in node:
            return fallback
        node = node[p]
    return float(node)
