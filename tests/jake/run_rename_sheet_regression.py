#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / 'output' / 'spreadsheet' / 'netbox_targeted_rename_proposals.csv'
GENERATOR = ROOT / 'scripts' / 'generate_targeted_rename_sheet.py'


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    subprocess.run([str(ROOT / '.venv' / 'bin' / 'python'), str(GENERATOR)], check=True, cwd=ROOT, env={**dict(**__import__('os').environ), 'PYTHONPATH': str(ROOT)})
    rows = list(csv.DictReader(CSV_PATH.open()))
    by_name = {r['current_name']: r for r in rows}
    counts = Counter(r['confidence'] for r in rows)

    checks = []
    def record(name: str, fn) -> None:
        fn()
        checks.append({'name': name, 'status': 'pass'})

    record('row_count', lambda: assert_true(len(rows) == 59, f'expected 59 rows, got {len(rows)}'))
    record('proposed_count', lambda: assert_true(sum(1 for r in rows if r['proposed_name']) == 49, 'expected 49 proposed names'))
    record('confidence_high', lambda: assert_true(counts['high'] == 34, f"expected 34 high-confidence rows, got {counts['high']}"))
    record('confidence_medium', lambda: assert_true(counts['medium'] == 7, f"expected 7 medium-confidence rows, got {counts['medium']}"))
    record('confidence_low', lambda: assert_true(counts['low'] == 8, f"expected 8 low-confidence rows, got {counts['low']}"))
    record('confidence_none', lambda: assert_true(counts['none'] == 10, f"expected 10 unresolved rows, got {counts['none']}"))
    record('savoy_b1', lambda: assert_true(by_name['Savoy Building 1 v5000']['proposed_name'] == '000002.004.V5K01', 'Savoy Building 1 mapping drifted'))
    record('savoy_b7', lambda: assert_true(by_name['Savoy - Building 7 v5000']['proposed_name'] == '000002.010.V5K01', 'Savoy Building 7 mapping drifted'))
    record('cambridge_clubhouse', lambda: assert_true(by_name['Cambridge Square Clubhouse V2000']['proposed_name'] == '000004.008.V2K01', 'Cambridge clubhouse mapping drifted'))
    record('tapscott', lambda: assert_true(by_name['104 Tapscott V5000']['proposed_name'] == '000007.001.V5K01', '104 Tapscott mapping drifted'))
    record('buffalo_unresolved', lambda: assert_true(by_name['225 Buffalo V5000']['proposed_name'] == '', '225 Buffalo should remain unresolved until a safe prefix exists'))

    print(json.dumps({'status': 'pass', 'checks': checks}, indent=2))


if __name__ == '__main__':
    main()
