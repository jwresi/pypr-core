#!/usr/bin/env python3
import argparse
import ipaddress
import json
import os
import socket
import sqlite3
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from dotenv import dotenv_values
from librouteros import connect


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def b2s(v):
    if isinstance(v, bytes):
        return v.decode(errors="ignore")
    return v


def to_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def to_bool(v):
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"true", "yes", "1", "on"}


def mac_norm(v):
    if not v:
        return None
    s = str(v).strip().lower().replace("-", ":")
    parts = s.split(":")
    if len(parts) == 6 and all(len(p) == 2 for p in parts):
        return s
    return None


def load_creds(env_path):
    cfg = dotenv_values(env_path)
    user = cfg.get("username")
    pw = cfg.get("password")
    if not user or not pw:
        raise RuntimeError(f"Missing username/password in {env_path}")
    return user, pw


def db_connect(path):
    con = sqlite3.connect(path)
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def init_db(con):
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS scans (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          subnet TEXT NOT NULL,
          hosts_tested INTEGER NOT NULL DEFAULT 0,
          api_reachable INTEGER NOT NULL DEFAULT 0,
          notes TEXT
        );

        CREATE TABLE IF NOT EXISTS devices (
          scan_id INTEGER NOT NULL,
          ip TEXT NOT NULL,
          identity TEXT,
          board_name TEXT,
          model TEXT,
          version TEXT,
          architecture TEXT,
          uptime TEXT,
          is_crs INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (scan_id, ip),
          FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS interfaces (
          scan_id INTEGER NOT NULL,
          ip TEXT NOT NULL,
          name TEXT NOT NULL,
          type TEXT,
          running INTEGER,
          disabled INTEGER,
          slave INTEGER,
          mtu INTEGER,
          actual_mtu INTEGER,
          rx_byte INTEGER,
          tx_byte INTEGER,
          rx_packet INTEGER,
          tx_packet INTEGER,
          last_link_up_time TEXT,
          PRIMARY KEY (scan_id, ip, name),
          FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS neighbors (
          scan_id INTEGER NOT NULL,
          ip TEXT NOT NULL,
          interface TEXT,
          neighbor_address TEXT,
          neighbor_identity TEXT,
          platform TEXT,
          version TEXT,
          FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS bridge_ports (
          scan_id INTEGER NOT NULL,
          ip TEXT NOT NULL,
          interface TEXT,
          pvid INTEGER,
          ingress_filtering INTEGER,
          frame_types TEXT,
          trusted INTEGER,
          hw INTEGER,
          FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS bridge_vlans (
          scan_id INTEGER NOT NULL,
          ip TEXT NOT NULL,
          vlan_ids TEXT,
          tagged TEXT,
          untagged TEXT,
          FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS bridge_hosts (
          scan_id INTEGER NOT NULL,
          ip TEXT NOT NULL,
          mac TEXT,
          on_interface TEXT,
          vid INTEGER,
          local INTEGER,
          external INTEGER,
          FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS router_ppp_active (
          scan_id INTEGER NOT NULL,
          router_ip TEXT NOT NULL,
          name TEXT,
          service TEXT,
          caller_id TEXT,
          address TEXT,
          uptime TEXT,
          FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS router_arp (
          scan_id INTEGER NOT NULL,
          router_ip TEXT NOT NULL,
          address TEXT,
          mac TEXT,
          interface TEXT,
          dynamic INTEGER,
          FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS one_way_outliers (
          scan_id INTEGER NOT NULL,
          ip TEXT NOT NULL,
          interface TEXT NOT NULL,
          rx_delta INTEGER NOT NULL,
          tx_delta INTEGER NOT NULL,
          direction TEXT NOT NULL,
          severity TEXT NOT NULL,
          note TEXT,
          FOREIGN KEY (scan_id) REFERENCES scans(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_devices_identity ON devices(identity);
        CREATE INDEX IF NOT EXISTS idx_neighbors_scan_ip ON neighbors(scan_id, ip);
        CREATE INDEX IF NOT EXISTS idx_bridge_hosts_scan_ip_mac ON bridge_hosts(scan_id, ip, mac);
        CREATE INDEX IF NOT EXISTS idx_interfaces_scan_ip_name ON interfaces(scan_id, ip, name);
        CREATE INDEX IF NOT EXISTS idx_outliers_scan ON one_way_outliers(scan_id);
        """
    )
    con.commit()


def tcp_open(ip, port=8728, timeout=0.7):
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False


def gather_device(ip, user, pw, timeout=3):
    api = connect(host=ip, username=user, password=pw, port=8728, timeout=timeout)

    ident = None
    for r in api.path("system", "identity").select("name"):
        ident = b2s(r.get("name"))
        break

    resource = {}
    for r in api.path("system", "resource").select(
        "board-name", "version", "architecture-name", "uptime", "platform"
    ):
        resource = {k: b2s(v) for k, v in r.items()}
        break

    board_name = resource.get("board-name")
    is_crs = bool(board_name and str(board_name).upper().startswith("CRS"))

    interfaces = []
    for r in api.path("interface").select(
        "name",
        "type",
        "running",
        "disabled",
        "slave",
        "mtu",
        "actual-mtu",
        "rx-byte",
        "tx-byte",
        "rx-packet",
        "tx-packet",
        "last-link-up-time",
    ):
        interfaces.append(
            {
                "name": b2s(r.get("name")),
                "type": b2s(r.get("type")),
                "running": to_bool(r.get("running")),
                "disabled": to_bool(r.get("disabled")),
                "slave": to_bool(r.get("slave")),
                "mtu": to_int(r.get("mtu"), None),
                "actual_mtu": to_int(r.get("actual-mtu"), None),
                "rx_byte": to_int(r.get("rx-byte"), 0),
                "tx_byte": to_int(r.get("tx-byte"), 0),
                "rx_packet": to_int(r.get("rx-packet"), 0),
                "tx_packet": to_int(r.get("tx-packet"), 0),
                "last_link_up_time": b2s(r.get("last-link-up-time")),
            }
        )

    neighbors = []
    try:
        for r in api.path("ip", "neighbor").select(
            "address", "identity", "interface", "platform", "version"
        ):
            neighbors.append(
                {
                    "address": b2s(r.get("address")),
                    "identity": b2s(r.get("identity")),
                    "interface": b2s(r.get("interface")),
                    "platform": b2s(r.get("platform")),
                    "version": b2s(r.get("version")),
                }
            )
    except Exception:
        pass

    bridge_ports = []
    bridge_vlans = []
    bridge_hosts = []

    try:
        for r in api.path("interface", "bridge", "port").select(
            "interface", "pvid", "ingress-filtering", "frame-types", "trusted", "hw"
        ):
            bridge_ports.append(
                {
                    "interface": b2s(r.get("interface")),
                    "pvid": to_int(r.get("pvid"), None),
                    "ingress_filtering": to_bool(r.get("ingress-filtering")),
                    "frame_types": b2s(r.get("frame-types")),
                    "trusted": to_bool(r.get("trusted")),
                    "hw": to_bool(r.get("hw")),
                }
            )
    except Exception:
        pass

    try:
        for r in api.path("interface", "bridge", "vlan").select("vlan-ids", "tagged", "untagged"):
            bridge_vlans.append(
                {
                    "vlan_ids": b2s(r.get("vlan-ids")),
                    "tagged": b2s(r.get("tagged")),
                    "untagged": b2s(r.get("untagged")),
                }
            )
    except Exception:
        pass

    try:
        for r in api.path("interface", "bridge", "host").select(
            "mac-address", "on-interface", "vid", "local", "external"
        ):
            bridge_hosts.append(
                {
                    "mac": mac_norm(r.get("mac-address")),
                    "on_interface": b2s(r.get("on-interface")),
                    "vid": to_int(r.get("vid"), None),
                    "local": to_bool(r.get("local")),
                    "external": to_bool(r.get("external")),
                }
            )
    except Exception:
        pass

    ppp_active = []
    arp = []
    try:
        for r in api.path("ppp", "active").select("name", "service", "caller-id", "address", "uptime"):
            ppp_active.append(
                {
                    "name": b2s(r.get("name")),
                    "service": b2s(r.get("service")),
                    "caller_id": mac_norm(r.get("caller-id")),
                    "address": b2s(r.get("address")),
                    "uptime": b2s(r.get("uptime")),
                }
            )
    except Exception:
        pass

    try:
        for r in api.path("ip", "arp").select("address", "mac-address", "interface", "dynamic"):
            arp.append(
                {
                    "address": b2s(r.get("address")),
                    "mac": mac_norm(r.get("mac-address")),
                    "interface": b2s(r.get("interface")),
                    "dynamic": to_bool(r.get("dynamic")),
                }
            )
    except Exception:
        pass

    return {
        "ip": ip,
        "identity": ident,
        "board_name": board_name,
        "model": resource.get("platform"),
        "version": resource.get("version"),
        "architecture": resource.get("architecture-name"),
        "uptime": resource.get("uptime"),
        "is_crs": is_crs,
        "interfaces": interfaces,
        "neighbors": neighbors,
        "bridge_ports": bridge_ports,
        "bridge_vlans": bridge_vlans,
        "bridge_hosts": bridge_hosts,
        "ppp_active": ppp_active,
        "arp": arp,
    }


def latest_scan_id(con):
    r = con.execute("SELECT id FROM scans ORDER BY id DESC LIMIT 1").fetchone()
    return r[0] if r else None


def previous_interface_counters(con, scan_id):
    if not scan_id:
        return {}
    rows = con.execute(
        "SELECT ip,name,rx_byte,tx_byte FROM interfaces WHERE scan_id=?", (scan_id,)
    ).fetchall()
    return {(ip, name): (rx or 0, tx or 0) for ip, name, rx, tx in rows}


def classify_one_way(rx_delta, tx_delta):
    low = 64 * 1024
    mid = 256 * 1024
    if rx_delta == 0 and tx_delta >= low:
        sev = "high" if tx_delta >= mid else "medium"
        return "tx_only", sev
    if tx_delta == 0 and rx_delta >= low:
        sev = "high" if rx_delta >= mid else "medium"
        return "rx_only", sev
    return None, None


def save_scan(con, scan_id, devices, prev_counters, host_vid):
    for d in devices:
        con.execute(
            """
            INSERT INTO devices(scan_id,ip,identity,board_name,model,version,architecture,uptime,is_crs)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                scan_id,
                d["ip"],
                d.get("identity"),
                d.get("board_name"),
                d.get("model"),
                d.get("version"),
                d.get("architecture"),
                d.get("uptime"),
                1 if d.get("is_crs") else 0,
            ),
        )

        for i in d.get("interfaces", []):
            con.execute(
                """
                INSERT INTO interfaces(
                  scan_id,ip,name,type,running,disabled,slave,mtu,actual_mtu,rx_byte,tx_byte,rx_packet,tx_packet,last_link_up_time
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    scan_id,
                    d["ip"],
                    i.get("name"),
                    i.get("type"),
                    1 if i.get("running") else 0,
                    1 if i.get("disabled") else 0,
                    1 if i.get("slave") else 0,
                    i.get("mtu"),
                    i.get("actual_mtu"),
                    i.get("rx_byte") or 0,
                    i.get("tx_byte") or 0,
                    i.get("rx_packet") or 0,
                    i.get("tx_packet") or 0,
                    i.get("last_link_up_time"),
                ),
            )

            prev = prev_counters.get((d["ip"], i.get("name")))
            if prev and i.get("type") == "ether" and i.get("running") and not i.get("disabled"):
                rx_delta = max(0, (i.get("rx_byte") or 0) - prev[0])
                tx_delta = max(0, (i.get("tx_byte") or 0) - prev[1])
                direction, sev = classify_one_way(rx_delta, tx_delta)
                if direction:
                    con.execute(
                        """
                        INSERT INTO one_way_outliers(scan_id,ip,interface,rx_delta,tx_delta,direction,severity,note)
                        VALUES(?,?,?,?,?,?,?,?)
                        """,
                        (
                            scan_id,
                            d["ip"],
                            i.get("name"),
                            rx_delta,
                            tx_delta,
                            direction,
                            sev,
                            "running-ether one-way byte delta vs previous scan",
                        ),
                    )

        for n in d.get("neighbors", []):
            con.execute(
                """
                INSERT INTO neighbors(scan_id,ip,interface,neighbor_address,neighbor_identity,platform,version)
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    scan_id,
                    d["ip"],
                    n.get("interface"),
                    n.get("address"),
                    n.get("identity"),
                    n.get("platform"),
                    n.get("version"),
                ),
            )

        for bp in d.get("bridge_ports", []):
            con.execute(
                """
                INSERT INTO bridge_ports(scan_id,ip,interface,pvid,ingress_filtering,frame_types,trusted,hw)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    scan_id,
                    d["ip"],
                    bp.get("interface"),
                    bp.get("pvid"),
                    1 if bp.get("ingress_filtering") else 0,
                    bp.get("frame_types"),
                    1 if bp.get("trusted") else 0,
                    1 if bp.get("hw") else 0,
                ),
            )

        for bv in d.get("bridge_vlans", []):
            con.execute(
                """
                INSERT INTO bridge_vlans(scan_id,ip,vlan_ids,tagged,untagged)
                VALUES(?,?,?,?,?)
                """,
                (scan_id, d["ip"], bv.get("vlan_ids"), bv.get("tagged"), bv.get("untagged")),
            )

        for bh in d.get("bridge_hosts", []):
            vid = bh.get("vid")
            if host_vid is not None and vid not in (host_vid, None):
                continue
            con.execute(
                """
                INSERT INTO bridge_hosts(scan_id,ip,mac,on_interface,vid,local,external)
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    scan_id,
                    d["ip"],
                    bh.get("mac"),
                    bh.get("on_interface"),
                    vid,
                    1 if bh.get("local") else 0,
                    1 if bh.get("external") else 0,
                ),
            )

        if d.get("ppp_active"):
            for p in d["ppp_active"]:
                con.execute(
                    """
                    INSERT INTO router_ppp_active(scan_id,router_ip,name,service,caller_id,address,uptime)
                    VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        scan_id,
                        d["ip"],
                        p.get("name"),
                        p.get("service"),
                        p.get("caller_id"),
                        p.get("address"),
                        p.get("uptime"),
                    ),
                )

        if d.get("arp"):
            for a in d["arp"]:
                con.execute(
                    """
                    INSERT INTO router_arp(scan_id,router_ip,address,mac,interface,dynamic)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (
                        scan_id,
                        d["ip"],
                        a.get("address"),
                        a.get("mac"),
                        a.get("interface"),
                        1 if a.get("dynamic") else 0,
                    ),
                )


def purge_old_scans(con, keep_scans):
    rows = con.execute("SELECT id FROM scans ORDER BY id DESC").fetchall()
    old_ids = [r[0] for r in rows[keep_scans:]]
    if old_ids:
        con.executemany("DELETE FROM scans WHERE id=?", [(i,) for i in old_ids])


def run_scan(args):
    user, pw = load_creds(args.env)
    subnet = ipaddress.ip_network(args.subnet, strict=False)
    ips = [str(ip) for ip in subnet.hosts()]

    con = db_connect(args.db)
    init_db(con)

    started = utc_now()
    cur = con.execute(
        "INSERT INTO scans(started_at,subnet,hosts_tested,notes) VALUES(?,?,?,?)",
        (started, args.subnet, len(ips), "read-only api discovery"),
    )
    scan_id = cur.lastrowid
    con.commit()

    prev_id = latest_scan_id(con)
    if prev_id == scan_id:
        prev_id = con.execute("SELECT id FROM scans WHERE id < ? ORDER BY id DESC LIMIT 1", (scan_id,)).fetchone()
        prev_id = prev_id[0] if prev_id else None
    prev_counters = previous_interface_counters(con, prev_id)

    open_ips = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        fut = {ex.submit(tcp_open, ip, 8728, args.tcp_timeout): ip for ip in ips}
        for f in as_completed(fut):
            ip = fut[f]
            if f.result():
                open_ips.append(ip)

    devices = []
    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        fut = {ex.submit(gather_device, ip, user, pw, args.api_timeout): ip for ip in sorted(open_ips)}
        for f in as_completed(fut):
            ip = fut[f]
            try:
                devices.append(f.result())
            except Exception as e:
                failures.append((ip, str(e)))

    host_vid = None if args.host_vid == -1 else args.host_vid
    save_scan(con, scan_id, devices, prev_counters, host_vid)

    con.execute(
        "UPDATE scans SET finished_at=?, api_reachable=? WHERE id=?",
        (utc_now(), len(devices), scan_id),
    )
    purge_old_scans(con, args.keep_scans)
    con.commit()

    summary = {
        "scan_id": scan_id,
        "subnet": args.subnet,
        "hosts_tested": len(ips),
        "tcp_8728_open": len(open_ips),
        "api_reachable": len(devices),
        "failures": len(failures),
        "router_like": sum(1 for d in devices if not d.get("is_crs")),
        "crs_switches": sum(1 for d in devices if d.get("is_crs")),
        "one_way_outliers": con.execute(
            "SELECT COUNT(*) FROM one_way_outliers WHERE scan_id=?", (scan_id,)
        ).fetchone()[0],
    }

    print(json.dumps(summary, indent=2))
    if failures:
        print("recent_failures:")
        for ip, err in failures[:20]:
            print(f"  {ip}: {err}")


def report_latest(args):
    con = db_connect(args.db)
    init_db(con)

    row = con.execute(
        "SELECT id,started_at,finished_at,subnet,hosts_tested,api_reachable FROM scans ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not row:
        print("No scans in database.")
        return

    scan_id = row[0]
    print(f"latest_scan_id={scan_id} started={row[1]} finished={row[2]} subnet={row[3]}")
    print(f"hosts_tested={row[4]} api_reachable={row[5]}")

    dev = con.execute(
        "SELECT COUNT(*), SUM(is_crs), SUM(CASE WHEN is_crs=0 THEN 1 ELSE 0 END) FROM devices WHERE scan_id=?",
        (scan_id,),
    ).fetchone()
    print(f"devices_total={dev[0]} crs_switches={dev[1] or 0} non_crs={dev[2] or 0}")

    print("\nLikely Uplinks By Device:")
    rows = con.execute(
        """
        SELECT d.ip, d.identity, n.interface, n.neighbor_identity, n.neighbor_address
        FROM devices d
        JOIN neighbors n ON n.scan_id=d.scan_id AND n.ip=d.ip
        WHERE d.scan_id=?
          AND n.neighbor_identity IS NOT NULL
          AND n.neighbor_identity<>''
          AND (
            lower(coalesce(n.interface,'')) LIKE 'sfp%'
            OR lower(coalesce(n.interface,'')) LIKE 'roof%'
            OR lower(coalesce(n.interface,'')) LIKE 'wlan%'
            OR lower(coalesce(n.interface,'')) LIKE 'ether24%'
          )
        ORDER BY d.ip, n.interface, n.neighbor_identity
        """,
        (scan_id,),
    ).fetchall()
    by_dev = defaultdict(list)
    for ip, ident, iface, n_ident, n_addr in rows:
        by_dev[(ip, ident or "?")].append((iface or "?", n_ident or "?", n_addr or "n/a"))
    shown = 0
    for (ip, ident), links in by_dev.items():
        print(f"{ip} {ident}")
        dedup = []
        seen = set()
        for l in links:
            if l in seen:
                continue
            seen.add(l)
            dedup.append(l)
        for iface, n_ident, n_addr in dedup[:4]:
            print(f"  {iface} -> {n_ident} ({n_addr})")
        if len(dedup) > 4:
            print(f"  ... +{len(dedup) - 4} more")
        shown += 1
        if shown >= args.limit:
            break

    print("\nOne-way Outliers (latest):")
    out_rows = con.execute(
        """
        SELECT o.ip, d.identity, o.interface, o.direction, o.severity, o.rx_delta, o.tx_delta
        FROM one_way_outliers o
        LEFT JOIN devices d ON d.scan_id=o.scan_id AND d.ip=o.ip
        WHERE o.scan_id=?
        ORDER BY CASE o.severity WHEN 'high' THEN 0 ELSE 1 END, o.ip, o.interface
        LIMIT ?
        """,
        (scan_id, args.limit),
    ).fetchall()
    if not out_rows:
        print("none")
    else:
        for r in out_rows:
            print(
                f"{r[0]} {r[1] or '?'} {r[2]} {r[3]} {r[4]} rx_delta={r[5]} tx_delta={r[6]}"
            )


def export_graph(args):
    con = db_connect(args.db)
    init_db(con)
    row = con.execute("SELECT id FROM scans ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        raise RuntimeError("No scans found")
    scan_id = row[0]

    nodes = []
    for r in con.execute(
        "SELECT ip, identity, board_name, version, is_crs FROM devices WHERE scan_id=? ORDER BY ip",
        (scan_id,),
    ):
        nodes.append(
            {
                "ip": r[0],
                "identity": r[1],
                "board_name": r[2],
                "version": r[3],
                "is_crs": bool(r[4]),
            }
        )

    edges = []
    for r in con.execute(
        """
        SELECT n.ip, n.interface, n.neighbor_identity, n.neighbor_address
        FROM neighbors n
        WHERE n.scan_id=?
        """,
        (scan_id,),
    ):
        edges.append(
            {
                "from_ip": r[0],
                "from_interface": r[1],
                "to_identity": r[2],
                "to_address": r[3],
            }
        )
    uniq = []
    seen = set()
    for e in edges:
        k = (e["from_ip"], e["from_interface"], e["to_identity"], e["to_address"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(e)

    out = {"scan_id": scan_id, "nodes": nodes, "edges": uniq}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print(f"wrote {args.out} with {len(nodes)} nodes and {len(uniq)} neighbor edges")


def path_lookup(args):
    con = db_connect(args.db)
    init_db(con)
    row = con.execute("SELECT id FROM scans ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        raise RuntimeError("No scans found")
    scan_id = row[0]
    mac = mac_norm(args.mac)
    if not mac:
        raise RuntimeError("Invalid MAC format")

    q = """
      SELECT b.ip, d.identity, b.on_interface, b.vid, b.local, b.external
      FROM bridge_hosts b
      LEFT JOIN devices d ON d.scan_id=b.scan_id AND d.ip=b.ip
      WHERE b.scan_id=? AND b.mac=?
    """
    params = [scan_id, mac]
    if args.vid is not None:
        q += " AND b.vid=?"
        params.append(args.vid)
    q += " ORDER BY b.ip, b.on_interface"
    rows = con.execute(q, tuple(params)).fetchall()

    print(f"scan_id={scan_id} mac={mac} matches={len(rows)}")
    for r in rows:
        print(
            f"{r[0]} {r[1] or '?'} {r[2] or '?'} vid={r[3]} local={bool(r[4])} external={bool(r[5])}"
        )


def build_parser():
    p = argparse.ArgumentParser(description="Read-only MikroTik network mapper with SQLite backend")
    p.add_argument("--db", default="network_map.db", help="SQLite database path")
    p.add_argument("--env", default=".env", help="Credentials env file path")

    sub = p.add_subparsers(dest="cmd", required=True)

    ps = sub.add_parser("scan", help="Run full subnet discovery + snapshot")
    ps.add_argument("--subnet", default="192.168.44.0/24")
    ps.add_argument("--workers", type=int, default=48)
    ps.add_argument("--tcp-timeout", type=float, default=0.7)
    ps.add_argument("--api-timeout", type=float, default=3.0)
    ps.add_argument("--keep-scans", type=int, default=20)
    ps.add_argument(
        "--host-vid",
        type=int,
        default=20,
        help="Store bridge host rows only for this VLAN ID (and null VID). Use -1 to keep all.",
    )

    pr = sub.add_parser("report", help="Report latest snapshot and one-way outliers")
    pr.add_argument("--limit", type=int, default=120)

    pg = sub.add_parser("export-graph", help="Export latest snapshot graph JSON")
    pg.add_argument("--out", default="network_graph_latest.json")

    pp = sub.add_parser("path", help="Find where a MAC is learned in latest snapshot")
    pp.add_argument("--mac", required=True, help="MAC address, e.g. E8:DA:00:14:E9:B3")
    pp.add_argument("--vid", type=int, default=None, help="Optional VLAN ID filter")

    return p


def main():
    args = build_parser().parse_args()
    if args.cmd == "scan":
        run_scan(args)
    elif args.cmd == "report":
        report_latest(args)
    elif args.cmd == "export-graph":
        export_graph(args)
    elif args.cmd == "path":
        path_lookup(args)


if __name__ == "__main__":
    main()
