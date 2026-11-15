"""Probe which Neo4j Enterprise features the Aura instance exposes.

    uv run python -m enterprise.probe     -> results/enterprise_probe.json

Each probe runs one statement and records ok / blocked (with the server's
message). Temporary schema objects use the label `AobProbe` and are dropped at
the end, so the probe leaves nothing behind.
"""

from __future__ import annotations

import json
from pathlib import Path

from neo4j.exceptions import Neo4jError

from enterprise.conn import DATABASE, driver

ROOT = Path(__file__).resolve().parents[1]

PROBES: list[tuple[str, str, str]] = [
    # area, feature, statement
    ("server", "dbms.components", "CALL dbms.components() YIELD name, versions, edition RETURN *"),
    ("server", "db.info", "CALL db.info()"),
    ("server", "SHOW DATABASES (store format)", "SHOW DATABASES YIELD name, type, store, currentStatus, currentPrimariesCount RETURN *"),
    ("server", "SHOW SERVERS", "SHOW SERVERS"),
    ("runtime", "slotted", "CYPHER runtime=slotted EXPLAIN MATCH (n) RETURN count(n)"),
    ("runtime", "pipelined", "CYPHER runtime=pipelined EXPLAIN MATCH (n) RETURN count(n)"),
    ("runtime", "parallel", "CYPHER runtime=parallel EXPLAIN MATCH (n) RETURN count(n)"),
    ("runtime", "Cypher 25", "CYPHER 25 RETURN 1 AS x"),
    ("monitoring", "SHOW TRANSACTIONS (all columns)", "SHOW TRANSACTIONS YIELD *"),
    ("monitoring", "dbms.listPools", "CALL dbms.listPools()"),
    ("monitoring", "dbms.listConnections", "CALL dbms.listConnections()"),
    ("monitoring", "dbms.listActiveLocks", "CALL dbms.listActiveLocks('query-0')"),
    ("monitoring", "SHOW INDEXES readCount/lastRead", "SHOW INDEXES YIELD name, readCount, lastRead, trackedSince RETURN *"),
    ("monitoring", "dbms.queryJmx", "CALL dbms.queryJmx('*:*') YIELD name RETURN count(*) AS beans"),
    ("monitoring", "db.stats.retrieve", "CALL db.stats.retrieve('GRAPH COUNTS')"),
    ("monitoring", "SHOW SETTINGS", "SHOW SETTINGS YIELD name RETURN count(*) AS n"),
    ("monitoring", "dbms.listConfig", "CALL dbms.listConfig() YIELD name RETURN count(*) AS n"),
    ("monitoring", "CDC db.cdc.current", "CALL db.cdc.current()"),
    ("schema", "property existence constraint", "CREATE CONSTRAINT aob_probe_exists IF NOT EXISTS FOR (n:AobProbe) REQUIRE n.x IS NOT NULL"),
    ("schema", "property type constraint", "CREATE CONSTRAINT aob_probe_type IF NOT EXISTS FOR (n:AobProbe) REQUIRE n.x IS :: INTEGER"),
    ("schema", "node key constraint", "CREATE CONSTRAINT aob_probe_key IF NOT EXISTS FOR (n:AobProbe) REQUIRE (n.x, n.y) IS NODE KEY"),
    ("schema", "relationship key constraint", "CREATE CONSTRAINT aob_probe_relkey IF NOT EXISTS FOR ()-[r:AOB_PROBE]-() REQUIRE r.k IS RELATIONSHIP KEY"),
    ("security", "SHOW CURRENT USER", "SHOW CURRENT USER"),
    ("security", "SHOW USERS", "SHOW USERS"),
    ("security", "SHOW ROLES", "SHOW ROLES"),
    ("security", "SHOW PRIVILEGES", "SHOW PRIVILEGES"),
    ("security", "CREATE ROLE", "CREATE ROLE aob_probe_role IF NOT EXISTS"),
    ("library", "APOC", "RETURN apoc.version() AS v"),
    ("library", "GDS", "RETURN gds.version() AS v"),
]

CLEANUP = [
    "DROP CONSTRAINT aob_probe_exists IF EXISTS",
    "DROP CONSTRAINT aob_probe_type IF EXISTS",
    "DROP CONSTRAINT aob_probe_key IF EXISTS",
    "DROP CONSTRAINT aob_probe_relkey IF EXISTS",
    "DROP ROLE aob_probe_role IF EXISTS",
]


def _jsonable(v: object) -> object:
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    return str(v)


def main() -> None:
    out = []
    with driver() as d:
        for area, feature, stmt in PROBES:
            try:
                records, summary, keys = d.execute_query(stmt, database_=DATABASE)
                rows = [_jsonable(dict(r)) for r in records[:5]]
                entry = {"area": area, "feature": feature, "status": "ok", "columns": list(keys), "sample": rows}
                if summary.plan:
                    entry["runtime"] = summary.plan.get("args", {}).get("runtime")
            except Neo4jError as e:
                entry = {"area": area, "feature": feature, "status": "blocked", "code": e.code, "message": (e.message or "")[:200]}
            out.append(entry)
            print(f"{entry['status']:8} {area:10} {feature}" + (f"  [{entry.get('runtime')}]" if entry.get("runtime") else ""))
        for stmt in CLEANUP:
            try:
                d.execute_query(stmt, database_=DATABASE)
            except Neo4jError:
                pass
    (ROOT / "results").mkdir(exist_ok=True)
    (ROOT / "results" / "enterprise_probe.json").write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
