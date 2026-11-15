"""Schema-only context (the `no-onto` arms).

Postgres: CREATE TABLE statements rebuilt from the catalog -- columns, types,
primary and foreign keys -- with no comments, so "schema only" carries no
descriptions that the ontology would otherwise supply.

Neo4j: node labels with properties and types, relationship types with their
endpoints and properties, from graph_sync/mapping.json plus the live
db.schema introspection for property types.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import psycopg

from data_pipeline.load_structured import connect
from enterprise.conn import DATABASE, driver

ROOT = Path(__file__).resolve().parents[1]


@lru_cache(maxsize=4)
def pg_schema(db: str = "finder_real") -> str:
    import os

    with psycopg.connect(host="127.0.0.1", port=int(os.environ["BENCH_PG_PORT"]), user="bench",
                         password=os.environ["BENCH_PG_PASSWORD"], dbname=db) as conn, conn.cursor() as cur:
        cur.execute("""SELECT table_name, column_name, data_type FROM information_schema.columns
                       WHERE table_schema = 'public' AND table_name NOT LIKE 'pg_%' ORDER BY table_name, ordinal_position""")
        cols: dict[str, list[str]] = {}
        for t, c, ty in cur.fetchall():
            cols.setdefault(t, []).append(f"  {c} {ty}")
        cur.execute("""SELECT tc.table_name, tc.constraint_type, kcu.column_name, ccu.table_name, ccu.column_name
                       FROM information_schema.table_constraints tc
                       JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                       JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
                       WHERE tc.table_schema = 'public' AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')""")
        for t, kind, col, rt, rc in cur.fetchall():
            if t in cols:
                cols[t].append(f"  PRIMARY KEY ({col})" if kind == "PRIMARY KEY" else f"  FOREIGN KEY ({col}) REFERENCES {rt}({rc})")
    return "\n\n".join(f"CREATE TABLE {t} (\n" + ",\n".join(dict.fromkeys(c)) + "\n);" for t, c in cols.items())


@lru_cache(maxsize=1)
def neo_schema() -> str:
    mapping = json.loads((ROOT / "graph_sync" / "mapping.json").read_text())
    types: dict[tuple[str, str], str] = {}
    try:
        with driver() as d:
            rows, _, _ = d.execute_query(
                "CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName, propertyTypes "
                "RETURN nodeLabels, propertyName, propertyTypes", database_=DATABASE)
            for r in rows:
                for lab in r["nodeLabels"]:
                    types[(lab, r["propertyName"])] = "|".join(r["propertyTypes"] or [])
            rows, _, _ = d.execute_query(
                "CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes "
                "RETURN relType, propertyName, propertyTypes", database_=DATABASE)
            for r in rows:
                if r["propertyName"]:
                    types[(r["relType"].strip(":`"), r["propertyName"])] = "|".join(r["propertyTypes"] or [])
    except Exception:
        pass
    lines = ["Node labels:"]
    for n in mapping["nodes"].values():
        props = ", ".join(f"{p}: {types.get((n['label'], p), '?')}" for p in n["properties"])
        lines.append(f"(:{n['label']} {{{props}}})")
    lines.append("\nRelationship types:")
    seen = set()
    for r in mapping["relationships"]:
        key = (r["from"], r["type"], r["to"])
        if key in seen:
            continue
        seen.add(key)
        props = ", ".join(f"{p}: {types.get((r['type'], p), '?')}" for p in r["properties"] if p)
        lines.append(f"(:{r['from']})-[:{r['type']}{' {' + props + '}' if props else ''}]->(:{r['to']})")
    return "\n".join(lines)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
    print(pg_schema()[:1500])
    print("...")
    print(neo_schema()[:1500])
    _ = connect
