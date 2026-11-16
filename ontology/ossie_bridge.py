"""Bridge Auto-Ontology's Apache Ossie export onto the Neo4j graph model.

    uv run python -m ontology.ossie_bridge     -> ontology/export/ossie_graph_mapping.json

Reads ontology/export/ossie/semantic_model.yaml (dataset -> source table,
business field name -> column expression) and graph_sync/mapping.json (table ->
label or relationship), and writes, per table, the graph element plus the
business-name -> graph-property map. The Cypher `onto` context uses it so that
"stock_ticker_symbol" is presented as `(:Company).ticker`.
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def build() -> dict:
    sm = yaml.safe_load((ROOT / "ontology/export/ossie/semantic_model.yaml").read_text())
    gm = json.loads((ROOT / "graph_sync/mapping.json").read_text())
    rel_for_table = {r["from_table"]: r for r in gm["relationships"] if r["replaces_join_table"]}
    out = {}
    for ds in sm.get("datasets", []):
        table = ds["source"].split(".")[-1]
        if table in rel_for_table:
            r = rel_for_table[table]
            element, var = f"(:{r['from']})-[x:{r['type']}]->(:{r['to']})", "x"
        elif table in gm["nodes"]:
            element, var = f"(x:{gm['nodes'][table]['label']})", "x"
        else:
            continue
        fields = {}
        for f in ds.get("fields", []):
            exprs = (f.get("expression") or {}).get("dialects") or []
            col = exprs[0]["expression"] if exprs else None
            if col and col.isidentifier():
                fields[f["name"]] = f"{var}.{col}"
        out[table] = {"concept": ds["name"], "graph": element, "description": (ds.get("description") or "")[:300],
                      "fields": fields}
    return out


def main() -> None:
    m = build()
    path = ROOT / "ontology/export/ossie_graph_mapping.json"
    path.write_text(json.dumps(m, indent=1))
    print(f"{len(m)} datasets mapped; e.g. companies -> {m.get('companies', {}).get('graph')} "
          f"{list(m.get('companies', {}).get('fields', {}).items())[:3]}")


if __name__ == "__main__":
    main()
