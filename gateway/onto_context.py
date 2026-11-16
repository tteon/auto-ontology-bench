"""Ontology context (the `onto` arms) from Auto-Ontology's REST API.

Calls the same endpoints the MCP tools wrap, directly, to keep MCP protocol
overhead out of the measurement:
  check_answerable        POST /api/question-entity-coverage
  get_term                GET  /api/terms/{id}
  get_term_columns        GET  /api/terms/{id}/column-attributes
  get_term_sql_attributes GET  /api/terms/{id}/sql-attributes

For Cypher the SQL-side names are translated with graph_sync/mapping.json:
table -> label (or -> relationship when the table was a join table),
column -> property of the same name.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
API = os.environ.get("AUTO_ONTOLOGY_API_URL", "http://127.0.0.1:3001")
MAX_TERMS = 6


class OntologyContext:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self.http = http
        m = json.loads((ROOT / "graph_sync" / "mapping.json").read_text())
        self.table_to_graph: dict[str, str] = {t: f"(:{n['label']})" for t, n in m["nodes"].items()}
        for r in m["relationships"]:
            if r["replaces_join_table"]:
                self.table_to_graph[r["from_table"]] = f"relationship (:{r['from']})-[:{r['type']}]->(:{r['to']})"
        self.term_ids: dict[str, str] | None = None
        self.sql_by_name: dict[str, dict] | None = None
        cache = ROOT / "ontology" / "coverage_cache.json"
        self.coverage_cache: dict[str, dict] = json.loads(cache.read_text()) if cache.exists() else {}
        ossie = ROOT / "ontology" / "export" / "ossie_graph_mapping.json"
        self.ossie: dict[str, dict] = json.loads(ossie.read_text()) if ossie.exists() else {}

    async def _call(self, calls: list[dict], name: str, method: str, path: str, **kw: Any) -> Any:
        t = time.perf_counter()
        r = await self.http.request(method, f"{API}{path}", timeout=180, **kw)
        calls.append({"tool": name, "ms": (time.perf_counter() - t) * 1000, "status": r.status_code})
        r.raise_for_status()
        return r.json()

    async def context(self, question: str, engine: str) -> dict[str, Any]:
        calls: list[dict] = []
        cached = self.coverage_cache.get(question)
        if cached and cached.get("data") is not None:
            # Serialized server-side (Auto-Ontology `_run_lock`), so precomputed once per question;
            # its unloaded latency is carried into t_ctx by the caller via `coverage_ms`.
            cov = cached["data"]
            calls.append({"tool": "check_answerable", "ms": cached["ms"], "status": cached["status"], "cached": True})
        else:
            cov = (await self._call(calls, "check_answerable", "POST", "/api/question-entity-coverage",
                                    json={"question": question}))["data"]
        # A candidate's `id` is the matched attribute (ColumnAttribute or SqlAttribute), not a Term;
        # the Term is given by name, so resolve names through the term list.
        if self.term_ids is None:
            listing = await self._call(calls, "list_terms", "GET", "/api/terms", params={"limit": 100})
            self.term_ids = {t["name"]: t["id"] for t in listing["terms"]}
        # Matched SQL attributes are resolved by name (ids are instance-specific, so a coverage cache
        # built against one Auto-Ontology instance stays valid on another).
        if self.sql_by_name is None:
            listing = await self._call(calls, "list_sql_attributes", "GET", "/api/sql-attributes", params={"limit": 100})
            self.sql_by_name = {s["name"]: s for s in listing.get("data", [])}
        cands = cov.get("candidates", []) or []
        term_ids = list(dict.fromkeys(self.term_ids[c["term"]] for c in cands if c.get("term") in self.term_ids))[:MAX_TERMS]
        matched_sql_list = [self.sql_by_name[c["attribute"]] for c in cands
                            if c.get("label") == "SqlAttribute" and c.get("attribute") in self.sql_by_name]

        async def term_bundle(tid: str) -> dict[str, Any]:
            detail, cols, sqls = await asyncio.gather(
                self._call(calls, "get_term", "GET", f"/api/terms/{tid}"),
                self._call(calls, "get_term_columns", "GET", f"/api/terms/{tid}/column-attributes", params={"limit": 50}),
                self._call(calls, "get_term_sql_attributes", "GET", f"/api/terms/{tid}/sql-attributes", params={"limit": 50}),
            )
            return {"detail": detail.get("data"), "columns": cols.get("data", []), "sql": sqls.get("data", [])}

        bundles = await asyncio.gather(*(term_bundle(t) for t in term_ids))
        matched = {"columns": {c["attribute"] for c in cands if c.get("label") == "ColumnAttribute" and c.get("attribute")},
                   "sql": list({s["name"]: s for s in matched_sql_list}.values())}
        text = self.render(cov, bundles, engine, matched)
        cached_ms = sum(c["ms"] for c in calls if c.get("cached"))
        return {"coverage": cov.get("coverage"), "uncovered": cov.get("uncovered_entities"), "calls": calls, "coverage_ms": cached_ms,
                "terms": [b["detail"]["name"] for b in bundles if b["detail"]],
                "matched_sql_attributes": [m.get("name") for m in matched["sql"]], "text": text}

    def render(self, cov: dict, bundles: list[dict], engine: str, matched: dict | None = None) -> str:
        matched = matched or {"columns": set(), "sql": []}
        lines = ["SEMANTIC CONTEXT (business ontology for this database):"]
        if matched["sql"]:
            lines.append("\nCertified calculations matched to this question (use these definitions):")
            for s in matched["sql"]:
                lines.append(f"  * {s.get('name')}: {(s.get('description') or '').strip()[:300]}\n    SQL: {(s.get('sql') or s.get('expression') or '')[:900]}")
        if matched["columns"]:
            lines.append("\nColumns matched to this question: " + ", ".join(sorted(matched["columns"])))
        matched_sql_names = {s.get("name") for s in matched["sql"]}
        tables: set[str] = set()
        for b in bundles:
            d = b["detail"]
            if not d:
                continue
            syn = f" (synonyms: {', '.join(d['synonyms'])})" if d.get("synonyms") else ""
            lines.append(f"\nTerm: {d.get('name')}{syn}\n  {d.get('description') or ''}".rstrip())
            for t in d.get("tables") or []:
                if t.get("name"):
                    tables.add(t["name"])
            for c in b["columns"]:
                pc = c.get("primary_column") or {}
                where = f"{pc.get('table_name')}.{pc.get('column_name')}" if pc.get("table_name") else (c.get("source_column") or "")
                if pc.get("table_name"):
                    tables.add(pc["table_name"])
                samples = c.get("sample_values") or []
                sv = f" e.g. {', '.join(map(str, samples[:5]))}" if samples else ""
                mark = " [matched]" if c.get("name") in matched["columns"] else ""
                lines.append(f"  - {c.get('name')}{mark}: {where} -- {(c.get('description') or '').strip()[:200]}{sv}")
            other = [x for x in b["sql"] if x.get("name") not in matched_sql_names]
            if other:
                lines.append("  other certified calculations on this term (name: definition):")
                for x in other:
                    lines.append(f"    * {x.get('name')}: {(x.get('description') or '').strip()[:160]}")
        if cov.get("uncovered_entities"):
            lines.append(f"\nNot covered by the ontology: {', '.join(cov['uncovered_entities'])}")
        if engine == "neo" and tables:
            lines.append("\nGraph mapping of these concepts (from the Apache Ossie export):")
            for t in sorted({t.split('.')[-1] for t in tables}):
                o = self.ossie.get(t)
                if o:
                    fields = ", ".join(f"{k}={v}" for k, v in list(o["fields"].items())[:12])
                    lines.append(f"  {o['concept']}: {o['graph']}  fields: {fields}")
                else:
                    lines.append(f"  {t}: {self.table_to_graph.get(t, '?')}")
            lines.append("Foreign keys are relationships. Calculations are given in SQL: express the same formula in Cypher.")
        return "\n".join(lines)
