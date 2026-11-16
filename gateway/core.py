"""One benchmark request: context -> LLM query generation -> execution, with spans.

Conditions (the 2x2): {pg,neo}-{no-onto,onto}. Replay conditions for Stage B
and the runtime study skip context and the LLM and execute a stored query:
replay-pg-<single|parallel|default>, replay-neo-<slotted|pipelined|parallel|default>.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from gateway import llm
from gateway.executors import NeoExecutor, PgExecutor
from gateway.schema_context import neo_schema, pg_schema
from gateway.tracing import tracer

NO_ANSWER = "NO_ANSWER"

SYSTEM = {
    "pg": (
        "You translate a question about SEC filings data into ONE read-only PostgreSQL 16 query.\n"
        "Rules: use only tables and columns from the schema; return only the columns the question asks for "
        "(plus names that identify the rows); no comments. Express ratios, margins and growth rates as decimal fractions "
        "(0.25 means 25%). Reply with the query in a ```sql block. "
        f"If the data cannot answer the question, reply exactly {NO_ANSWER}."
    ),
    "neo": (
        "You translate a question about SEC filings data into ONE read-only Cypher query for Neo4j 5.\n"
        "Rules: use only labels, relationship types and properties from the graph schema; return only the values "
        "the question asks for (plus names that identify the rows); no comments. Express ratios, margins and growth rates as "
        "decimal fractions (0.25 means 25%). Reply with the query in a ```cypher block. "
        f"If the data cannot answer the question, reply exactly {NO_ANSWER}."
    ),
}


@dataclass
class Gateway:
    db: str = "finder_real"
    pool_size: int = 16
    answerable_threshold: float | None = None   # onto arms: reject below this coverage (set from dev split)
    pg: PgExecutor = field(init=False)
    neo: NeoExecutor = field(init=False)
    http: httpx.AsyncClient = field(init=False)
    onto: Any = field(init=False, default=None)

    async def start(self) -> None:
        self.pg = PgExecutor(self.db, self.pool_size)
        self.neo = NeoExecutor(self.pool_size)
        self.http = httpx.AsyncClient(limits=httpx.Limits(max_connections=64))
        await self.pg.start()
        await self.neo.start()
        from gateway.onto_context import OntologyContext

        self.onto = OntologyContext(self.http)

    async def close(self) -> None:
        await self.pg.close()
        await self.neo.close()
        await self.http.aclose()

    @staticmethod
    def extract_query(text: str, engine: str) -> str | None:
        if NO_ANSWER in text and "```" not in text:
            return None
        m = re.search(r"```(?:sql|cypher|postgresql)?\s*(.*?)```", text, re.S | re.I)
        q = (m.group(1) if m else text).strip()
        return q or None

    async def run(self, question: str, condition: str, *, cq_id: str = "", request_id: str = "",
                  replay_query: str | None = None, run_tag: str = "") -> dict[str, Any]:
        parts = condition.split("-")
        out: dict[str, Any] = {"cq_id": cq_id, "condition": condition, "request_id": request_id}
        meta = {"bench": run_tag or "adhoc", "cq": cq_id, "cond": condition, "rid": request_id}
        with tracer.start_as_current_span("request", attributes={"cq_id": cq_id, "condition": condition,
                                                                   "request_id": request_id}) as root:
            t0 = time.perf_counter()
            if parts[0] == "replay":
                engine, runtime = parts[1], parts[2] if len(parts) > 2 else "default"
                query = replay_query or ""
                out.update(engine=engine, query=query)
            else:
                engine, arm = parts[0], "-".join(parts[1:])
                out.update(engine=engine, arm=arm)
                runtime = "default"
                context = pg_schema(self.db) if engine == "pg" else neo_schema()
                if arm == "onto":
                    with tracer.start_as_current_span("ctx.lookup") as sp:
                        c0 = time.perf_counter()
                        try:
                            oc = await self.onto.context(question, engine)
                        except (httpx.HTTPError, KeyError, ValueError) as e:
                            out.update(error_type="CTX_ERROR", error=f"{type(e).__name__}: {e}"[:300],
                                       t_ctx_ms=(time.perf_counter() - c0) * 1000, e2e_ms=(time.perf_counter() - t0) * 1000)
                            return out
                        # t_ctx = live term lookups + the (cached, serially measured) check_answerable latency
                        out["t_ctx_live_ms"] = (time.perf_counter() - c0) * 1000
                        out["t_ctx_ms"] = out["t_ctx_live_ms"] + oc.get("coverage_ms", 0)
                        out["coverage"] = oc["coverage"]
                        out["ctx_calls"] = oc["calls"]
                        out["ctx_terms"] = oc["terms"]
                        out["ctx_chars"] = len(oc["text"])
                        out["ctx_matched_sql"] = oc.get("matched_sql_attributes", [])
                        sp.set_attributes({"coverage": oc["coverage"] or -1, "t_ctx_ms": out["t_ctx_ms"],
                                           "terms": len(oc["terms"])})
                    if (self.answerable_threshold is not None and oc["coverage"] is not None
                            and oc["coverage"] < self.answerable_threshold):
                        out.update(rejected=True, reject_stage="check_answerable", e2e_ms=(time.perf_counter() - t0) * 1000)
                        root.set_attribute("rejected", True)
                        return out
                    context = context + "\n\n" + oc["text"]
                out["context_chars"] = len(context)
                with tracer.start_as_current_span("llm.query_gen") as sp:
                    try:
                        res = await llm.complete(self.http, [
                            {"role": "system", "content": SYSTEM[engine]},
                            {"role": "user", "content": f"{'SCHEMA' if engine == 'pg' else 'GRAPH SCHEMA'}:\n{context}\n\nQUESTION: {question}"},
                        ])
                    except llm.RateLimited:
                        out.update(error_type="RATE_LIMITED", e2e_ms=(time.perf_counter() - t0) * 1000)
                        return out
                    except httpx.HTTPError as e:
                        out.update(error_type="LLM_ERROR", error=str(e)[:300], e2e_ms=(time.perf_counter() - t0) * 1000)
                        return out
                    out["llm"] = {k: v for k, v in res.items() if k != "text"}
                    sp.set_attributes({k: v for k, v in out["llm"].items() if isinstance(v, (int, float, str))})
                query = self.extract_query(res["text"], engine)
                out["query"] = query
                if query is None:
                    out.update(rejected=True, reject_stage="llm", e2e_ms=(time.perf_counter() - t0) * 1000)
                    return out
            with tracer.start_as_current_span("db.query_exec", attributes={"engine": engine, "runtime_requested": runtime}) as sp:
                if engine == "pg":
                    r = await self.pg.run(query, runtime=runtime, tag=f"{run_tag}:{cq_id}")
                else:
                    r = await self.neo.run(query, runtime=runtime, meta=meta)
                db = {k: v for k, v in r.items() if k not in ("rows",)}
                if r.get("ok") and "client_wall_ms" in r and "server_ms" in r:
                    db["overhead_ms"] = r["client_wall_ms"] - r["server_ms"]
                sp.set_attributes({k: v for k, v in db.items() if isinstance(v, (int, float, str, bool))})
            out["db"] = db
            out["rows"] = r.get("rows")
            if not r.get("ok"):
                out["error_type"] = r.get("error_type")
            out["e2e_ms"] = (time.perf_counter() - t0) * 1000
            return out
