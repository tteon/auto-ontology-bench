"""Archive everything we know about the Aura instance, then (optionally) snapshot and pause it.

    uv run python -m enterprise.records                 # DB-side + Aura API records only
    uv run python -m enterprise.records --shutdown      # ... then snapshot (await) and pause

DB-side records come over Bolt (always available). Aura API records need an Aura CLI credential that the
operator adds themselves (`aura-cli credential add --name ... --client-id ... --client-secret ...` and
`aura-cli credential use <name>`); without one those steps are recorded as skipped and --shutdown refuses.
Writes results/aura_instance/<UTC timestamp>/ (git-ignored; contains the instance id).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from enterprise.conn import DATABASE, driver
from enterprise.metrics import jmx_read

ROOT = Path(__file__).resolve().parents[1]
AURA_CLI = os.environ.get("AURA_CLI", str(Path.home() / ".local/opt/aura-cli-1.9.1/aura-cli"))

DB_QUERIES: dict[str, str] = {
    "components": "CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition",
    "databases": "SHOW DATABASES YIELD *",
    "indexes": "SHOW INDEXES YIELD *",
    "constraints": "SHOW CONSTRAINTS YIELD *",
    "settings": "SHOW SETTINGS YIELD name, value, isDynamic, description RETURN name, value, isDynamic, description",
    "procedures": "SHOW PROCEDURES YIELD name, mode, worksOnSystem RETURN name, mode, worksOnSystem",
    "functions": "SHOW FUNCTIONS YIELD name, category RETURN name, category",
    "transactions": "SHOW TRANSACTIONS YIELD transactionId, currentQuery, status, elapsedTime, metaData "
    "RETURN transactionId, currentQuery, status, toString(elapsedTime) AS elapsed, metaData",
    "labels": "CALL db.labels() YIELD label CALL (label) { MATCH (n) WHERE label IN labels(n) RETURN count(n) AS n } "
    "RETURN label, n ORDER BY label",
    "relationship_types": "CALL db.relationshipTypes() YIELD relationshipType AS t "
    "CALL (t) { MATCH ()-[r]->() WHERE type(r) = t RETURN count(r) AS n } RETURN t, n ORDER BY t",
    "store_size": "CALL dbms.queryJmx('*:*') YIELD name, attributes WHERE name CONTAINS 'StoreSize' RETURN name, attributes",
}


def _jsonable(v: Any) -> Any:
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return str(v)


def db_records(out: Path) -> dict[str, Any]:
    status: dict[str, Any] = {}
    with driver() as d:
        for name, q in DB_QUERIES.items():
            try:
                rec, _, _ = d.execute_query(q, database_=DATABASE)
                rows = [_jsonable(r.data()) for r in rec]
                (out / f"db_{name}.json").write_text(json.dumps(rows, indent=1, ensure_ascii=False))
                status[name] = len(rows)
            except Exception as e:  # noqa: BLE001 - record and keep going; Aura forbids some calls
                status[name] = f"error: {type(e).__name__}: {str(e)[:160]}"
        try:
            (out / "db_jmx_metrics.json").write_text(json.dumps(jmx_read(d), indent=1))
            status["jmx_metrics"] = "ok"
        except Exception as e:  # noqa: BLE001
            status["jmx_metrics"] = f"error: {e}"[:200]
    return status


def cli(*args: str) -> tuple[int, str]:
    p = subprocess.run([AURA_CLI, *args, "--output", "json"], capture_output=True, text=True, timeout=1800)
    return p.returncode, (p.stdout or p.stderr).strip()


def has_credential() -> bool:
    rc, txt = cli("credential", "list")
    try:
        return rc == 0 and len(json.loads(txt)) > 0
    except json.JSONDecodeError:
        return False


def api_records(out: Path, instance_id: str) -> dict[str, Any]:
    status: dict[str, Any] = {}
    steps = {
        "instances": ("instance", "list"),
        "instance": ("instance", "get", instance_id),
        "snapshots_today": ("instance", "snapshot", "list", "--instance-id", instance_id),
        "tenants": ("tenant", "list"),
    }
    for name, args in steps.items():
        rc, txt = cli(*args)
        (out / f"api_{name}.json").write_text(txt + "\n")
        status[name] = "ok" if rc == 0 else f"rc={rc}"
    try:
        for t in json.loads((out / "api_tenants.json").read_text()):
            rc, txt = cli("tenant", "get", t["id"])
            (out / f"api_tenant_{t['id'][:8]}.json").write_text(txt + "\n")
    except (json.JSONDecodeError, KeyError, TypeError):
        status["tenant_detail"] = "skipped"
    return status


def shutdown(out: Path, instance_id: str) -> dict[str, Any]:
    log: dict[str, Any] = {}
    t0 = time.time()
    rc, txt = cli("instance", "snapshot", "create", "--instance-id", instance_id, "--await")
    (out / "api_snapshot_create.json").write_text(txt + "\n")
    log["snapshot"] = {"rc": rc, "seconds": round(time.time() - t0, 1)}
    if rc != 0:
        raise SystemExit(f"snapshot failed (rc={rc}); not pausing. See {out / 'api_snapshot_create.json'}")
    rc, txt = cli("instance", "snapshot", "list", "--instance-id", instance_id)
    (out / "api_snapshots_after.json").write_text(txt + "\n")
    rc, txt = cli("instance", "pause", instance_id)
    (out / "api_pause.json").write_text(txt + "\n")
    log["pause_rc"] = rc
    deadline = time.time() + 1800
    status = "?"
    while time.time() < deadline:
        _, txt = cli("instance", "get", instance_id)
        try:
            status = json.loads(txt).get("status", "?")
        except (json.JSONDecodeError, AttributeError):
            status = "?"
        if status == "paused":
            break
        time.sleep(20)
    (out / "api_instance_final.json").write_text(txt + "\n")
    log["final_status"] = status
    log["seconds_total"] = round(time.time() - t0, 1)
    return log


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shutdown", action="store_true", help="after archiving: snapshot (await), then pause")
    ap.add_argument("--skip-db", action="store_true")
    a = ap.parse_args()
    instance_id = os.environ["AURA_INSTANCEID"]
    out = ROOT / "results/aura_instance" / dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    out.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {"at": dt.datetime.now(dt.UTC).isoformat(), "aura_cli": shutil.which(AURA_CLI) or AURA_CLI}
    if not a.skip_db:
        manifest["db"] = db_records(out)
    cred = has_credential()
    manifest["api"] = api_records(out, instance_id) if cred else "skipped: no aura-cli credential"
    if a.shutdown:
        if not cred:
            raise SystemExit("--shutdown needs an aura-cli credential (add it with `aura-cli credential add`)")
        manifest["shutdown"] = shutdown(out, instance_id)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1))
    print(json.dumps(manifest, indent=1))
    print("wrote", out)


if __name__ == "__main__":
    main()
