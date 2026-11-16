"""Result-set equivalence between a gold result and a predicted result.

Both engines return rows as tuples. Comparison is order-insensitive (multiset)
unless `ordered=True`, ignores column names, and normalizes values:

- numbers -> float rounded to 6 significant digits (covers the 1e-4 relative
  tolerance at the magnitudes involved; Decimal and int compare equal to float),
- strings -> stripped, case-folded,
- dates/datetimes (Python or Neo4j temporal types) -> ISO date string,
- lists (Cypher collect()) -> sorted tuple of normalized items.

Two verdicts:
- strict: same multiset of rows with the same arity,
- lenient: every gold column is matched by a distinct predicted column with the
  same multiset of values, and the row count is equal -- the predicted query
  may carry extra columns (ids, labels) without being wrong.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

Row = Sequence[Any]


def norm_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, Decimal)):
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return str(f)
        if f == 0:
            return 0.0
        return float(f"{f:.6g}")
    if isinstance(v, (datetime, date)):
        return (v.date() if isinstance(v, datetime) else v).isoformat()
    if hasattr(v, "to_native"):  # neo4j.time.Date / DateTime
        return norm_value(v.to_native())
    if isinstance(v, str):
        return v.strip().casefold()
    if isinstance(v, (list, tuple)):
        return tuple(sorted((norm_value(x) for x in v), key=repr))
    return str(v).strip().casefold()


def norm_rows(rows: Sequence[Row]) -> list[tuple]:
    return [tuple(norm_value(v) for v in r) for r in rows]


def strict_equal(gold: Sequence[Row], pred: Sequence[Row], ordered: bool = False) -> bool:
    g, p = norm_rows(gold), norm_rows(pred)
    if ordered:
        return g == p
    return Counter(g) == Counter(p)


def lenient_equal(gold: Sequence[Row], pred: Sequence[Row], ordered: bool = False) -> bool:
    if strict_equal(gold, pred, ordered):
        return True
    g, p = norm_rows(gold), norm_rows(pred)
    if len(g) != len(p):
        return False
    if not g:
        return True
    gcols = list(zip(*g)) if g[0] else []
    pcols = list(zip(*p)) if p[0] else []
    if len(pcols) < len(gcols):
        return False
    # Greedy column matching by value multiset, then confirm row-wise on the chosen projection.
    used: set[int] = set()
    chosen: list[int] = []
    for gc in gcols:
        target = Counter(gc)
        match = next((j for j, pc in enumerate(pcols) if j not in used and Counter(pc) == target), None)
        if match is None:
            return False
        used.add(match)
        chosen.append(match)
    proj = [tuple(r[j] for j in chosen) for r in p]
    return proj == g if ordered else Counter(proj) == Counter(g)


def compare(gold: Sequence[Row], pred: Sequence[Row] | None, ordered: bool = False) -> dict[str, bool]:
    if pred is None:
        return {"strict": False, "lenient": False}
    return {"strict": strict_equal(gold, pred, ordered), "lenient": lenient_equal(gold, pred, ordered)}
