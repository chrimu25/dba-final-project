"""Part 1 - HBase query examples (Python / happybase), each timed. Shell equivalents: hbase/queries.hbase

Q1  latest 5 sessions of one user            prefix scan (newest first thanks to reversed timestamp)
Q2  one user's sessions in a date range      start/stop row scan
Q3  session summary only (family 's')         column-family projection: page views are not read
Q4  one product's daily metrics for a month   start/stop row scan on product_metrics
Q5  converted sessions, no row key help      full-table scan with a filter (shown for contrast)
"""
import json
import os
import time
from datetime import datetime, timezone

import happybase

MAX_TS = 9_999_999_999
conn = happybase.Connection(os.getenv("HBASE_HOST", "localhost"), port=9090, timeout=600_000)
S, M = conn.table("user_sessions"), conn.table("product_metrics")
results = {}


def rev(text):
    return f"{MAX_TS - int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp()):010d}"


def timed(name, fn):
    t = time.perf_counter()
    rows = fn()
    ms = (time.perf_counter() - t) * 1000
    results[name] = {"ms": round(ms, 2), "rows": len(rows)}
    print(f"\n=== {name}: {len(rows)} rows in {ms:.1f} ms ===")
    return rows


def show(rows, cols=(b"s:start_time", b"s:duration_seconds", b"s:device_type", b"s:conversion_status",
                     b"s:page_view_count")):
    for k, d in rows[:10]:
        print(" ", k.decode(), "|", " | ".join(d.get(c, b"").decode() for c in cols))


# pick a user that exists in the sample
user = next(S.scan(columns=[b"s:user_id"], limit=1))[1][b"s:user_id"].decode()
print("example user:", user)

rows = timed("Q1 latest 5 sessions (prefix scan)",
             lambda: list(S.scan(row_prefix=f"{user}#".encode(), columns=[b"s"], limit=5)))
show(rows)

# Q2: newest bound gives the START row, oldest bound the STOP row (keys are reversed)
lo, hi = "2026-09-01T00:00:00", "2026-10-01T00:00:00"
rows = timed("Q2 sessions in September (start/stop row)",
             lambda: list(S.scan(row_start=f"{user}#{rev(hi)}".encode(), row_stop=f"{user}#{rev(lo)}~".encode(),
                                 columns=[b"s"])))
show(rows)

rows = timed("Q3 full rows incl. page views and cart (all families) for comparison",
             lambda: list(S.scan(row_prefix=f"{user}#".encode(), limit=5)))
print("  cells per row with all families:", [len(d) for _, d in rows],
      "| first page view:", rows[0][1].get(b"v:p000", b"").decode() if rows else None)

prod = next(M.scan(limit=1))[0].decode().split("#")[0]
rows = timed(f"Q4 {prod} daily metrics, September (start/stop row)",
             lambda: list(M.scan(row_start=f"{prod}#20260901".encode(), row_stop=f"{prod}#20261001".encode())))
for k, d in rows[:10]:
    print(" ", k.decode(), {c.decode()[2:]: int(v) for c, v in d.items()})

flt = "SingleColumnValueFilter('s', 'conversion_status', =, 'binary:converted', true, true)"
rows = timed("Q5 converted sessions of ALL users (full scan + filter, contrast)",
             lambda: list(S.scan(filter=flt, columns=[b"s:conversion_status"])))

os.makedirs("results/hbase", exist_ok=True)
with open("results/hbase/query_timings.json", "w") as f:
    json.dump({"user": user, "product": prod, "queries": results}, f, indent=2)
print("\nsaved results/hbase/query_timings.json")
conn.close()
