"""Part 1 - HBase: load a representative subset of sessions + daily product metrics (via Thrift / happybase).

Sampling: a session is kept when crc32(session_id) % SAMPLE_MOD == 0 (default 5 -> about 20 %), across ALL
20 files. Taking the first files instead would be biased: sessions are written in generation order and late
sessions never get transactions (the generator's transaction cap).

Row keys
  user_sessions    <user_id>#<9999999999 - start_epoch_s (10 digits)>#<session_id>   newest session first per user
  product_metrics  <product_id>#<yyyymmdd>                                            days in order per product
Usage:  python hbase/load_hbase.py
"""
import glob
import json
import os
import time
import zlib
from collections import defaultdict
from datetime import datetime, timezone

import happybase

DATA = os.getenv("DATA_DIR", "data")
SAMPLE_MOD = int(os.getenv("SAMPLE_MOD", "5"))
MAX_TS = 9_999_999_999


def epoch(text):
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp())


def row_key(user_id, start_text, session_id):
    return f"{user_id}#{MAX_TS - epoch(start_text):010d}#{session_id}".encode()


def b(value):
    return str(value).encode()


def main():
    conn = happybase.Connection(os.getenv("HBASE_HOST", "localhost"), port=9090, timeout=600_000)
    sessions_t = conn.table("user_sessions")
    metrics_t = conn.table("product_metrics")
    metrics = defaultdict(lambda: defaultdict(int))     # (product, yyyymmdd) -> counters
    kept = seen = cells = 0
    status = defaultdict(int)
    start = time.perf_counter()

    files = sorted(glob.glob(os.path.join(DATA, "sessions_*.json")), key=lambda p: int(p.split("_")[-1].split(".")[0]))
    for path in files:
        with open(path) as f:
            batch_sessions = json.load(f)
        with sessions_t.batch(batch_size=2000) as batch:
            for s in batch_sessions:
                seen += 1
                if zlib.crc32(s["session_id"].encode()) % SAMPLE_MOD:
                    continue
                kept += 1
                status[s["conversion_status"]] += 1
                geo, dev = s["geo_data"], s["device_profile"]
                data = {
                    b"s:user_id": b(s["user_id"]), b"s:session_id": b(s["session_id"]),
                    b"s:start_time": b(s["start_time"]), b"s:end_time": b(s["end_time"]),
                    b"s:duration_seconds": b(s["duration_seconds"]),
                    b"s:device_type": b(dev["type"]), b"s:os": b(dev["os"]), b"s:browser": b(dev["browser"]),
                    b"s:city": b(geo.get("city")), b"s:state": b(geo.get("state")), b"s:country": b(geo.get("country")),
                    b"s:referrer": b(s["referrer"]), b"s:conversion_status": b(s["conversion_status"]),
                    b"s:page_view_count": b(len(s["page_views"])), b"s:cart_item_count": b(len(s["cart_contents"])),
                }
                for i, pv in enumerate(s["page_views"]):
                    data[f"v:p{i:03d}".encode()] = json.dumps(pv, separators=(",", ":")).encode()
                    if pv["product_id"]:
                        m = metrics[(pv["product_id"], pv["timestamp"][:10].replace("-", ""))]
                        m["views"] += 1
                        m["view_seconds"] += pv["view_duration"]
                for pid, item in s["cart_contents"].items():
                    data[f"c:{pid}".encode()] = b(item["quantity"])
                    metrics[(pid, s["start_time"][:10].replace("-", ""))]["cart_adds"] += item["quantity"]
                for pid in s["viewed_products"]:
                    metrics[(pid, s["start_time"][:10].replace("-", ""))]["sessions"] += 1
                cells += len(data)
                batch.put(row_key(s["user_id"], s["start_time"], s["session_id"]), data)
        del batch_sessions
        print(f"{os.path.basename(path):<18} seen {seen:>9,}  kept {kept:>8,}  {time.perf_counter() - start:7.1f} s")
    t_sessions = time.perf_counter() - start

    t0 = time.perf_counter()
    with metrics_t.batch(batch_size=5000) as batch:
        for (pid, day), m in metrics.items():
            batch.put(f"{pid}#{day}".encode(), {f"m:{k}".encode(): b(v) for k, v in m.items()})
    t_metrics = time.perf_counter() - t0

    summary = {"sessions_seen": seen, "sessions_loaded": kept, "sample_share": round(kept / seen, 4),
               "cells_written": cells, "status_in_sample": dict(status), "product_metric_rows": len(metrics),
               "load_seconds_sessions": round(t_sessions, 1), "load_seconds_metrics": round(t_metrics, 1),
               "rows_per_second": round(kept / t_sessions)}
    print(json.dumps(summary, indent=2))
    os.makedirs("results/hbase", exist_ok=True)
    with open("results/hbase/load_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    conn.close()


if __name__ == "__main__":
    main()
