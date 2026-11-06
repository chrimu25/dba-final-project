"""Part 3 - Integrated query: conversion funnel across HBase (sessions) + MongoDB (purchases), joined in Spark.

Business question: where do shoppers drop off between viewing a product, adding it to the cart and paying,
and how much does the 'converted' session flag overstate real purchases?

  HBase   user_sessions sample (20 %): s = summary, v = page views (JSON), c = cart (product -> quantity)
  MongoDB transactions with channel='session' (filter + projection pushed down to MongoDB)
  Spark   joins on session_id / (session_id, product_id), computes funnels, checks cross-system consistency
  MongoDB product_funnel <- per-product funnel written back for serving (dashboards / APIs)
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))
import happybase                                   # noqa: E402
from pymongo import MongoClient, UpdateOne         # noqa: E402
from pyspark.sql import functions as F             # noqa: E402

from common import Timer, spark_session            # noqa: E402

OUT = "results/integration"


def read_hbase():
    """Scan all three families; return session, product-view and cart rows."""
    conn = happybase.Connection(os.getenv("HBASE_HOST", "localhost"), port=9090, timeout=600_000)
    sessions, views, carts = [], [], []
    for _, d in conn.table("user_sessions").scan(batch_size=2000):
        sid = d[b"s:session_id"].decode()
        pages = [json.loads(v) for k, v in d.items() if k.startswith(b"v:")]
        types = {p["page_type"] for p in pages}
        sessions.append((sid, d[b"s:user_id"].decode(), d[b"s:device_type"].decode(), d[b"s:referrer"].decode(),
                         d[b"s:conversion_status"].decode(), d[b"s:start_time"].decode()[:10],
                         "checkout" in types, "confirmation" in types))
        views.extend((sid, p["product_id"]) for p in pages if p["page_type"] == "product_detail" and p["product_id"])
        carts.extend((sid, k[2:].decode(), int(v)) for k, v in d.items() if k.startswith(b"c:"))
    conn.close()
    return sessions, views, carts


def read_mongo():
    """Session-linked transactions only; MongoDB does the filtering and projection."""
    db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))["ecommerce"]
    tx, items = [], []
    for t in db.transactions.find({"channel": "session"},
                                  {"session_id": 1, "total": 1, "items.product_id": 1, "items.quantity": 1}):
        tx.append((t["session_id"], t["_id"], float(t["total"])))
        items.extend((t["session_id"], i["product_id"], int(i["quantity"])) for i in t["items"])
    return tx, items


def main():
    spark = spark_session("ecommerce-funnel")
    timer = Timer()
    os.makedirs(OUT, exist_ok=True)

    sess_rows, view_rows, cart_rows = timer.run("read HBase (3 families, ~400k rows)", read_hbase)
    tx_rows, item_rows = timer.run("read MongoDB session transactions", read_mongo)
    print(f"  HBase: {len(sess_rows):,} sessions, {len(view_rows):,} product views, {len(cart_rows):,} cart lines")
    print(f"  MongoDB: {len(tx_rows):,} session-linked transactions, {len(item_rows):,} items")

    S = spark.createDataFrame(sess_rows, "session_id string, user_id string, device_type string, referrer string, "
                                         "conversion_status string, session_date string, reached_checkout boolean, "
                                         "reached_confirmation boolean").cache()
    V = spark.createDataFrame(view_rows, "session_id string, product_id string").distinct().cache()
    C = spark.createDataFrame(cart_rows, "session_id string, product_id string, cart_qty int").cache()
    T_all = spark.createDataFrame(tx_rows, "session_id string, transaction_id string, total double")
    I_all = spark.createDataFrame(item_rows, "session_id string, product_id string, qty int")
    # keep only purchases of sessions that are in the HBase sample (semi join = filter by existence)
    T = T_all.join(S.select("session_id"), "session_id", "left_semi").cache()
    I = I_all.join(S.select("session_id"), "session_id", "left_semi").cache()

    flag = lambda df, name: df.select("session_id").distinct().withColumn(name, F.lit(True))
    sess = (S.join(flag(V, "viewed_product"), "session_id", "left")
             .join(flag(C, "added_to_cart"), "session_id", "left")
             .join(flag(T, "purchased"), "session_id", "left")
             .fillna(False, ["viewed_product", "added_to_cart", "purchased"])
             .withColumn("flag_converted", F.col("conversion_status") == "converted")).cache()

    # ---------- session funnel ----------
    def funnel(df, by=None):
        aggs = [F.count("*").alias("sessions"),
                F.sum(F.col("viewed_product").cast("int")).alias("viewed_product"),
                F.sum(F.col("added_to_cart").cast("int")).alias("added_to_cart"),
                F.sum(F.col("reached_checkout").cast("int")).alias("reached_checkout"),
                F.sum(F.col("flag_converted").cast("int")).alias("flagged_converted"),
                F.sum(F.col("purchased").cast("int")).alias("purchased")]
        return (df.groupBy(*by).agg(*aggs) if by else df.agg(*aggs))

    overall = timer.run("session funnel", lambda: funnel(sess).toPandas())
    stages = ["sessions", "viewed_product", "added_to_cart", "reached_checkout", "flagged_converted", "purchased"]
    rows = []
    for i, st in enumerate(stages):
        n = int(overall[st][0])
        prev = int(overall[stages[i - 1]][0]) if i else n
        rows.append({"stage": st, "sessions": n, "pct_of_all": round(100 * n / int(overall["sessions"][0]), 2),
                     "pct_of_previous": round(100 * n / prev, 2) if prev else None})
    import pandas as pd
    session_funnel = pd.DataFrame(rows)
    session_funnel.to_csv(f"{OUT}/session_funnel.csv", index=False)
    print("\n=== Session funnel (HBase sample + MongoDB purchases) ===")
    print(session_funnel.to_string(index=False))

    by_device = funnel(sess, ["device_type"]).toPandas()
    for st in stages[1:]:
        by_device[f"{st}_pct"] = (100 * by_device[st] / by_device["sessions"]).round(2)
    by_device.to_csv(f"{OUT}/funnel_by_device.csv", index=False)
    print("\n=== Funnel by device (% of sessions) ===")
    print(by_device[["device_type", "sessions"] + [f"{s}_pct" for s in stages[1:]]].to_string(index=False))

    # why flagged-converted sessions have no purchase (cross-system reconciliation)
    gap = (sess.where("flag_converted AND NOT purchased")
           .agg(F.count("*").alias("flagged_but_no_purchase"),
                F.sum(F.col("reached_checkout").cast("int")).alias("of_which_reached_checkout")).first())
    print(f"\nFlagged 'converted' but no transaction in MongoDB: {gap[0]:,} sessions "
          f"(the generator's transaction cap / stock failures)")

    # ---------- product funnel: view -> cart -> purchase, same session ----------
    pv = (V.withColumn("viewed", F.lit(1))
            .join(C.select("session_id", "product_id", F.lit(1).alias("carted")), ["session_id", "product_id"], "full")
            .join(I.groupBy("session_id", "product_id").agg(F.sum("qty").alias("bought_qty"))
                  .withColumn("bought", F.lit(1)), ["session_id", "product_id"], "full")
            .fillna(0, ["viewed", "carted", "bought", "bought_qty"])).cache()
    consistency = pv.agg(F.sum(F.when((F.col("bought") == 1) & (F.col("carted") == 0), 1).otherwise(0)).alias("bought_not_in_cart"),
                         F.sum(F.when((F.col("carted") == 1) & (F.col("viewed") == 0), 1).otherwise(0)).alias("carted_not_viewed"),
                         F.sum("bought").alias("bought_lines")).first()
    print(f"\nConsistency HBase vs MongoDB: purchased lines {consistency['bought_lines']:,} | "
          f"purchased but not in the session's cart: {consistency['bought_not_in_cart']:,} | "
          f"carted but never viewed: {consistency['carted_not_viewed']:,}")

    product = (pv.groupBy("product_id")
               .agg(F.sum("viewed").alias("view_sessions"), F.sum("carted").alias("cart_sessions"),
                    F.sum("bought").alias("purchase_sessions"), F.sum("bought_qty").alias("units"))
               # try_divide returns NULL instead of failing when a product was never viewed / carted
               # (Spark 4 runs in ANSI mode by default, where x / 0 is an error, not NULL)
               .withColumn("view_to_cart_pct", F.round(100 * F.try_divide("cart_sessions", "view_sessions"), 2))
               .withColumn("cart_to_purchase_pct", F.round(100 * F.try_divide("purchase_sessions", "cart_sessions"), 2))
               .withColumn("view_to_purchase_pct", F.round(100 * F.try_divide("purchase_sessions", "view_sessions"), 2)))
    pdf = timer.run("product funnel", lambda: product.toPandas())
    pdf.to_csv(f"{OUT}/product_funnel.csv", index=False)
    tot = pdf[["view_sessions", "cart_sessions", "purchase_sessions"]].sum()
    print(f"\nProduct level: {int(tot.view_sessions):,} views -> {int(tot.cart_sessions):,} carts "
          f"({100 * tot.cart_sessions / tot.view_sessions:.1f}%) -> {int(tot.purchase_sessions):,} purchases "
          f"({100 * tot.purchase_sessions / tot.cart_sessions:.1f}% of carts)")
    q = pdf[pdf.view_sessions >= 300]
    print(f"Spread of view->purchase rate (products with >=300 view sessions, n={len(q)}): "
          f"p5 {q.view_to_purchase_pct.quantile(.05):.1f}% | median {q.view_to_purchase_pct.median():.1f}% | "
          f"p95 {q.view_to_purchase_pct.quantile(.95):.1f}%")

    # ---------- write back to MongoDB (serving layer) ----------
    def write_back():
        db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))["ecommerce"]
        ops = [UpdateOne({"_id": r.product_id}, {"$set": {k: (None if pd.isna(v) else (float(v) if isinstance(v, float) else int(v)))
                                                          for k, v in r._asdict().items() if k not in ("Index", "product_id")}},
                         upsert=True) for r in pdf.itertuples()]
        db.product_funnel.bulk_write(ops, ordered=False)
        return db.product_funnel.count_documents({})
    n = timer.run("write product_funnel to MongoDB", write_back)
    print(f"  MongoDB product_funnel documents: {n:,}")

    with open(f"{OUT}/summary.json", "w") as f:
        json.dump({"session_funnel": rows, "flagged_but_no_purchase": gap[0], "of_which_reached_checkout": gap[1],
                   "consistency": consistency.asDict(), "timings": timer.steps}, f, indent=2)
    timer.save(f"{OUT}/timings.json")
    spark.stop()


if __name__ == "__main__":
    main()
