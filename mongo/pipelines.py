"""Part 1 - MongoDB aggregation pipelines (all printed, timed and saved to results/mongo/).

P1  Revenue by category and month  ($unwind, $group, $facet, $lookup) - discount spread over items
P2  Customer summary + segments    ($group, $dateDiff, $merge into users = computed pattern; then $bucketAuto)
P3  Product popularity and stock   ($unwind, $group, $lookup) - top sellers that are running out
"""
import json
import os
import time

from pymongo import MongoClient

db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))["ecommerce"]
OUT = "results/mongo"

# Line revenue after the order-level discount: item subtotal x (order total / order subtotal)
NET_RATIO = {"$cond": [{"$gt": ["$subtotal", 0]}, {"$divide": ["$total", "$subtotal"]}, 0]}

P1 = [
    {"$project": {"month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
                  "ratio": NET_RATIO, "items": 1}},
    {"$unwind": "$items"},
    {"$group": {"_id": {"category": "$items.category_id", "month": "$month"},
                "gross": {"$sum": "$items.subtotal"},
                "net": {"$sum": {"$multiply": ["$items.subtotal", "$ratio"]}},
                "units": {"$sum": "$items.quantity"},
                "lines": {"$sum": 1}}},
    {"$facet": {
        "by_category": [
            {"$group": {"_id": "$_id.category", "net": {"$sum": "$net"}, "gross": {"$sum": "$gross"},
                        "units": {"$sum": "$units"}}},
            {"$lookup": {"from": "categories", "localField": "_id", "foreignField": "_id", "as": "c"}},
            {"$set": {"name": {"$first": "$c.name"}, "net": {"$round": ["$net", 2]},
                      "gross": {"$round": ["$gross", 2]}}},
            {"$project": {"c": 0}},
            {"$sort": {"net": -1}}],
        "by_month": [
            {"$group": {"_id": "$_id.month", "net": {"$sum": "$net"}, "units": {"$sum": "$units"}}},
            {"$set": {"net": {"$round": ["$net", 2]}}},
            {"$sort": {"_id": 1}}],
        "category_month": [
            {"$set": {"net": {"$round": ["$net", 2]}}},
            {"$sort": {"_id.category": 1, "_id.month": 1}}],
    }},
]


def p2_summary(ref_date):
    return [
        {"$group": {"_id": "$user_id", "orders": {"$sum": 1}, "spent": {"$sum": "$total"},
                    "first_purchase": {"$min": "$timestamp"}, "last_purchase": {"$max": "$timestamp"},
                    "session_orders": {"$sum": {"$cond": [{"$eq": ["$channel", "session"]}, 1, 0]}}}},
        {"$set": {"spent": {"$round": ["$spent", 2]},
                  "avg_order_value": {"$round": [{"$divide": ["$spent", "$orders"]}, 2]},
                  "recency_days": {"$dateDiff": {"startDate": "$last_purchase", "endDate": ref_date, "unit": "day"}}}},
        {"$merge": {"into": "users", "on": "_id",
                    "whenMatched": [{"$set": {"purchase_summary": "$$new"}}, {"$unset": "purchase_summary._id"}],
                    "whenNotMatched": "discard"}},
    ]


P2_SEGMENTS = [
    {"$match": {"purchase_summary": {"$exists": True}}},
    {"$bucketAuto": {"groupBy": "$purchase_summary.spent", "buckets": 5,
                     "output": {"users": {"$sum": 1},
                                "avg_spent": {"$avg": "$purchase_summary.spent"},
                                "avg_orders": {"$avg": "$purchase_summary.orders"},
                                "avg_order_value": {"$avg": "$purchase_summary.avg_order_value"},
                                "avg_recency_days": {"$avg": "$purchase_summary.recency_days"}}}},
]

P3 = [
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id", "units": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"}, "orders": {"$sum": 1}}},
    {"$sort": {"units": -1}},
    {"$limit": 15},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "p"}},
    {"$set": {"name": {"$first": "$p.name"}, "category": {"$first": "$p.category.name"},
              "current_stock": {"$first": "$p.current_stock"}, "is_active": {"$first": "$p.is_active"},
              "revenue": {"$round": ["$revenue", 2]}}},
    {"$project": {"p": 0}},
]


def run(name, coll, pipeline):
    t = time.perf_counter()
    rows = list(db[coll].aggregate(pipeline, allowDiskUse=True))
    secs = time.perf_counter() - t
    with open(f"{OUT}/{name}.json", "w") as f:
        json.dump({"pipeline": pipeline, "seconds": round(secs, 3), "result": rows}, f, indent=1, default=str)
    print(f"\n=== {name} ({secs:.2f} s) ===")
    return rows


def main():
    os.makedirs(OUT, exist_ok=True)

    r = run("p1_revenue_by_category_month", "transactions", P1)[0]
    print(f"{'category':<10}{'name':<34}{'net revenue':>16}{'units':>10}")
    for c in r["by_category"]:
        print(f"{c['_id']:<10}{str(c['name'])[:32]:<34}{c['net']:>16,.2f}{c['units']:>10,}")
    print("by month:", [(m["_id"], f"{m['net']:,.0f}") for m in r["by_month"]])

    ref = db.transactions.find_one(sort=[("timestamp", -1)])["timestamp"]   # end of the data, not 'now'
    run("p2a_user_summary_merge", "transactions", p2_summary(ref))
    print(f"purchase_summary written to {db.users.count_documents({'purchase_summary': {'$exists': True}}):,} users "
          f"(reference date {ref:%Y-%m-%d %H:%M})")
    print("example:", db.users.find_one({"purchase_summary": {"$exists": True}}, {"purchase_summary": 1}))
    seg = run("p2b_spend_segments", "users", P2_SEGMENTS)
    print(f"{'spend range':<26}{'users':>7}{'avg spent':>12}{'avg orders':>12}{'avg AOV':>10}{'recency d':>11}")
    for s in seg:
        rng = f"{s['_id']['min']:,.0f} - {s['_id']['max']:,.0f}"
        print(f"{rng:<26}{s['users']:>7}{s['avg_spent']:>12,.0f}{s['avg_orders']:>12.1f}"
              f"{s['avg_order_value']:>10,.0f}{s['avg_recency_days']:>11.1f}")

    top = run("p3_top_products_stock", "transactions", P3)
    print(f"{'product':<12}{'name':<36}{'units':>7}{'revenue':>14}{'stock':>7}  active")
    for p in top:
        print(f"{p['_id']:<12}{str(p['name'])[:34]:<36}{p['units']:>7}{p['revenue']:>14,.2f}"
              f"{p['current_stock']:>7}  {p['is_active']}")


if __name__ == "__main__":
    main()
