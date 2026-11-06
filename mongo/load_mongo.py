"""Part 1 - MongoDB: load categories, products, users and transactions (full dataset).

Design (see report):
  categories    _id = category_id, subcategories embedded (bounded: 3-5)
  products      _id = product_id, category {id, name} copied in (extended reference), price_history embedded
  users         _id = user_id, purchase_summary added later by pipelines.py (computed pattern)
  transactions  _id = transaction_id, items embedded with category_id (extended reference),
                channel = 'session' | 'direct' (generator makes two kinds of transaction)
ISO text timestamps are converted to BSON dates (stored as UTC; the generator gives no time zone).

Usage:  python mongo/load_mongo.py          (DATA_DIR defaults to ./data)
"""
import json
import os
import time
from datetime import datetime

from pymongo import ASCENDING, DESCENDING, MongoClient

DATA = os.getenv("DATA_DIR", "data")
BATCH = 10_000

client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
db = client["ecommerce"]


def dt(text):
    """ISO text -> datetime (None stays None)."""
    return datetime.fromisoformat(text) if text else None


def read(name):
    with open(os.path.join(DATA, name)) as f:
        return json.load(f)


def insert(coll, docs):
    """Insert in batches; unordered so one bad document does not stop the rest."""
    for i in range(0, len(docs), BATCH):
        coll.insert_many(docs[i:i + BATCH], ordered=False)


TRANSACTION_SCHEMA = {   # MongoDB schema validation: rejects malformed transactions at write time
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["user_id", "timestamp", "items", "total", "channel"],
        "properties": {
            "user_id": {"bsonType": "string"},
            "timestamp": {"bsonType": "date"},
            "total": {"bsonType": ["double", "int"], "minimum": 0},
            "channel": {"enum": ["session", "direct"]},
            "items": {"bsonType": "array", "minItems": 1,
                      "items": {"bsonType": "object",
                                "required": ["product_id", "quantity", "unit_price", "category_id"]}},
        },
    }
}


def main():
    start = time.perf_counter()
    for name in ("categories", "products", "users", "transactions"):
        db.drop_collection(name)
    db.create_collection("transactions", validator=TRANSACTION_SCHEMA)
    timings = {}

    # categories ------------------------------------------------------------
    t = time.perf_counter()
    cats = read("categories.json")
    cat_name = {c["category_id"]: c["name"] for c in cats}
    insert(db.categories, [{"_id": c["category_id"], "name": c["name"], "subcategories": c["subcategories"]}
                           for c in cats])
    timings["categories"] = time.perf_counter() - t

    # products --------------------------------------------------------------
    t = time.perf_counter()
    prods = read("products.json")
    prod_cat = {p["product_id"]: p["category_id"] for p in prods}
    insert(db.products, [{
        "_id": p["product_id"],
        "name": p["name"],
        "category": {"id": p["category_id"], "name": cat_name.get(p["category_id"])},
        "base_price": p["base_price"],
        "current_stock": p["current_stock"],
        "is_active": p["is_active"],
        "price_history": [{"price": h["price"], "date": dt(h["date"])} for h in p["price_history"]],
        "creation_date": dt(p["creation_date"]),
    } for p in prods])
    timings["products"] = time.perf_counter() - t

    # users -----------------------------------------------------------------
    t = time.perf_counter()
    insert(db.users, [{
        "_id": u["user_id"],
        "geo_data": u["geo_data"],
        "registration_date": dt(u["registration_date"]),
        "last_active": dt(u["last_active"]),
    } for u in read("users.json")])
    timings["users"] = time.perf_counter() - t

    # transactions ------------------------------------------------------------
    t = time.perf_counter()
    txns = read("transactions.json")
    docs = []
    for x in txns:
        docs.append({
            "_id": x["transaction_id"],
            "session_id": x["session_id"],
            "channel": "session" if x["session_id"] else "direct",
            "user_id": x["user_id"],
            "timestamp": dt(x["timestamp"]),
            "items": [{**i, "category_id": prod_cat.get(i["product_id"])} for i in x["items"]],
            "subtotal": round(x["subtotal"], 2),   # generator sums floats without rounding
            "discount": x["discount"],
            "total": x["total"],
            "payment_method": x["payment_method"],
            "status": x["status"],
        })
    del txns
    insert(db.transactions, docs)
    timings["transactions"] = time.perf_counter() - t

    # indexes (chosen for the pipelines and the later integration queries) ------
    t = time.perf_counter()
    db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])  # one user's history, newest first
    db.transactions.create_index([("timestamp", ASCENDING)])                            # date-range revenue
    db.transactions.create_index([("items.product_id", ASCENDING)])                     # multikey: orders containing a product
    db.products.create_index([("category.id", ASCENDING)])
    timings["indexes"] = time.perf_counter() - t

    counts = {n: db[n].count_documents({}) for n in ("categories", "products", "users", "transactions")}
    stats = {n: db.command("collStats", n) for n in counts}
    print(f"{'collection':<14}{'documents':>12}{'data MB':>10}{'storage MB':>12}{'index MB':>10}{'load s':>9}")
    for n in counts:
        s = stats[n]
        print(f"{n:<14}{counts[n]:>12,}{s['size'] / 1e6:>10.2f}{s['storageSize'] / 1e6:>12.2f}"
              f"{s['totalIndexSize'] / 1e6:>10.2f}{timings[n]:>9.2f}")
    print(f"index build {timings['indexes']:.2f} s | total {time.perf_counter() - start:.2f} s")
    print("channels:", {d["_id"]: d["n"] for d in db.transactions.aggregate(
        [{"$group": {"_id": "$channel", "n": {"$sum": 1}}}])})

    os.makedirs("results/mongo", exist_ok=True)
    with open("results/mongo/load_summary.json", "w") as f:
        json.dump({"counts": counts, "seconds": {k: round(v, 2) for k, v in timings.items()},
                   "storage_mb": {n: round(stats[n]["storageSize"] / 1e6, 2) for n in counts},
                   "index_mb": {n: round(stats[n]["totalIndexSize"] / 1e6, 2) for n in counts}}, f, indent=2)


if __name__ == "__main__":
    main()
