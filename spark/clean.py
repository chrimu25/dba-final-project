"""Part 2.1 - Clean and normalise the raw JSON into Parquet tables (lake/).

Raw files are JSON *arrays* (not JSON Lines), so multiLine=True is required; Spark cannot split such a file,
so each file becomes one task. Explicit schemas avoid a costly schema-inference pass over 3.7 GB and fix types.

Output tables
  users, products, categories, subcategories          small dimension tables
  transactions, transaction_items                      facts (items exploded, net line revenue after discount)
  sessions, page_views, cart_items                     facts (nested arrays/maps exploded), partitioned by month
Data-quality checks are printed and saved to results/spark/quality.json.
"""
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import LAKE, OUT, RAW, Timer, spark_session

S = T.StringType(); D = T.DoubleType(); I = T.IntegerType(); L = T.LongType(); B = T.BooleanType()
GEO = T.StructType([T.StructField("city", S), T.StructField("state", S), T.StructField("country", S),
                    T.StructField("ip_address", S)])
USERS = T.StructType([T.StructField("user_id", S), T.StructField("geo_data", GEO),
                      T.StructField("registration_date", S), T.StructField("last_active", S)])
CATS = T.StructType([T.StructField("category_id", S), T.StructField("name", S),
                     T.StructField("subcategories", T.ArrayType(T.StructType([
                         T.StructField("subcategory_id", S), T.StructField("name", S),
                         T.StructField("profit_margin", D)])))])
PRODS = T.StructType([T.StructField("product_id", S), T.StructField("name", S), T.StructField("category_id", S),
                      T.StructField("base_price", D), T.StructField("current_stock", I),
                      T.StructField("is_active", B),
                      T.StructField("price_history", T.ArrayType(T.StructType([
                          T.StructField("price", D), T.StructField("date", S)]))),
                      T.StructField("creation_date", S)])
ITEM = T.StructType([T.StructField("product_id", S), T.StructField("quantity", I),
                     T.StructField("unit_price", D), T.StructField("subtotal", D)])
TXNS = T.StructType([T.StructField("transaction_id", S), T.StructField("session_id", S),
                     T.StructField("user_id", S), T.StructField("timestamp", S),
                     T.StructField("items", T.ArrayType(ITEM)), T.StructField("subtotal", D),
                     T.StructField("discount", D), T.StructField("total", D),
                     T.StructField("payment_method", S), T.StructField("status", S)])
PV = T.StructType([T.StructField("timestamp", S), T.StructField("page_type", S), T.StructField("product_id", S),
                   T.StructField("category_id", S), T.StructField("view_duration", I)])
SESS = T.StructType([T.StructField("session_id", S), T.StructField("user_id", S),
                     T.StructField("start_time", S), T.StructField("end_time", S),
                     T.StructField("duration_seconds", I), T.StructField("geo_data", GEO),
                     T.StructField("device_profile", T.StructType([T.StructField("type", S), T.StructField("os", S),
                                                                   T.StructField("browser", S)])),
                     T.StructField("viewed_products", T.ArrayType(S)), T.StructField("page_views", T.ArrayType(PV)),
                     T.StructField("cart_contents", T.MapType(S, T.StructType([T.StructField("quantity", I),
                                                                               T.StructField("price", D)]))),
                     T.StructField("conversion_status", S), T.StructField("referrer", S)])


def clean_text(c):
    """Trim and lower-case categorical text; empty strings become null."""
    return F.when(F.trim(c) == "", None).otherwise(F.lower(F.trim(c)))


def main():
    spark = spark_session("ecommerce-clean")
    timer = Timer()
    read = lambda schema, path: spark.read.schema(schema).option("multiLine", True).json(f"{RAW}/{path}")
    q = {}

    # ---------------- dimensions ----------------
    cats = read(CATS, "categories.json")
    categories = cats.select("category_id", F.trim("name").alias("category_name")).dropDuplicates(["category_id"])
    subcats = (cats.select("category_id", F.explode("subcategories").alias("s"))
               .select("category_id", "s.subcategory_id", F.trim("s.name").alias("subcategory_name"),
                       F.col("s.profit_margin")))

    prods = read(PRODS, "products.json").dropDuplicates(["product_id"])
    products = (prods
                .join(F.broadcast(categories), "category_id", "left")
                .select("product_id", F.trim("name").alias("product_name"), "category_id", "category_name",
                        # the generator's 'base_price' is actually the LATEST price in price_history
                        F.col("base_price").alias("current_price"),
                        F.col("price_history")[0]["price"].alias("first_price"),
                        (F.size("price_history") - 1).alias("price_changes"),
                        "current_stock", "is_active",
                        F.to_timestamp("creation_date").alias("created_at")))

    users = (read(USERS, "users.json").dropDuplicates(["user_id"])
             .select("user_id", F.trim("geo_data.city").alias("city"), F.upper(F.trim("geo_data.state")).alias("state"),
                     F.upper(F.trim("geo_data.country")).alias("country"),
                     F.to_timestamp("registration_date").alias("registered_at"),
                     F.to_timestamp("last_active").alias("last_active_at"))
             .withColumn("registration_month", F.date_format("registered_at", "yyyy-MM")))

    # ---------------- transactions ----------------
    raw_tx = read(TXNS, "transactions.json")
    q["transactions_raw"] = raw_tx.count()
    tx = (raw_tx.dropDuplicates(["transaction_id"])
          .withColumn("ts", F.to_timestamp("timestamp"))
          .withColumn("channel", F.when(F.col("session_id").isNull(), "direct").otherwise("session"))
          .withColumn("items_sum", F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["subtotal"])))
    transactions = tx.select(
        "transaction_id", "session_id", "user_id", "ts", F.to_date("ts").alias("order_date"),
        F.date_format("ts", "yyyy-MM").alias("month"), "channel",
        F.size("items").alias("line_count"),
        F.round("subtotal", 2).alias("subtotal"), F.round("discount", 2).alias("discount"), F.round("total", 2).alias("total"),
        clean_text(F.col("payment_method")).alias("payment_method"), clean_text(F.col("status")).alias("status"),
        # quality flags: items must add up to the subtotal, and total = subtotal - discount
        (F.abs(F.col("items_sum") - F.col("subtotal")) > 0.01).alias("flag_items_mismatch"),
        (F.abs(F.col("subtotal") - F.col("discount") - F.col("total")) > 0.01).alias("flag_total_mismatch"))

    items = (tx.select("transaction_id", "user_id", "ts", "channel", "subtotal", "total", F.explode("items").alias("i"))
             .join(F.broadcast(products.select("product_id", "category_id")), F.col("i.product_id") == F.col("product_id"), "left")
             .select("transaction_id", "user_id", F.to_date("ts").alias("order_date"),
                     F.date_format("ts", "yyyy-MM").alias("month"), "channel",
                     F.col("i.product_id").alias("product_id"), "category_id",
                     F.col("i.quantity").alias("quantity"), F.round("i.unit_price", 2).alias("unit_price"),
                     F.round("i.subtotal", 2).alias("line_gross"),
                     # spread the order-level discount over its lines in proportion to their value
                     F.round(F.when(F.col("subtotal") > 0, F.col("i.subtotal") * F.col("total") / F.col("subtotal"))
                             .otherwise(0.0), 2).alias("line_net")))

    # ---------------- sessions ----------------
    raw_s = read(SESS, "sessions_*.json")
    s = (raw_s.dropDuplicates(["session_id"])
         .withColumn("start_ts", F.to_timestamp("start_time")).withColumn("end_ts", F.to_timestamp("end_time")))
    purchased = transactions.where("session_id IS NOT NULL").select("session_id").distinct().withColumn("has_transaction", F.lit(True))
    sessions = (s.join(purchased, "session_id", "left")
                .select("session_id", "user_id", "start_ts", "end_ts", "duration_seconds",
                        F.to_date("start_ts").alias("session_date"), F.date_format("start_ts", "yyyy-MM").alias("month"),
                        clean_text(F.col("device_profile.type")).alias("device_type"),
                        clean_text(F.col("device_profile.os")).alias("os"),
                        clean_text(F.col("device_profile.browser")).alias("browser"),
                        clean_text(F.col("referrer")).alias("referrer"),
                        clean_text(F.col("conversion_status")).alias("conversion_status"),
                        F.size("page_views").alias("page_view_count"),
                        F.size("viewed_products").alias("viewed_product_count"),
                        F.size(F.map_keys("cart_contents")).alias("cart_product_count"),
                        F.coalesce("has_transaction", F.lit(False)).alias("has_transaction"),
                        (F.abs(F.col("end_ts").cast("long") - F.col("start_ts").cast("long") - F.col("duration_seconds")) > 1)
                        .alias("flag_duration_mismatch")))

    page_views = (s.select("session_id", "user_id", F.date_format("start_ts", "yyyy-MM").alias("month"),
                           F.posexplode("page_views").alias("seq", "pv"))
                  .select("session_id", "user_id", "month", "seq", F.to_timestamp("pv.timestamp").alias("view_ts"),
                          clean_text(F.col("pv.page_type")).alias("page_type"),
                          F.col("pv.product_id").alias("product_id"), F.col("pv.category_id").alias("category_id"),
                          F.col("pv.view_duration").alias("view_seconds")))

    cart_items = (s.select("session_id", "user_id", F.date_format("start_ts", "yyyy-MM").alias("month"),
                           F.explode("cart_contents").alias("product_id", "c"))
                  .select("session_id", "user_id", "month", "product_id",
                          F.col("c.quantity").alias("quantity"), F.round("c.price", 2).alias("price")))

    # ---------------- write Parquet ----------------
    print("Writing Parquet tables:")
    w = lambda df, name, part=None: (df.write.mode("overwrite").partitionBy(*([part] if part else []))
                                     .parquet(f"{LAKE}/{name}"))
    for name, df in [("categories", categories), ("subcategories", subcats), ("products", products), ("users", users)]:
        timer.run(name, lambda df=df, name=name: w(df, name))
    timer.run("transactions", lambda: w(transactions, "transactions", "month"))
    timer.run("transaction_items", lambda: w(items, "transaction_items", "month"))
    timer.run("sessions (2M, reads 3.7 GB JSON)", lambda: w(sessions, "sessions", "month"))
    timer.run("page_views (exploded)", lambda: w(page_views, "page_views", "month"))
    timer.run("cart_items (exploded map)", lambda: w(cart_items, "cart_items", "month"))

    # ---------------- data-quality report (read back from Parquet: cheap) ----------------
    P = lambda n: spark.read.parquet(f"{LAKE}/{n}")
    T_, IT, SS, PVW, CI, U = P("transactions"), P("transaction_items"), P("sessions"), P("page_views"), P("cart_items"), P("users")
    q.update({
        "users": U.count(), "products": P("products").count(), "categories": P("categories").count(),
        "subcategories": P("subcategories").count(),
        "transactions": T_.count(), "transaction_items": IT.count(), "sessions": SS.count(),
        "page_views": PVW.count(), "cart_items": CI.count(),
        "tx_items_mismatch": T_.where("flag_items_mismatch").count(),
        "tx_total_mismatch": T_.where("flag_total_mismatch").count(),
        "tx_by_channel": {r[0]: r[1] for r in T_.groupBy("channel").count().collect()},
        "tx_status": {r[0]: r[1] for r in T_.groupBy("status").count().collect()},
        "items_without_product": IT.where("category_id IS NULL").count(),
        "sessions_duration_mismatch": SS.where("flag_duration_mismatch").count(),
        "sessions_by_status": {r[0]: r[1] for r in SS.groupBy("conversion_status").count().collect()},
        "converted_sessions_without_transaction": SS.where("conversion_status='converted' AND NOT has_transaction").count(),
        "page_views_without_product": PVW.where("product_id IS NULL").count(),
        "users_last_active_before_registration": U.where("last_active_at < registered_at").count(),
        "users_without_transactions": U.join(T_.select("user_id").distinct(), "user_id", "left_anti").count(),
    })
    import json, os
    os.makedirs(OUT, exist_ok=True)
    print("\nData quality:")
    print(json.dumps(q, indent=2))
    with open(f"{OUT}/quality.json", "w") as f:
        json.dump(q, f, indent=2)
    timer.save(f"{OUT}/clean_timings.json")
    spark.stop()


if __name__ == "__main__":
    main()
