"""Part 2.1 - (a) "Users who bought X also bought Y" indicators and (b) cohort analysis (both options).

(a) Co-purchase: for every pair of products in the same order:
      support    = orders with A and B / all orders
      confidence = orders with A and B / orders with A        (P(B | A))
      lift       = confidence / (orders with B / all orders)  (>1: bought together more than by chance)
    Lift corrects for popularity: a best-seller has high confidence with everything, but lift near 1.
    Same indicators for "viewed together" from page views (much denser, so better supported).
(b) Cohorts: users grouped by registration month; spend per calendar month and per user.
"""
from pyspark.sql import functions as F

from common import LAKE, OUT, Timer, spark_session

MIN_PAIR_ORDERS = int(__import__("os").getenv("MIN_PAIR_ORDERS", "3"))
MIN_COVIEW = int(__import__("os").getenv("MIN_COVIEW", "5"))


def pair_indicators(baskets, n_baskets, key, min_count):
    """baskets: (key, product_id) distinct. Returns pair counts with support, confidence (both ways), lift."""
    item_counts = baskets.groupBy("product_id").agg(F.count("*").alias("n"))
    a, b = baskets.alias("a"), baskets.alias("b")
    pairs = (a.join(b, (F.col(f"a.{key}") == F.col(f"b.{key}")) & (F.col("a.product_id") < F.col("b.product_id")))
             .groupBy(F.col("a.product_id").alias("product_a"), F.col("b.product_id").alias("product_b"))
             .agg(F.count("*").alias("n_ab"))
             .where(F.col("n_ab") >= min_count))
    ia, ib = item_counts.alias("ia"), item_counts.alias("ib")
    return (pairs.join(F.broadcast(ia), F.col("product_a") == F.col("ia.product_id"))
            .join(F.broadcast(ib), F.col("product_b") == F.col("ib.product_id"))
            .select("product_a", "product_b", "n_ab", F.col("ia.n").alias("n_a"), F.col("ib.n").alias("n_b"))
            .withColumn("support", F.col("n_ab") / F.lit(n_baskets))
            .withColumn("confidence_a_to_b", F.col("n_ab") / F.col("n_a"))
            .withColumn("confidence_b_to_a", F.col("n_ab") / F.col("n_b"))
            .withColumn("lift", F.col("n_ab") * F.lit(n_baskets) / (F.col("n_a") * F.col("n_b"))))


def main():
    spark = spark_session("ecommerce-recommend-cohort")
    timer = Timer()
    items = spark.read.parquet(f"{LAKE}/transaction_items")
    names = spark.read.parquet(f"{LAKE}/products").select("product_id", "product_name", "category_name")

    # ---------- (a) bought together ----------
    baskets = items.select("transaction_id", "product_id").distinct().cache()
    n_orders = timer.run("count orders", lambda: baskets.select("transaction_id").distinct().count())
    sizes = baskets.groupBy("transaction_id").count()
    size_dist = {r["count"]: r["n"] for r in sizes.groupBy("count").agg(F.count("*").alias("n")).collect()}
    print("distinct products per order:", dict(sorted(size_dist.items())))
    bought = timer.run("co-purchase pairs", lambda: pair_indicators(baskets, n_orders, "transaction_id", MIN_PAIR_ORDERS).cache())
    n_pairs = bought.count()
    lift_stats = bought.agg(F.min("lift"), F.expr("percentile_approx(lift, 0.5)"), F.max("lift")).first()
    if n_pairs:
        print(f"pairs bought together in >= {MIN_PAIR_ORDERS} orders: {n_pairs:,} | lift min/median/max: "
              f"{lift_stats[0]:.2f} / {lift_stats[1]:.2f} / {lift_stats[2]:.2f}")
    else:
        print(f"no pair was bought together in >= {MIN_PAIR_ORDERS} orders")
    top_bought = (bought.orderBy(F.desc("lift"), F.desc("n_ab")).limit(15)
                  .join(names.withColumnRenamed("product_id", "product_a").withColumnRenamed("product_name", "name_a")
                        .drop("category_name"), "product_a")
                  .join(names.withColumnRenamed("product_id", "product_b").withColumnRenamed("product_name", "name_b")
                        .drop("category_name"), "product_b")
                  .orderBy(F.desc("lift")))
    print("\nTop co-purchase pairs by lift:")
    top_bought.select("product_a", "product_b", "n_ab", "n_a", "n_b", F.round("confidence_a_to_b", 3).alias("conf_a_b"),
                      F.round("lift", 1).alias("lift")).show(15, truncate=False)

    # ---------- (a') viewed together (page views in the same session) ----------
    views = (spark.read.parquet(f"{LAKE}/page_views").where("product_id IS NOT NULL")
             .select("session_id", "product_id").distinct())
    n_view_sessions = views.select("session_id").distinct().count()
    viewed = timer.run("co-view pairs", lambda: pair_indicators(views, n_view_sessions, "session_id", MIN_COVIEW).cache())
    print(f"\nPairs viewed together in >= {MIN_COVIEW} sessions: {viewed.count():,} (of {n_view_sessions:,} sessions with product views)")
    viewed.orderBy(F.desc("n_ab")).select("product_a", "product_b", "n_ab", F.round("lift", 2).alias("lift")).show(10)

    # recommendation list: for each product, top 5 partners by lift (bought together), both directions
    both = (bought.select(F.col("product_a").alias("product_id"), F.col("product_b").alias("recommend"), "lift", "n_ab")
            .unionByName(bought.select(F.col("product_b").alias("product_id"), F.col("product_a").alias("recommend"), "lift", "n_ab")))
    w = __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy("product_id").orderBy(F.desc("lift"), F.desc("n_ab"))
    recs = both.withColumn("rank", F.row_number().over(w)).where("rank <= 5")
    timer.run("write recommendations", lambda: recs.write.mode("overwrite").parquet(f"{LAKE}/recommendations"))
    bought.orderBy(F.desc("lift")).limit(200).toPandas().to_csv(f"{OUT}/co_purchase_top200.csv", index=False)

    # ---------- (b) cohort analysis ----------
    users = spark.read.parquet(f"{LAKE}/users").select("user_id", "registration_month")
    cohort_size = users.groupBy("registration_month").agg(F.count("*").alias("cohort_users"))
    spend = (spark.read.parquet(f"{LAKE}/transactions").join(users, "user_id")
             .groupBy("registration_month", "month")
             .agg(F.countDistinct("user_id").alias("active_users"), F.count("*").alias("orders"),
                  F.round(F.sum("total"), 2).alias("revenue")))
    cohorts = (spend.join(cohort_size, "registration_month")
               .withColumn("months_since_registration",
                           F.months_between(F.to_date(F.concat("month", F.lit("-01"))),
                                            F.to_date(F.concat("registration_month", F.lit("-01")))).cast("int"))
               .withColumn("active_share", F.round(F.col("active_users") / F.col("cohort_users"), 3))
               .withColumn("revenue_per_cohort_user", F.round(F.col("revenue") / F.col("cohort_users"), 2))
               .orderBy("registration_month", "month"))
    pdf = timer.run("cohort table", lambda: cohorts.toPandas())
    pdf.to_csv(f"{OUT}/cohorts.csv", index=False)
    print("\nCohort analysis (registration month x calendar month):")
    print(pdf.to_string(index=False))
    print("\nRevenue per cohort user (rows: registration month, columns: calendar month):")
    print(pdf.pivot(index="registration_month", columns="month", values="revenue_per_cohort_user").to_string())
    timer.save(f"{OUT}/recommend_cohort_timings.json")
    spark.stop()


if __name__ == "__main__":
    main()
