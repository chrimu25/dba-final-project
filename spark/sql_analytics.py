"""Part 2.2 - Spark SQL analytics on the Parquet tables, plus a join of data that lives in MongoDB and HBase.

SQL1  Top 3 products per category by net revenue, with share of category revenue (window: RANK, SUM OVER)
SQL2  Weekly revenue with week-over-week growth and 4-week moving average (window: LAG, AVG OVER ROWS)
SQL3  Device x referrer: sessions, purchase rate (from real transactions) vs the 'converted' flag
SQL4  Engagement vs spend: MongoDB users.purchase_summary JOIN HBase user_sessions sample (loaded as DataFrames)
EXPLAIN of SQL1 is saved to show the broadcast join chosen for the small products table.
"""
import os

from pyspark.sql import functions as F

from common import LAKE, OUT, Timer, spark_session

SQL1 = """
WITH product_rev AS (
    SELECT i.category_id, p.category_name, i.product_id, p.product_name,
           SUM(i.line_net) AS net_revenue, SUM(i.quantity) AS units
    FROM transaction_items i JOIN products p ON p.product_id = i.product_id
    GROUP BY i.category_id, p.category_name, i.product_id, p.product_name
), ranked AS (
    SELECT *, RANK() OVER (PARTITION BY category_id ORDER BY net_revenue DESC) AS rnk,
           net_revenue / SUM(net_revenue) OVER (PARTITION BY category_id) AS share_of_category
    FROM product_rev
)
SELECT category_name, rnk, product_id, product_name, ROUND(net_revenue, 2) AS net_revenue, units,
       ROUND(100 * share_of_category, 2) AS pct_of_category
FROM ranked WHERE rnk <= 3 ORDER BY category_name, rnk
"""

SQL2 = """
WITH weekly AS (
    SELECT DATE_TRUNC('WEEK', ts) AS week, COUNT(*) AS orders, ROUND(SUM(total), 2) AS revenue
    FROM transactions GROUP BY DATE_TRUNC('WEEK', ts)
)
SELECT CAST(week AS DATE) AS week, orders, revenue,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY week)) / LAG(revenue) OVER (ORDER BY week), 2) AS wow_growth_pct,
       ROUND(AVG(revenue) OVER (ORDER BY week ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS moving_avg_4w
FROM weekly ORDER BY week
"""

SQL3 = """
SELECT device_type, referrer, COUNT(*) AS sessions,
       ROUND(100 * AVG(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END), 2) AS flagged_converted_pct,
       ROUND(100 * AVG(CASE WHEN has_transaction THEN 1 ELSE 0 END), 2) AS real_purchase_pct,
       ROUND(AVG(duration_seconds) / 60, 1) AS avg_minutes
FROM sessions GROUP BY device_type, referrer ORDER BY real_purchase_pct DESC
"""

SQL4 = """
SELECT CASE WHEN h.sessions_in_sample >= 45 THEN '45+'
            WHEN h.sessions_in_sample >= 40 THEN '40-44'
            WHEN h.sessions_in_sample >= 35 THEN '35-39' ELSE '<35' END AS sampled_sessions_band,
       COUNT(*) AS users,
       ROUND(AVG(h.avg_session_minutes), 1) AS avg_session_minutes,
       ROUND(AVG(m.orders), 1) AS avg_orders,
       ROUND(AVG(m.spent), 0) AS avg_spent,
       ROUND(CORR(h.sessions_in_sample, m.spent), 3) AS corr_sessions_spent
FROM mongo_users m JOIN hbase_engagement h ON h.user_id = m.user_id
GROUP BY 1 ORDER BY 1
"""


def load_mongo_users(spark):
    """MongoDB users with the purchase_summary computed in Part 1 (P2) -> DataFrame."""
    from pymongo import MongoClient
    db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))["ecommerce"]
    rows = [(u["_id"], u["purchase_summary"]["orders"], float(u["purchase_summary"]["spent"]))
            for u in db.users.find({"purchase_summary": {"$exists": True}}, {"purchase_summary": 1})]
    return spark.createDataFrame(rows, "user_id string, orders int, spent double")


def load_hbase_engagement(spark):
    """HBase user_sessions sample, family 's' only -> per-user engagement DataFrame."""
    import happybase
    conn = happybase.Connection(os.getenv("HBASE_HOST", "localhost"), port=9090, timeout=600_000)
    rows = [(d[b"s:user_id"].decode(), int(d[b"s:duration_seconds"]))
            for _, d in conn.table("user_sessions").scan(columns=[b"s:user_id", b"s:duration_seconds"], batch_size=5000)]
    conn.close()
    df = spark.createDataFrame(rows, "user_id string, duration_seconds int")
    return df.groupBy("user_id").agg(F.count("*").alias("sessions_in_sample"),
                                     (F.avg("duration_seconds") / 60).alias("avg_session_minutes"))


def main():
    spark = spark_session("ecommerce-sql")
    timer = Timer()
    for t in ("transactions", "transaction_items", "products", "sessions", "users"):
        spark.read.parquet(f"{LAKE}/{t}").createOrReplaceTempView(t)
    os.makedirs(OUT, exist_ok=True)

    for name, sql in (("SQL1 top 3 products per category", SQL1), ("SQL2 weekly revenue trend", SQL2),
                      ("SQL3 device x referrer purchase rate", SQL3)):
        pdf = timer.run(name, lambda sql=sql: spark.sql(sql).toPandas())
        pdf.to_csv(f"{OUT}/{name.split()[0].lower()}.csv", index=False)
        print(f"\n=== {name} ===")
        print(pdf.head(30).to_string(index=False))

    with open(f"{OUT}/sql1_explain.txt", "w") as f:
        f.write(spark.sql(SQL1)._jdf.queryExecution().explainString(
            spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")))
    print("\nEXPLAIN FORMATTED of SQL1 saved to results/spark/sql1_explain.txt")

    mongo = timer.run("load MongoDB users (pymongo)", lambda: load_mongo_users(spark).cache())
    hbase = timer.run("load HBase sessions sample (happybase)", lambda: load_hbase_engagement(spark).cache())
    mongo.createOrReplaceTempView("mongo_users"); hbase.createOrReplaceTempView("hbase_engagement")
    print(f"\nMongoDB users: {mongo.count():,} | HBase users with sampled sessions: {hbase.count():,}")
    pdf = timer.run("SQL4 MongoDB x HBase join", lambda: spark.sql(SQL4).toPandas())
    pdf.to_csv(f"{OUT}/sql4_mongo_hbase.csv", index=False)
    print("\n=== SQL4 engagement (HBase) vs spend (MongoDB) ===")
    print(pdf.to_string(index=False))
    timer.save(f"{OUT}/sql_timings.json")
    spark.stop()


if __name__ == "__main__":
    main()
