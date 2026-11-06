"""Part 4 - Charts from the saved results (no database needed). Output: results/figures/*.png"""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

FIG = "results/figures"
os.makedirs(FIG, exist_ok=True)
plt.rcParams.update({"figure.dpi": 150, "axes.grid": True, "grid.alpha": 0.3, "axes.spines.top": False,
                     "axes.spines.right": False})


def save(fig, name):
    fig.tight_layout()
    fig.savefig(f"{FIG}/{name}.png")
    plt.close(fig)
    print("saved", f"{FIG}/{name}.png")


# 1 Revenue by category (MongoDB P1)
p1 = json.load(open("results/mongo/p1_revenue_by_category_month.json"))["result"][0]
cat = pd.DataFrame(p1["by_category"]).sort_values("net")
fig, ax = plt.subplots(figsize=(9, 7))
ax.barh(cat["name"], cat["net"] / 1e6, color="#2b6cb0")
ax.set_xlabel("Net revenue (millions, after discounts)")
ax.set_title("Net revenue by category (MongoDB aggregation P1)")
ax.set_xlim(cat["net"].min() / 1e6 * 0.9, cat["net"].max() / 1e6 * 1.02)
save(fig, "fig1_revenue_by_category")

# 2 Weekly revenue trend (Spark SQL2)
wk = pd.read_csv("results/spark/sql2.csv", parse_dates=["week"])
fig, ax = plt.subplots(figsize=(10, 4.5))
ax.plot(wk["week"], wk["revenue"] / 1e6, marker="o", label="Weekly revenue")
ax.plot(wk["week"], wk["moving_avg_4w"] / 1e6, linestyle="--", label="4-week moving average")
for i in (0, len(wk) - 1):
    ax.annotate("partial week", (wk["week"][i], wk["revenue"][i] / 1e6), textcoords="offset points",
                xytext=(0, 10), ha="center", fontsize=8)
ax.set_ylabel("Revenue (millions)")
ax.set_title("Weekly revenue with 4-week moving average (Spark SQL window functions)")
ax.legend()
save(fig, "fig2_weekly_revenue")

# 3 Customer spend segments (MongoDB P2 $bucketAuto)
seg = pd.DataFrame(json.load(open("results/mongo/p2b_spend_segments.json"))["result"])
seg["label"] = seg["_id"].apply(lambda r: f"{r['min'] / 1000:.0f}k-{r['max'] / 1000:.0f}k")
fig, ax1 = plt.subplots(figsize=(9, 4.5))
ax1.bar(seg["label"], seg["avg_spent"] / 1000, color="#38a169", label="Average spend (thousands)")
ax1.set_ylabel("Average spend per user (thousands)")
ax1.set_xlabel("Spend quintile (2,000 users each)")
ax2 = ax1.twinx()
ax2.plot(seg["label"], seg["avg_orders"], color="#c05621", marker="o", label="Average orders")
ax2.set_ylabel("Average orders per user")
ax2.grid(False)
ax1.set_title("Customer segments by total spend (MongoDB $bucketAuto)")
save(fig, "fig3_customer_segments")

# 4 Conversion funnel (Part 3: HBase + MongoDB in Spark)
fn = pd.read_csv("results/integration/session_funnel.csv")
labels = {"sessions": "All sessions", "viewed_product": "Viewed a product", "added_to_cart": "Added to cart",
          "reached_checkout": "Reached checkout", "flagged_converted": "Flagged 'converted'",
          "purchased": "Real purchase (MongoDB)"}
fig, ax = plt.subplots(figsize=(9, 4.8))
colors = ["#2b6cb0"] * 4 + ["#a0aec0", "#c53030"]
bars = ax.barh([labels[s] for s in fn["stage"]][::-1], fn["pct_of_all"][::-1], color=colors[::-1])
for b, v in zip(bars, fn["pct_of_all"][::-1]):
    ax.text(b.get_width() + 1, b.get_y() + b.get_height() / 2, f"{v:.1f}%", va="center")
ax.set_xlabel("% of all sampled sessions")
ax.set_xlim(0, 110)
ax.set_title("Conversion funnel: HBase sessions joined with MongoDB transactions in Spark")
save(fig, "fig4_conversion_funnel")

# 5 Flag vs real conversion by device (Spark SQL3)
s3 = pd.read_csv("results/spark/sql3.csv")
dev = s3.groupby("device_type").apply(lambda g: pd.Series({
    "flagged": (g.flagged_converted_pct * g.sessions).sum() / g.sessions.sum(),
    "real": (g.real_purchase_pct * g.sessions).sum() / g.sessions.sum()}), include_groups=False).reset_index()
x = range(len(dev))
fig, ax = plt.subplots(figsize=(8, 4.5))
ax.bar([i - 0.2 for i in x], dev["flagged"], width=0.4, label="'converted' flag", color="#a0aec0")
ax.bar([i + 0.2 for i in x], dev["real"], width=0.4, label="real purchase", color="#c53030")
ax.set_xticks(list(x)); ax.set_xticklabels(dev["device_type"])
ax.set_ylabel("% of sessions")
ax.set_title("Session flag overstates purchases on every device")
ax.legend()
save(fig, "fig5_flag_vs_real_by_device")

# 6 Cohort heatmap (Spark cohort analysis)
co = pd.read_csv("results/spark/cohorts.csv")
pv = co.pivot(index="registration_month", columns="month", values="revenue_per_cohort_user")
fig, ax = plt.subplots(figsize=(8, 5))
im = ax.imshow(pv.values / 1000, cmap="Blues", aspect="auto")
ax.set_xticks(range(len(pv.columns))); ax.set_xticklabels(pv.columns)
ax.set_yticks(range(len(pv.index))); ax.set_yticklabels(pv.index)
for i in range(pv.shape[0]):
    for j in range(pv.shape[1]):
        ax.text(j, i, f"{pv.values[i, j] / 1000:.1f}k", ha="center", va="center", fontsize=8)
ax.set_xlabel("Calendar month (Aug and Nov are partial)")
ax.set_ylabel("Registration cohort")
ax.set_title("Revenue per cohort user (Spark cohort analysis)")
ax.grid(False)
fig.colorbar(im, ax=ax, label="thousands per user")
save(fig, "fig6_cohort_heatmap")

# 7 Most-viewed vs top-selling products (Part 3 product funnel: HBase views + MongoDB purchases)
pf = pd.read_csv("results/integration/product_funnel.csv").fillna(0)
r = pf["view_sessions"].corr(pf["units"])
top_viewed = set(pf.nlargest(50, "view_sessions")["product_id"])
top_selling = set(pf.nlargest(50, "units")["product_id"])
overlap = len(top_viewed & top_selling)
fig, ax = plt.subplots(figsize=(8, 5.5))
ax.scatter(pf["view_sessions"], pf["units"], s=6, alpha=0.35, color="#2b6cb0", label="product")
both = pf[pf["product_id"].isin(top_viewed & top_selling)]
ax.scatter(both["view_sessions"], both["units"], s=30, color="#c53030", label=f"in both top-50 lists ({overlap})")
ax.set_xlabel("Sessions in which the product was viewed (HBase 20% sample)")
ax.set_ylabel("Units bought in those sessions (MongoDB)")
ax.set_title(f"Most-viewed vs top-selling products (correlation r = {r:.2f})")
ax.legend()
save(fig, "fig7_viewed_vs_selling")
print(f"views-units correlation r = {r:.3f} | overlap of top-50 viewed and top-50 selling: {overlap}")
