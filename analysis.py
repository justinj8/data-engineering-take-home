"""
Data Engineering Take-Home - Analysis
=============================================
This script loads the parquet data, cleans it, answers the questions,
and generates the required chart.

Libraries: pandas, pyarrow, matplotlib
Install:  pip install pandas pyarrow matplotlib
Run:      python3 analysis.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ─────────────────────────────────────────────
# 1. Load raw data
# ─────────────────────────────────────────────

usage = pd.read_parquet("data/usage_events.parquet")
profile = pd.read_parquet("data/profile_installation.parquet")
sim_plan = pd.read_parquet("data/sim_card_plan_history.parquet")
rate = pd.read_parquet("data/rate_card.parquet")

print("=" * 60)
print("RAW DATA LOADED")
print(f"usage_events:          {len(usage)} rows")
print(f"profile_installation:  {len(profile)} rows")
print(f"sim_card_plan_history: {len(sim_plan)} rows")
print(f"rate_card:             {len(rate)} rows")
print("=" * 60)

# ─────────────────────────────────────────────
# 2. Identify & remove duplicate usage events
# ─────────────────────────────────────────────
# sid=2 appears 3 times (from usage_1, usage_2, usage_3 parquet files).
# Rows from usage_1 and usage_2 are exact duplicates (same mb=50).
# Row from usage_3 has mb=55, a conflicting value.
# Since they share the same sid (surrogate key), pid, and evt_dttm,
# these represent the same event loaded multiple times. Keep highest mb
# as the most conservative approach, then deduplicate.

dup_mask = usage.duplicated(subset=["sid"], keep=False)
dup_count = usage[dup_mask].shape[0] - usage[dup_mask]["sid"].nunique()
print(f"\nDuplicate usage events identified: {dup_count}")
print("  (sid=2 loaded from 3 source files; keeping first occurrence)")

# Keep first occurrence per sid (usage_1 is the earliest load)
usage_clean = usage.sort_values("ld_dttm").drop_duplicates(subset=["sid"], keep="first").copy()

# ─────────────────────────────────────────────
# 3. Data quality cleanup on usage events
# ─────────────────────────────────────────────

# 3a. Negative MB (sid=26): remove — usage can't be negative
neg_mb = usage_clean[usage_clean["mb"] < 0]
print(f"\nNegative MB rows removed: {len(neg_mb)} (sid={neg_mb['sid'].tolist()})")
usage_clean = usage_clean[usage_clean["mb"] >= 0]

# 3b. Null evt_dttm (sid=27): remove — can't attribute to a day
null_dt = usage_clean[usage_clean["evt_dttm"].isnull()]
print(f"Null evt_dttm rows removed: {len(null_dt)} (sid={null_dt['sid'].tolist()})")
usage_clean = usage_clean[usage_clean["evt_dttm"].notnull()]

# 3c. Far-future date (sid=30, evt_dttm=2035): likely a typo — remove
future = usage_clean[usage_clean["evt_dttm"] > "2026-02-01"]
print(f"Far-future date rows removed: {len(future)} (sid={future['sid'].tolist()})")
usage_clean = usage_clean[usage_clean["evt_dttm"] <= "2026-02-01"]

# 3d. Orphan profile (pid=999): no matching profile_installation — flag but keep
# (the usage itself is valid data; it just can't be linked)
orphan = usage_clean[~usage_clean["pid"].isin(profile["pid"])]
print(f"Orphan pid (no profile): {len(orphan)} rows (pid={orphan['pid'].unique().tolist()}) — kept in usage totals")

# 3e. Normalize tech field to standard generations
tech_normalize = {
    "LTE": "4G", "lte": "4G", "4g": "4G", "4G": "4G",
    "5g": "5G", "5G": "5G", "NR": "5G",
    "CDMA": "3G", "HSPA+": "3G",
    "GSM": "2G",
}
usage_clean["tech_clean"] = usage_clean["tech"].map(tech_normalize)
null_tech = usage_clean[usage_clean["tech_clean"].isnull()]
print(f"Null/unknown tech after normalization: {len(null_tech)} rows (original tech={null_tech['tech'].tolist()})")

# 3f. Suspicious cc2 values (999, 99999): likely placeholder/invalid MCC codes — flag
suspect_cc2 = usage_clean[usage_clean["cc2"].isin([999, 99999])]
print(f"Suspicious cc2 values: {len(suspect_cc2)} rows (cc2={suspect_cc2['cc2'].tolist()})")

# 3g. Null cc1 (sid=28): missing country code
null_cc1 = usage_clean[usage_clean["cc1"].isnull()]
print(f"Null cc1: {len(null_cc1)} rows (sid={null_cc1['sid'].tolist()})")

print(f"\nCleaned usage events: {len(usage_clean)} rows (from {len(usage)} raw)")

# ─────────────────────────────────────────────
# 4. Answer the questions
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("ANSWERS")
print("=" * 60)

# Q1: Which sim_card_id had the highest total usage?
# The ERD connects usage_events.pid → profile_installation → sim_card_plan_history.asset_id
# "sim_card_id" most likely refers to asset_id (the SIM card identifier).
# We need to join usage → profile to get asset_id, then sum MB.

# Build profile lookup: for each pid + event time, find the active asset_id
# A profile is active when beg_dttm <= evt_dttm < end_dttm (or end_dttm is null)

# Clean profile: remove exact duplicate rows (pid=103, asset_id=1005 appears twice)
profile_clean = profile.drop_duplicates(subset=["pid", "asset_id", "beg_dttm", "end_dttm"])

# Fix profile with end < begin (pid=107): swap or nullify — since it's clearly wrong, nullify end
profile_clean.loc[profile_clean["end_dttm"] < profile_clean["beg_dttm"], "end_dttm"] = pd.NaT

def get_asset_id(row, prof_df):
    """Find the active asset_id for a usage event based on pid and event time."""
    pid_profiles = prof_df[prof_df["pid"] == row["pid"]]
    if pid_profiles.empty:
        return None
    for _, p in pid_profiles.iterrows():
        started = row["evt_dttm"] >= p["beg_dttm"]
        not_ended = pd.isnull(p["end_dttm"]) or row["evt_dttm"] < p["end_dttm"]
        if started and not_ended:
            return p["asset_id"]
    return None

usage_clean["asset_id"] = usage_clean.apply(lambda r: get_asset_id(r, profile_clean), axis=1)

sim_usage = usage_clean.dropna(subset=["asset_id"]).groupby("asset_id")["mb"].sum()
if not sim_usage.empty:
    top_sim = sim_usage.idxmax()
    print(f"\nQ1: SIM card (asset_id) with highest total usage: {int(top_sim)} ({sim_usage[top_sim]:.1f} MB)")
    print(f"    All SIM usage:\n{sim_usage.sort_values(ascending=False).to_string()}")
else:
    print("\nQ1: Could not determine — no usage events matched to SIM cards")

# --- Q2: How many usage events resolved to 3G after cleanup? ---
count_3g = (usage_clean["tech_clean"] == "3G").sum()
print(f"\nQ2: Usage events that resolved to 3G after cleanup: {count_3g}")
print(f"    (Original tech values mapped to 3G: CDMA, HSPA+)")

# --- Q3: How many duplicate usage events did you identify? ---
print(f"\nQ3: Duplicate usage events identified: {dup_count}")
print(f"    (sid=2 appeared 3 times across source files; 2 duplicates removed)")

# --- Q4: What is the cost of all data used? ---
# Join usage → profile → sim_card_plan_history → rate_card
# Need: asset_id → bundle_id (active at event time) → rate (matching cc1, cc2, tech, date)

# Clean rate_card: remove negative rates and orphan bundles
rate_clean = rate[rate["rt_amt"] >= 0].copy()
rate_clean = rate_clean[rate_clean["bundle_id"] != 9999]  # orphan bundle
rate_clean["curr_cd"] = rate_clean["curr_cd"].str.strip().str.replace(" ", "")  # fix "US D" → "USD"

# Deduplicate rate_card: for bundle 2000/310/260/4G there are 2 valid rates (0.010, 0.011)
# Keep the one with the latest entry (last row) — assume it's the correction
rate_clean = rate_clean.drop_duplicates(
    subset=["bundle_id", "cc1", "cc2", "tech_cd", "beg_dttm"], keep="last"
)

def get_bundle_id(row, plan_df):
    """Find the active bundle_id for an asset at a given time."""
    asset_plans = plan_df[plan_df["asset_id"] == row.get("asset_id")]
    if asset_plans.empty or pd.isnull(row.get("asset_id")):
        return None
    for _, p in asset_plans.iterrows():
        started = row["evt_dttm"] >= p["eff_dttm"]
        not_ended = pd.isnull(p["x_dttm"]) or row["evt_dttm"] < p["x_dttm"]
        if started and not_ended:
            return p["bundle_id"]
    return None

# Clean sim_plan: remove rows where x_dttm < eff_dttm (bad data)
# and deduplicate (asset_id=1002, bundle_id=2000 appears twice with same dates)
sim_plan_clean = sim_plan[
    sim_plan["x_dttm"].isnull() | (sim_plan["x_dttm"] >= sim_plan["eff_dttm"])
].copy()
sim_plan_clean = sim_plan_clean.drop_duplicates(subset=["asset_id", "bundle_id", "eff_dttm"])

usage_clean["bundle_id"] = usage_clean.apply(lambda r: get_bundle_id(r, sim_plan_clean), axis=1)

def get_rate(row, rate_df):
    """Find the applicable rate for a usage event."""
    if pd.isnull(row.get("bundle_id")) or pd.isnull(row.get("cc1")):
        return None
    
    # Filter rates for this bundle + country codes
    candidates = rate_df[
        (rate_df["bundle_id"] == row["bundle_id"]) &
        (rate_df["cc1"] == int(row["cc1"])) &
        (rate_df["cc2"] == row["cc2"])
    ].copy()
    
    if candidates.empty:
        return None
    
    # Filter by date range
    candidates = candidates[
        (candidates["beg_dttm"] <= row["evt_dttm"]) &
        (candidates["end_dttm"].isnull() | (candidates["end_dttm"] > row["evt_dttm"]))
    ]
    
    if candidates.empty:
        return None
    
    # Normalize tech for matching
    tech_gen = row.get("tech_clean")
    rate_tech_map = {"4G": "4G", "5G": "5G", "3G": "3G", "2G": "2G"}
    
    # Try specific tech match first (higher prio_nbr = higher priority)
    if tech_gen:
        tech_match = candidates[candidates["tech_cd"] == tech_gen]
        if not tech_match.empty:
            return tech_match.sort_values("prio_nbr", ascending=False).iloc[0]["rt_amt"]
    
    # Fall back to None/default rate (lower priority catch-all)
    default = candidates[candidates["tech_cd"].isnull()]
    if not default.empty:
        return default.sort_values("prio_nbr", ascending=False).iloc[0]["rt_amt"]
    
    return None

usage_clean["rate"] = usage_clean.apply(lambda r: get_rate(r, rate_clean), axis=1)
usage_clean["cost"] = usage_clean["mb"] * usage_clean["rate"]

total_cost = usage_clean["cost"].sum()
costed_rows = usage_clean["cost"].notna().sum()
uncosted_rows = usage_clean["cost"].isna().sum()

print(f"\nQ4: Total cost of all data used: ${total_cost:.2f} USD")
print(f"({costed_rows} events costed, {uncosted_rows} events could not be costed)")
print(f"\n Cost breakdown by event:")
cost_detail = usage_clean[["sid", "pid", "asset_id", "bundle_id", "mb", "tech_clean", "rate", "cost"]].copy()
print(cost_detail.to_string())

# ─────────────────────────────────────────────
# 5. Generate chart: Total Usage (MB) per day
# ─────────────────────────────────────────────
usage_clean["date"] = usage_clean["evt_dttm"].dt.date
daily_usage = usage_clean.groupby("date")["mb"].sum().reset_index()
daily_usage["date"] = pd.to_datetime(daily_usage["date"])

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(daily_usage["date"], daily_usage["mb"], marker="o", linewidth=2, color="#2563eb", markersize=6)
ax.fill_between(daily_usage["date"], daily_usage["mb"], alpha=0.15, color="#2563eb")
ax.set_title("Total Usage (MB) Per Day", fontsize=16, fontweight="bold", pad=12)
ax.set_xlabel("Date", fontsize=12)
ax.set_ylabel("Total Usage (MB)", fontsize=12)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
plt.xticks(rotation=45, ha="right")
ax.grid(True, alpha=0.3)
ax.set_ylim(bottom=0)
plt.tight_layout()
plt.savefig("daily_usage_chart.png", dpi=150, bbox_inches="tight")
print("\n✅ Chart saved to daily_usage_chart.png")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)
