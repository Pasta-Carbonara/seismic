"""
SEISMIC Scanner
Scans 5 countries for unusual Google search spikes
and saves results to your Supabase database.
"""

import time
import os
from datetime import datetime
from pytrends.request import TrendReq
from supabase import create_client
import pandas as pd

# ─────────────────────────────────────────
# YOUR 5 COUNTRIES
# These are ISO country codes — two letters per country
# ─────────────────────────────────────────
COUNTRIES = [
    "US",   # United States
    "GB",   # United Kingdom
    "AU",   # Australia
    "IL",   # Israel
    "IT",   # Italy
]

# ─────────────────────────────────────────
# WHAT TO SCAN FOR
# These are the search terms we check for spikes.
# If "earthquake" suddenly spikes in Australia,
# that's worth flagging.
# ─────────────────────────────────────────
SEED_TERMS = [
    "earthquake",
    "explosion",
    "hospital",
    "power outage",
    "flood",
    "virus",
    "evacuation",
    "emergency",
    "missing",
    "bank closed",
]

# How unusual does a spike need to be before we flag it?
# 2.5 = noticeably unusual. 4.0 = extremely rare.
Z_THRESHOLD = 2.5

# ─────────────────────────────────────────
# ANOMALY DETECTION MATHS
# ─────────────────────────────────────────

def z_score(series):
    """
    Measures how weird the latest value is vs the recent past.

    Example: if "earthquake" normally gets a score of 10,
    but today it's 80, the z-score will be very high —
    meaning something unusual is happening.

    Z-score above 2.5 = flag it.
    Z-score above 4.0 = very significant.
    """
    if len(series) < 5:
        return 0.0
    baseline = series[:-1]   # everything except the latest value
    mean = baseline.mean()
    std  = baseline.std()
    if std == 0:
        return 0.0
    latest = series.iloc[-1]
    return round((latest - mean) / std, 2)


def pct_increase(series):
    """How much did the latest value jump vs the average?"""
    if len(series) < 2:
        return 0.0
    baseline = series[:-1].mean()
    if baseline == 0:
        return 0.0
    latest = series.iloc[-1]
    return round(((latest - baseline) / baseline) * 100, 1)


# ─────────────────────────────────────────
# MAIN SCAN FUNCTION
# ─────────────────────────────────────────

def run_scan():
    print(f"\n{'='*50}")
    print(f"SEISMIC Scan — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    # Connect to Google Trends (via pytrends)
    pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 30))

    # Connect to your Supabase database
    # These values come from environment variables —
    # you'll set them in Render so they never appear in your code
    supabase = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_KEY"]
    )

    anomalies = []

    for i, country in enumerate(COUNTRIES):
        print(f"\nScanning {country} ({i+1}/{len(COUNTRIES)})...")

        try:
            # Ask Google Trends for data
            pytrends.build_payload(
                SEED_TERMS,
                timeframe="now 7-d",   # last 7 days
                geo=country
            )
            df = pytrends.interest_over_time()

            if df.empty:
                print(f"  No data returned for {country}")
                continue

            # Remove the 'isPartial' column pytrends adds
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])

            # Check each search term for anomalies
            for term in SEED_TERMS:
                if term not in df.columns:
                    continue

                z   = z_score(df[term])
                pct = pct_increase(df[term])

                if z >= Z_THRESHOLD and pct > 50:
                    anomalies.append({
                        "term":         term,
                        "country":      country,
                        "z_score":      z,
                        "pct_increase": pct,
                        "detected_at":  datetime.now().isoformat(),
                    })
                    level = "🔴" if z >= 4.5 else "🟡" if z >= 3.5 else "🔵"
                    print(f"  {level} ANOMALY: '{term}' — +{pct}% | {z}σ")

        except Exception as e:
            print(f"  Error scanning {country}: {e}")

        # Simple pause between each country
        # Stops Google seeing requests arrive too fast
        if i < len(COUNTRIES) - 1:
            print(f"  Waiting 15s before next country...")
            time.sleep(15)

    # Save results to Supabase
    if anomalies:
        supabase.table("anomalies").insert(anomalies).execute()
        print(f"\n✅ Saved {len(anomalies)} anomalies to database")
    else:
        print(f"\n✅ No anomalies this scan — all quiet")


# ─────────────────────────────────────────
# LOOP — runs forever, scans every 30 mins
# ─────────────────────────────────────────

if __name__ == "__main__":
    print("🛰  SEISMIC starting...")
    print(f"   Countries : {', '.join(COUNTRIES)}")
    print(f"   Threshold : {Z_THRESHOLD}σ")
    print(f"   Interval  : 30 minutes")

    while True:
        try:
            run_scan()
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            print("   Waiting 5 minutes then retrying...")
            time.sleep(300)
            continue

        print(f"\n⏰ Next scan in 30 minutes...")
        time.sleep(1800)
