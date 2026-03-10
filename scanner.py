"""
SEISMIC Scanner
Scans 5 countries for unusual Google search spikes
and saves results to your Supabase database.

Features:
- Seed terms + live trending searches per country
- 7d and 30d baselines
- Related queries (what people actually searched)
- Spike date (actual day term peaked)
- Historical readings database (builds your own baseline over time)
- Pre-spike detection (catches rising signals before they cross threshold)
"""

import time
import os
import random
import requests
from datetime import datetime, timedelta
from pytrends.request import TrendReq
from supabase import create_client
import pandas as pd

# ─────────────────────────────────────────
# YOUR 5 COUNTRIES
# ─────────────────────────────────────────
COUNTRIES = [
    "US",   # United States
    "GB",   # United Kingdom
    "AU",   # Australia
    "IL",   # Israel
    "IT",   # Italy
]

# ─────────────────────────────────────────
# SEED TERMS (always checked every scan)
# English terms checked across all 5 countries
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

# ─────────────────────────────────────────
# HYPERLOCAL NATIVE LANGUAGE TERMS
# Terms in local languages per country.
# These spike earlier than English equivalents
# because locals search in their own language first.
# ─────────────────────────────────────────
NATIVE_TERMS = {
    "IL": [
        # Hebrew — emergency and disaster terms
        "רעידת אדמה",    # earthquake
        "פיצוץ",          # explosion
        "שריפה",          # fire
        "פינוי",          # evacuation
        "אזעקה",          # alarm/siren
        "נפגעים",         # casualties
        "חירום",          # emergency
        "הצפה",           # flood
        "טיל",            # missile
        "פיגוע",          # attack
    ],
    "IT": [
        # Italian — emergency and disaster terms
        "terremoto",      # earthquake
        "esplosione",     # explosion
        "alluvione",      # flood
        "evacuazione",    # evacuation
        "incendio",       # fire
        "blackout",       # power outage
        "emergenza",      # emergency
        "ospedale",       # hospital
        "frana",          # landslide
        "vittime",        # casualties/victims
    ],
}

# Maps ISO country codes to pytrends trending_searches names
TRENDING_COUNTRY_MAP = {
    "US": "united_states",
    "GB": "united_kingdom",
    "AU": "australia",
    "IL": "israel",
    "IT": "italy",
}

# How unusual does a spike need to be before we flag it?
Z_THRESHOLD = 2.5

# NewsAPI key — stored as environment variable in GitHub Actions
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")

# Maps ISO country codes to full names for news search
COUNTRY_NAMES = {
    "US": "United States",
    "GB": "United Kingdom",
    "AU": "Australia",
    "IL": "Israel",
    "IT": "Italy",
}

# Pre-spike: flag if a term has risen for this many consecutive scans
PRESPIKE_CONSECUTIVE = 3

# ─────────────────────────────────────────
# ANOMALY DETECTION MATHS
# ─────────────────────────────────────────

def z_score(series):
    if len(series) < 5:
        return 0.0
    baseline = series[:-1]
    mean = baseline.mean()
    std  = baseline.std()
    if std == 0:
        return 0.0
    latest = series.iloc[-1]
    return round((latest - mean) / std, 2)


def pct_increase(series):
    if len(series) < 2:
        return 0.0
    baseline = series[:-1].mean()
    if baseline == 0:
        return 0.0
    latest = series.iloc[-1]
    return round(((latest - baseline) / baseline) * 100, 1)


# ─────────────────────────────────────────
# PRE-SPIKE DETECTION
# Looks at recent readings history for a term/country
# and flags if it has been consistently rising
# across PRESPIKE_CONSECUTIVE scans
# ─────────────────────────────────────────

def check_prespike(supabase, term, country, current_score):
    """
    Fetch the last N readings for this term+country from our
    historical database and check if it has been rising
    consistently without yet crossing the anomaly threshold.
    Returns a prespike dict if detected, else None.
    """
    try:
        result = supabase.table("readings") \
            .select("score, scanned_at") \
            .eq("term", term) \
            .eq("country", country) \
            .eq("timeframe", "7d") \
            .order("scanned_at", desc=True) \
            .limit(PRESPIKE_CONSECUTIVE + 1) \
            .execute()

        rows = result.data
        if len(rows) < PRESPIKE_CONSECUTIVE:
            return None  # Not enough history yet

        # Most recent first — reverse to get oldest first
        scores = [r["score"] for r in reversed(rows)]
        scores.append(current_score)  # add current reading

        # Check if consistently rising
        rising = all(scores[i] < scores[i+1] for i in range(len(scores)-1))
        if not rising:
            return None

        # Calculate slope (how fast it's rising)
        slope = round(scores[-1] - scores[0], 2)
        if slope < 5:
            return None  # Rising but too slowly to be interesting

        readings_str = ", ".join([str(round(s, 1)) for s in scores])
        print(f"  ⚠️  PRE-SPIKE: '{term}' in {country} rising for {len(scores)} scans: {readings_str}")

        return {
            "term":         term,
            "country":      country,
            "slope":        slope,
            "latest_score": current_score,
            "readings":     readings_str,
            "detected_at":  datetime.now().isoformat(),
        }

    except Exception as e:
        print(f"  Pre-spike check error for {term}/{country}: {e}")
        return None


# ─────────────────────────────────────────
# NEWS EXPLANATION CHECK
# When a spike is detected, search NewsAPI
# to see if there is a news story explaining it.
# Returns: "explained", "unexplained", or "partial"
# ─────────────────────────────────────────

def check_news(term, country):
    """
    Search NewsAPI for recent articles about this term in this country.
    Returns a dict with:
      - status: "explained" / "partial" / "unexplained"
      - headline: top matching headline if found
      - article_count: how many articles found
    """
    if not NEWS_API_KEY:
        return {"status": "unknown", "headline": None, "article_count": 0}

    country_name = COUNTRY_NAMES.get(country, country)
    query        = f"{term} {country_name}"

    # Search last 6 hours of news
    from_time = (datetime.now() - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%S")

    try:
        resp = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q":        query,
                "from":     from_time,
                "sortBy":   "publishedAt",
                "pageSize": 5,
                "apiKey":   NEWS_API_KEY,
            },
            timeout=10
        )
        data = resp.json()

        if data.get("status") != "ok":
            return {"status": "unknown", "headline": None, "article_count": 0}

        count    = data.get("totalResults", 0)
        articles = data.get("articles", [])
        headline = articles[0]["title"] if articles else None

        if count >= 3:
            status = "explained"
        elif count >= 1:
            status = "partial"
        else:
            status = "unexplained"

        label = "📰 EXPLAINED" if status == "explained" else "⚠️ PARTIAL" if status == "partial" else "🔴 UNEXPLAINED"
        print(f"    {label} — {count} articles found" + (f": {headline[:60]}..." if headline else ""))

        return {
            "status":        status,
            "headline":      headline,
            "article_count": count,
        }

    except Exception as e:
        print(f"    News check error: {e}")
        return {"status": "unknown", "headline": None, "article_count": 0}


# ─────────────────────────────────────────
# MAIN SCAN FUNCTION
# ─────────────────────────────────────────

def run_scan():
    print(f"\n{'='*50}")
    print(f"SEISMIC Scan — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    # ── ON/OFF SWITCH ──────────────────────────
    # Check environment variable SCANNER_ENABLED
    # Set to "false" in GitHub Actions variables to pause the scanner
    enabled = os.environ.get("SCANNER_ENABLED", "true").lower()
    if enabled == "false":
        print("⏸  Scanner is paused — SCANNER_ENABLED=false")
        print("   To resume: set SCANNER_ENABLED=true in GitHub Actions variables")
        return

    pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 30))

    # Publishable key — read only, safe for dashboard
    supabase_read = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_KEY"]
    )
    # Service role key — write access, never exposed publicly
    supabase = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_KEY"]
    )

    anomalies  = []
    readings   = []   # all raw scores — saved to historical db
    prespikes  = []   # rising signals below threshold

    for i, country in enumerate(COUNTRIES):
        print(f"\nScanning {country} ({i+1}/{len(COUNTRIES)})...")

        # ── SEED TERMS ─────────────────────────────────
        for timeframe, tf_label in [("now 7-d", "7d"), ("today 1-m", "30d")]:
            try:
                pytrends.build_payload(SEED_TERMS, timeframe=timeframe, geo=country)
                df = pytrends.interest_over_time()

                if df.empty:
                    print(f"  No data for {country} ({tf_label})")
                    continue

                if "isPartial" in df.columns:
                    df = df.drop(columns=["isPartial"])

                for term in SEED_TERMS:
                    if term not in df.columns:
                        continue

                    current_score = float(df[term].iloc[-1])
                    z   = z_score(df[term])
                    pct = pct_increase(df[term])

                    # Save every reading to historical database
                    if tf_label == "7d":
                        readings.append({
                            "term":       term,
                            "country":    country,
                            "score":      current_score,
                            "scanned_at": datetime.now().isoformat(),
                            "timeframe":  tf_label,
                        })

                        # Check for pre-spike pattern
                        if z < Z_THRESHOLD and current_score > 10:
                            ps = check_prespike(supabase, term, country, current_score)
                            if ps:
                                prespikes.append(ps)

                    if z >= Z_THRESHOLD and pct > 50:
                        related = []
                        try:
                            time.sleep(10 + random.uniform(0, 5.0))
                            rq = pytrends.related_queries()
                            if term in rq and rq[term]['top'] is not None:
                                related = rq[term]['top']['query'].head(5).tolist()
                        except Exception:
                            pass

                        peak_idx   = df[term].idxmax()
                        spike_date = peak_idx.isoformat() if hasattr(peak_idx, 'isoformat') else datetime.now().isoformat()

                        # Check news to see if spike is explained
                        news = check_news(term, country)

                        anomalies.append({
                            "term":            term,
                            "country":         country,
                            "z_score":         z,
                            "pct_increase":    pct,
                            "detected_at":     datetime.now().isoformat(),
                            "spike_date":      spike_date,
                            "timeframe":       tf_label,
                            "related_queries": ", ".join(related) if related else None,
                            "news_status":     news["status"],
                            "news_headline":   news["headline"],
                        })
                        level = "🔴" if z >= 4.5 else "🟡" if z >= 3.5 else "🔵"
                        rq_str = f" → {related[0]}, {related[1]}..." if len(related) >= 2 else ""
                        print(f"  {level} [{tf_label}] '{term}' — +{pct}% | {z}σ{rq_str}")

                time.sleep(15 + random.uniform(0, 7.5))

            except Exception as e:
                print(f"  Error scanning {country} ({tf_label}): {e}")

        # ── TRENDING SEARCHES ──────────────────────────
        pn = TRENDING_COUNTRY_MAP.get(country)
        if pn:
            try:
                print(f"  Fetching trending searches for {country}...")
                time.sleep(20 + random.uniform(0, 10.0))
                trending_df    = pytrends.trending_searches(pn=pn)
                trending_terms = trending_df[0].head(20).tolist()
                print(f"  Found {len(trending_terms)} trending terms")

                for batch_start in range(0, len(trending_terms), 5):
                    batch = trending_terms[batch_start:batch_start+5]
                    try:
                        time.sleep(20 + random.uniform(0, 10.0))
                        pytrends.build_payload(batch, timeframe="now 7-d", geo=country)
                        df_t = pytrends.interest_over_time()
                        if df_t.empty:
                            continue
                        if "isPartial" in df_t.columns:
                            df_t = df_t.drop(columns=["isPartial"])

                        for term in batch:
                            if term not in df_t.columns:
                                continue

                            current_score = float(df_t[term].iloc[-1])
                            z   = z_score(df_t[term])
                            pct = pct_increase(df_t[term])

                            # Save trending readings too
                            readings.append({
                                "term":       term,
                                "country":    country,
                                "score":      current_score,
                                "scanned_at": datetime.now().isoformat(),
                                "timeframe":  "7d",
                            })

                            # Check pre-spike for trending terms too
                            if z < Z_THRESHOLD and current_score > 10:
                                ps = check_prespike(supabase, term, country, current_score)
                                if ps:
                                    prespikes.append(ps)

                            if z >= Z_THRESHOLD and pct > 50:
                                already = any(
                                    a["term"].lower() == term.lower() and a["country"] == country
                                    for a in anomalies
                                )
                                if already:
                                    continue

                                related = []
                                try:
                                    time.sleep(10 + random.uniform(0, 5.0))
                                    rq = pytrends.related_queries()
                                    if term in rq and rq[term]['top'] is not None:
                                        related = rq[term]['top']['query'].head(5).tolist()
                                except Exception:
                                    pass

                                peak_idx   = df_t[term].idxmax()
                                spike_date = peak_idx.isoformat() if hasattr(peak_idx, 'isoformat') else datetime.now().isoformat()

                                # Check news for trending anomalies too
                                news = check_news(term, country)

                                anomalies.append({
                                    "term":            term,
                                    "country":         country,
                                    "z_score":         z,
                                    "pct_increase":    pct,
                                    "detected_at":     datetime.now().isoformat(),
                                    "spike_date":      spike_date,
                                    "timeframe":       "7d",
                                    "related_queries": ", ".join(related) if related else None,
                                    "news_status":     news["status"],
                                    "news_headline":   news["headline"],
                                })
                                level = "🔴" if z >= 4.5 else "🟡" if z >= 3.5 else "🔵"
                                print(f"  {level} [trending] '{term}' — +{pct}% | {z}σ")

                    except Exception as e:
                        print(f"  Batch error {batch}: {e}")
                        time.sleep(30 + random.uniform(0, 15.0))

            except Exception as e:
                print(f"  Could not fetch trending for {country}: {e}")

        # ── NATIVE LANGUAGE TERMS ─────────────────────
        native = NATIVE_TERMS.get(country)
        if native:
            print(f"  Scanning native language terms for {country}...")
            # Scan in batches of 5
            for batch_start in range(0, len(native), 5):
                batch = native[batch_start:batch_start+5]
                try:
                    time.sleep(20 + random.uniform(0, 10.0))
                    pytrends.build_payload(batch, timeframe="now 7-d", geo=country)
                    df_n = pytrends.interest_over_time()
                    if df_n.empty:
                        continue
                    if "isPartial" in df_n.columns:
                        df_n = df_n.drop(columns=["isPartial"])

                    for term in batch:
                        if term not in df_n.columns:
                            continue

                        current_score = float(df_n[term].iloc[-1])
                        z   = z_score(df_n[term])
                        pct = pct_increase(df_n[term])

                        # Save to historical readings
                        readings.append({
                            "term":       term,
                            "country":    country,
                            "score":      current_score,
                            "scanned_at": datetime.now().isoformat(),
                            "timeframe":  "7d",
                        })

                        # Pre-spike check
                        if z < Z_THRESHOLD and current_score > 10:
                            ps = check_prespike(supabase, term, country, current_score)
                            if ps:
                                prespikes.append(ps)

                        if z >= Z_THRESHOLD and pct > 50:
                            # Skip duplicates
                            already = any(
                                a["term"].lower() == term.lower() and a["country"] == country
                                for a in anomalies
                            )
                            if already:
                                continue

                            related = []
                            try:
                                time.sleep(10 + random.uniform(0, 5.0))
                                rq = pytrends.related_queries()
                                if term in rq and rq[term]['top'] is not None:
                                    related = rq[term]['top']['query'].head(5).tolist()
                            except Exception:
                                pass

                            news = check_news(term, country)

                            peak_idx   = df_n[term].idxmax()
                            spike_date = peak_idx.isoformat() if hasattr(peak_idx, 'isoformat') else datetime.now().isoformat()

                            anomalies.append({
                                "term":            term,
                                "country":         country,
                                "z_score":         z,
                                "pct_increase":    pct,
                                "detected_at":     datetime.now().isoformat(),
                                "spike_date":      spike_date,
                                "timeframe":       "7d",
                                "related_queries": ", ".join(related) if related else None,
                                "news_status":     news["status"],
                                "news_headline":   news["headline"],
                            })
                            level = "🔴" if z >= 4.5 else "🟡" if z >= 3.5 else "🔵"
                            print(f"  {level} [native] '{term}' — +{pct}% | {z}σ")

                except Exception as e:
                    print(f"  Native batch error {batch}: {e}")
                    time.sleep(30 + random.uniform(0, 15.0))

        if i < len(COUNTRIES) - 1:
            print(f"  Waiting 45s before next country...")
            time.sleep(45 + random.uniform(0, 22.5))

    # ── SAVE EVERYTHING TO SUPABASE ───────
    if readings:
        batch_size = 50
        for b in range(0, len(readings), batch_size):
            supabase.table("readings").insert(readings[b:b+batch_size]).execute()
        print(f"\n📊 Saved {len(readings)} readings to historical database")

    # ── AUTO CLEANUP — delete readings older than 90 days ──
    try:
        cutoff = (datetime.now() - timedelta(days=90)).isoformat()
        result = supabase.table("readings") \
            .delete() \
            .lt("scanned_at", cutoff) \
            .execute()
        print(f"🧹 Auto-cleanup: removed readings older than 90 days")
    except Exception as e:
        print(f"  Cleanup error: {e}")

    if anomalies:
        supabase.table("anomalies").insert(anomalies).execute()
        print(f"✅ Saved {len(anomalies)} anomalies")
    else:
        print(f"✅ No anomalies this scan — all quiet")

    if prespikes:
        supabase.table("prespikes").insert(prespikes).execute()
        print(f"⚠️  Saved {len(prespikes)} pre-spike warnings")
    else:
        print(f"✅ No pre-spikes detected")


# ─────────────────────────────────────────
# ENTRY POINT — GitHub Actions runs this once per cron
# ─────────────────────────────────────────

if __name__ == "__main__":
    print("🛰  SEISMIC starting...")
    print(f"   Countries : {', '.join(COUNTRIES)}")
    print(f"   Threshold : {Z_THRESHOLD}σ")
    print("   Triggered by GitHub Actions cron")

    try:
        run_scan()
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        raise
