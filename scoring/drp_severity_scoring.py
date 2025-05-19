from supabase import create_client
from datetime import datetime
from tqdm import tqdm
import logging
import os
import requests
import tempfile
import time
import hashlib
from dotenv import load_dotenv
import sys

# --- Load environment variables ---
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Constants ---
BATCH_SIZE = 5000  # Larger batch for full load
REQUEST_DELAY = 0.5
SCORING_VERSION = "v1.0"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("populate_advisor_advs.log"),
        logging.StreamHandler()
    ]
)

# --- Helper Functions ---
def generate_adv_url(crd: str) -> str:
    return f"https://reports.adviserinfo.sec.gov/reports/individual/individual_{crd}.pdf"

def hash_event(event) -> str:
    raw = f"{event.get('crd')}|{event.get('flag_type')}|{event.get('description', '')}|{event.get('event_date', '')}|{event.get('regulator', '')}|{SCORING_VERSION}"
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()

FLAG_TYPE_MAP = {
    "hasBankrupt": "Bankruptcy",
    "hasCustComp": "Customer Complaint",
    "hasCustDispute": "Customer Dispute",
    "hasRegAction": "Regulatory Action",
    "hasCriminal": "Criminal Charge",
    "hasTermination": "Termination",
    "hasInvstgn": "Investigation",
    "hasJudgment": "Civil Judgment",
    "hasCivilJudc": "Civil Judgment",
    "hasBond": "Bond Claim",
    "hasTestFlag": "Test"
}

unmapped_flag_counts = {}
unmapped_label_counts = {}
low_score_label_counts = {}

base_map = {
    "Bankruptcy": 0.2,
    "Customer Complaint": 0.6,
    "Customer Dispute": 0.6,
    "Regulatory Action": 0.8,
    "Criminal Charge": 0.9,
    "Criminal Conviction": 1.0,
    "Criminal Disclosure": 1.0,
    "Termination": 0.5,
    "Investigation": 0.7,
    "Civil Judgment": 0.6,
    "Bond Claim": 0.4,
    "Test": 0.4
}

def score_drp_event(event):
    raw_flag = event.get("flag_type") or event.get("event_type")  # fallback for previously scored records
        normalized = FLAG_TYPE_MAP.get(raw_flag, "Unknown")
    
    base = base_map.get(normalized, 0.4)
    
    desc = (event.get("description") or "").lower()
    adjusted = base
    reason = ""

    if "fraud" in desc:
        adjusted += 0.1
        reason += "fraud keyword; "
    if "unauthorized" in desc:
        adjusted += 0.1
        reason += "unauthorized keyword; "
    if "client harm" in desc or "loss" in desc:
        adjusted += 0.05
        reason += "client harm/loss keyword; "

    adjusted = min(adjusted, 1.0)
    return base, adjusted, reason.strip()

def insert_drp_event_scores(events):
    scored_events = []
    now = datetime.utcnow().isoformat()
    rows = []
    seen_event_ids = set()

    for e in events:
        base, adjusted, reason = score_drp_event(e)
        event_id = hash_event(e)
        if event_id in seen_event_ids:
            continue
        seen_event_ids.add(event_id)

        if round(base, 2) == 0.4:
            logging.warning(
                f"🚨 INSERTING 0.4 — flag_type: {e.get('flag_type')}, crd: {e['crd']}, "
                f"description: {(e.get('description') or '')[:80]}"
            )

        scored_event = {
            "crd": e["crd"],
            "event_id": event_id,
            "event_type": e.get("flag_type"),
            "description": e.get("description"),
            "event_date": e.get("event_date"),
            "regulator": e.get("regulator"),
            "resolution": e.get("resolution"),
            "base_score": round(base, 2),
            "adjusted_score": round(adjusted, 2),
            "reasoning": reason,
            "scored_at": now,
            "scoring_version": SCORING_VERSION
        }
        rows.append(scored_event)
        scored_events.append(scored_event)

    if rows:
        for i in range(0, len(rows), BATCH_SIZE):
            chunk = rows[i:i+BATCH_SIZE]
            supabase.table("drp_event_scores").upsert(chunk, on_conflict=["event_id"]).execute()
            logging.info(f"✅ Wrote batch {i}–{i + len(chunk) - 1} to drp_event_scores")

    return scored_events
    

def insert_advisor_rollups(scored_events):
    now = datetime.utcnow().isoformat()
    by_crd = {}
    for e in scored_events:
        crd = e["crd"]
        adjusted = e.get("adjusted_score")
        if adjusted is None:
            _, adjusted, _ = score_drp_event(e)
        if crd not in by_crd:
            by_crd[crd] = []
        by_crd[crd].append(adjusted)

    rows = []
    max_score = 0
    max_crd = None

    for crd, scores in by_crd.items():
        average_score = round(sum(scores) / len(scores), 3)
        volume_penalty = 1 + 0.05 * (len(scores) - 1)
        volume_adjusted_score = min(round(average_score * volume_penalty, 3), 1.0)

        if volume_adjusted_score > max_score:
            max_score = volume_adjusted_score
            max_crd = crd

        rows.append({
            "crd": crd,
            "drp_score": average_score,
            "event_count": len(scores),
            "volume_adjusted_score": volume_adjusted_score,
            "last_scored_at": now,
            "scoring_version": SCORING_VERSION
        })

    if rows:
        supabase.table("advisor_drp_scores").upsert(rows, on_conflict=["crd"]).execute()
        logging.info(f"✅ Wrote {len(rows)} rollups to advisor_drp_scores")
        logging.info(f"🏆 Max volume-adjusted score: {max_score} (CRD: {max_crd})")

def get_all_drp_events():
    logging.info("📥 Fetching all DRP events from Supabase...")
    all_events = []
    last_id = ""

    while True:
        query = supabase.table("advisor_drp_events").select("*").order("id").limit(BATCH_SIZE)
        if last_id:
            query = query.gte("id", last_id)

        result = query.execute()
        batch = result.data or []

        # Skip the first record if we're continuing from a previous ID
        if last_id and batch and batch[0]["id"] == last_id:
            batch = batch[1:]

        if not batch:
            break

        all_events.extend(batch)
        last_id = batch[-1]["id"]
        logging.info(f"🔁 Fetched {len(batch)} events (Total: {len(all_events)})")

    logging.info(f"📊 Total DRP events fetched: {len(all_events)}")
    return all_events

# --- Main Function ---
from collections import Counter

def main(debug: bool = False):
    events = get_all_drp_events()
    if not events:
        logging.info("✅ No DRP events found to score.")
        return 0

    scored_events = insert_drp_event_scores(events)
    insert_advisor_rollups(scored_events)
    logging.info("🎯 DRP severity scoring complete.")

    if debug:
        base_score_by_label = Counter()
        for e in scored_events:
            base, _, _ = score_drp_event(e)
            raw_flag = e.get("flag_type") or e.get("event_type")
            label = FLAG_TYPE_MAP.get(raw_flag, "Unknown")
            base_score_by_label[(label, base)] += 1

        logging.info("🧪 Base Score Counts by Normalized Label:")
        for (label, base), count in base_score_by_label.items():
            logging.info(f"  {label}: base={base} count={count}")

        if unmapped_flag_counts:
            logging.info("⚠️ Summary of unmapped flag_types:")
            for flag, count in unmapped_flag_counts.items():
                logging.info(f"  {flag}: {count}")

        if unmapped_label_counts:
            logging.info("⚠️ Summary of unmapped labels:")
            for label, count in unmapped_label_counts.items():
                logging.info(f"  {label}: {count}")

        if low_score_label_counts:
            logging.info("⚠️ Labels scoring 0.4 by default:")
            for label, count in low_score_label_counts.items():
                logging.info(f"  {label}: {count}")

    return len(events)

if __name__ == "__main__":
    debug_flag = '--debug' in sys.argv
    inserted = main(debug=debug_flag)
    sys.exit(0 if inserted == 0 else 10)
