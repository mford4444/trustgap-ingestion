from supabase import create_client
from datetime import datetime
from tqdm import tqdm
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# ----------------- Config -----------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BATCH_SIZE = 100
MAX_BATCHES = None  # e.g. set to 10 for testing; None for full run

# ----------------- Logging -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("populate_advisor_advs.log"),
        logging.StreamHandler()
    ]
)

# ----------------- Helpers -----------------

def generate_adv_url(crd: str) -> str:
    return f"https://reports.adviserinfo.sec.gov/reports/individual/individual_{crd}.pdf"

def get_crds_from_advisors(offset: int, limit: int):
    result = supabase.table("advisors") \
        .select("crd_number") \
        .range(offset, offset + limit - 1) \
        .execute()
    return [row["crd_number"] for row in result.data if row.get("crd_number")]

def get_existing_adv_crds(crd_list):
    if not crd_list:
        return set()
    result = supabase.table("advisor_advs") \
        .select("crd") \
        .in_("crd", crd_list) \
        .execute()
    return {row["crd"] for row in result.data if row.get("crd")}

def insert_adv_records(crd_list):
    now = datetime.utcnow().isoformat()
    records = [{
        "crd": crd,
        "adv_url": generate_adv_url(crd),
        "last_fetched_at": now
    } for crd in crd_list]

    if records:
        supabase.table("advisor_advs").insert(records).execute()

# ----------------- Main -----------------

def main():
    offset = 0
    batch_number = 1

    while True:
        crd_batch = get_crds_from_advisors(offset, BATCH_SIZE)
        if not crd_batch:
            logging.info("🚫 No more CRDs found. Exiting.")
            break

        logging.info(f"📦 Processing Batch {batch_number} — CRDs {offset}–{offset + BATCH_SIZE - 1}")

        existing_crds = get_existing_adv_crds(crd_batch)
        crds_to_insert = [crd for crd in crd_batch if crd not in existing_crds]

        logging.info(f"🔍 Total: {len(crd_batch)} | Already exist: {len(existing_crds)} | New: {len(crds_to_insert)}")

        if crds_to_insert:
            insert_adv_records(crds_to_insert)
            logging.info(f"✅ Inserted {len(crds_to_insert)} ADV records.")

        # Safety limit
        if MAX_BATCHES is not None and batch_number >= MAX_BATCHES:
            logging.info("⏹️ Max batch limit reached. Stopping.")
            break

        offset += BATCH_SIZE
        batch_number += 1

if __name__ == "__main__":
    main()
