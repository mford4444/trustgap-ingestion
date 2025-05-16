from supabase import create_client
from datetime import datetime
from tqdm import tqdm
import logging
import os
import requests
import tempfile
import time
from dotenv import load_dotenv
from storage.s3_upload import upload_pdf_to_s3

# --- Load environment variables ---
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Constants ---
BATCH_SIZE = 100
MAX_BATCHES = None  # Run all
REQUEST_DELAY = 0.5
CRD_LOAD_BATCH_SIZE = 1000
CRD_LOAD_MAX = None  # Remove limit for prod

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

def get_crds_from_advisors(offset: int, limit: int):
    result = supabase.table("advisors") \
        .select("crd_number") \
        .range(offset, offset + limit - 1) \
        .execute()
    return [row["crd_number"] for row in result.data if row.get("crd_number")]

def insert_adv_records(crd_list):
    now = datetime.utcnow().isoformat()
    records = []

    for crd in tqdm(crd_list, desc="Uploading PDFs to S3"):
        url = generate_adv_url(crd)
        try:
            headers = {"User-Agent": "TrustGapBot/1.0"}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
        except Exception as e:
            logging.warning(f"⚠️ Failed to download ADV for CRD {crd}: {e}")
            continue

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
            tmp_file.write(response.content)
            tmp_file_path = tmp_file.name

        s3_key = f"adv_pdfs/{crd}.pdf"
        s3_url = upload_pdf_to_s3(tmp_file_path, s3_key)

        if not s3_url:
            logging.warning(f"⚠️ Skipping CRD {crd} — S3 upload failed")
            continue

        records.append({
            "crd": crd,
            "adv_url": s3_url,
            "last_fetched_at": now
        })

        os.remove(tmp_file_path)
        time.sleep(REQUEST_DELAY)

    if records:
        supabase.table("advisor_advs").insert(records).execute()
        logging.info(f"✅ Inserted {len(records)} records with S3 URLs.")

# --- Main Function ---
def main():
    logging.info("📥 Loading all existing ADV CRDs from Supabase...")
    all_existing_crds = set()
    offset = 0
    total_loaded = 0

    while True:
        result = supabase.table("advisor_advs") \
            .select("crd") \
            .range(offset, offset + CRD_LOAD_BATCH_SIZE - 1) \
            .execute()

        if not result.data:
            break

        all_existing_crds.update(row["crd"] for row in result.data if row.get("crd"))
        offset += CRD_LOAD_BATCH_SIZE
        total_loaded += len(result.data)

        if CRD_LOAD_MAX and total_loaded >= CRD_LOAD_MAX:
            logging.info(f"🧪 CRD load capped at {CRD_LOAD_MAX} for test")
            break

    logging.info(f"🧮 Loaded {len(all_existing_crds):,} existing CRDs from advisor_advs table")

    offset = 0
    inserted_total = 0
    batch_number = 1

    while True:
        crd_batch = get_crds_from_advisors(offset, BATCH_SIZE)
        if not crd_batch:
            break

        crds_to_insert = [crd for crd in crd_batch if crd not in all_existing_crds]
        logging.info(f"📦 Batch {batch_number}: Read {len(crd_batch)} | New: {len(crds_to_insert)}")

        if crds_to_insert:
            insert_adv_records(crds_to_insert)
            inserted_total += len(crds_to_insert)

        offset += BATCH_SIZE
        batch_number += 1

        if MAX_BATCHES is not None and batch_number > MAX_BATCHES:
            logging.info("⏹️ Max batch limit reached. Stopping.")
            break

    logging.info(f"✅ ADV sync complete. Inserted {inserted_total} new records.")
    return inserted_total

import sys

if __name__ == "__main__":
    inserted = main()
    sys.exit(0 if inserted == 0 else 10)
