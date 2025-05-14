import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

CHECKPOINT_FILE = "checkpoint.json"

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_checkpoint(processed_crds):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(processed_crds), f)

def write_advisors_to_supabase(advisors, batch_size=100, upsert_on=None, resume_from_checkpoint=False):
    print(f"\U0001F680 Uploading {len(advisors)} advisors to Supabase...")

    processed_crds = load_checkpoint() if resume_from_checkpoint else set()
    total_written = 0

    for i in range(0, len(advisors), batch_size):
        batch = advisors[i:i + batch_size]

        if resume_from_checkpoint:
            batch = [a for a in batch if a["CRD Number"] not in processed_crds]
            if not batch:
                continue

        payload = []
        for a in batch:
            payload.append({
                "crd_number": a["CRD Number"],
                "advisor_name": a["Advisor Name"],
                "firm_crd_number": a["Firm CRD Number"],
                "firm_name": a["Firm Name"],
                "status": a["Status"],
                "has_disclosures": a["Has Disclosures"],
                "disclosures_count": a["Disclosures Count"],
                "last_updated": a["Last Updated"]
            })

        try:
            url = f"{SUPABASE_URL}/rest/v1/advisors"
            if upsert_on:
                url += f"?on_conflict={upsert_on}"

            resp = requests.post(url, headers=HEADERS, json=payload)

            # If batch fails, fall back to writing individually
            if resp.status_code == 409 and upsert_on:
                print(f"⚠️ Batch conflict on upsert — retrying individually...")
                for advisor in payload:
                    try:
                        single_resp = requests.post(url, headers=HEADERS, json=[advisor])
                        if single_resp.status_code in [200, 201]:
                            total_written += 1
                            if resume_from_checkpoint:
                                processed_crds.add(advisor["crd_number"])
                        else:
                            print(f"❌ Advisor {advisor['crd_number']} failed: {single_resp.status_code}")
                    except Exception as e:
                        print(f"❌ Error posting individual advisor {advisor['crd_number']}: {e}")

            
                print(f"❌ Failed to write batch {i // batch_size}: {resp.status_code} - {resp.text}")
            else:
                print(f"✅ Wrote batch {i // batch_size + 1} ({len(batch)} records)")
                total_written += len(batch)
                if resume_from_checkpoint:
                    processed_crds.update([a["CRD Number"] for a in batch])
                    save_checkpoint(processed_crds)
        except Exception as e:
            print(f"❌ Error posting batch {i // batch_size}: {e}")

    print(f"\n✅ Total written to Supabase: {total_written}")
