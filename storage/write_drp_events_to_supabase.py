import os
import requests
from time import sleep
from dotenv import load_dotenv

load_dotenv()

def write_drp_events_to_supabase(records, batch_size=100):
    print("📤 Writing DRP events to Supabase via REST...")
    total = len(records)
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    table_url = f"{supabase_url}/rest/v1/advisor_drp_events?on_conflict=crd,flag_type"

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]

        # Normalize all keys to lowercase to match Supabase schema
        batch = [{k.lower(): v for k, v in row.items()} for row in batch]

        print("👀 Sample record keys:", list(batch[0].keys()))
        print("👀 Sample record:", batch[0])

        try:
            response = requests.post(table_url, json=batch, headers=headers)
            if response.status_code in [201, 204]:
                print(f"✅ Batch {i // batch_size + 1}: Inserted or updated {len(batch)} records")
            else:
                print(f"❌ Batch {i // batch_size + 1}: {response.status_code} → {response.text}")
        except Exception as e:
            print(f"❌ Batch {i // batch_size + 1} request failed: {e}")

        sleep(0.25)  # Throttle requests slightly to avoid rate limits
