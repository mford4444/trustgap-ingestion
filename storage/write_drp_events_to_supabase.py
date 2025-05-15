import os
from supabase import create_client, Client
from time import sleep
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY environment variable.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def write_drp_events_to_supabase(records, batch_size=100):
    print("📤 Writing DRP events to Supabase...")
    total = len(records)
    for i in range(0, total, batch_size):
    batch = records[i:i + batch_size]

    # Normalize keys
    batch = [{k.lower(): v for k, v in row.items()} for row in batch]

    # Optional: debug output
    print("👀 Sample record keys:", list(batch[0].keys()))
    print("👀 Sample record:", batch[0])

    try:
        # ✅ No on_conflict, uses DB constraints
        response = supabase.table("advisor_drp_events").upsert(batch).execute()

        if hasattr(response, 'data'):
            print(f"✅ Batch {i // batch_size + 1}: Inserted {len(response.data)} records")
        else:
            print(f"⚠️ Batch {i // batch_size + 1}: Inserted with unknown response")

    except Exception as e:
        print(f"❌ Error inserting batch {i // batch_size + 1}: {e}")

    sleep(0.25)

