import os
from supabase import create_client, Client
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print("📊 Fetching DRP data from Supabase...")

# Step 1: Fetch all advisor DRP events
response = supabase.table("advisor_drp_events").select("crd").execute()
if not hasattr(response, "data") or not response.data:
    print("❌ No DRP data returned.")
    exit()

# Step 2: Count DRPs per CRD
crd_counts = defaultdict(int)
for row in response.data:
    crd = row.get("crd")
    if crd:
        crd_counts[crd] += 1

print(f"✅ Found {len(crd_counts)} advisors with at least one DRP.")

# Step 3: Prepare batch updates for advisor table
updates = []
for crd, count in crd_counts.items():
    updates.append({
        "crd_number": crd,
        "has_disclosures": True,
        "disclosures_count": count
    })

print(f"📤 Updating {len(updates)} advisor records...")

# Step 4: Perform updates (not upserts)
batch_size = 100
for i in range(0, len(updates), batch_size):
    batch = updates[i:i + batch_size]
    print(f"📦 Processing batch {i // batch_size + 1} with {len(batch)} records")
    for row in batch:
        try:
            supabase.table("advisors").update({
                "has_disclosures": row["has_disclosures"],
                "disclosures_count": row["disclosures_count"]
            }).eq("crd_number", row["crd_number"]).execute()
        except Exception as e:
            print(f"❌ Update failed for CRD {row['crd_number']}: {e}")

print("🎉 Advisor disclosure flags updated successfully.")
