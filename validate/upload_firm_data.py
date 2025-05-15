import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import os

# ✅ Load environment variables
load_dotenv(dotenv_path="/Users/markford/Documents/Trustgap/trustgap-ingestion/validate/.env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
print("Key loaded:", SUPABASE_KEY[:20], "...")  # Safe preview

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")

# ✅ Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ Load CSV
df = pd.read_csv("firm_data.csv")

# ✅ Standardize column names and fill in missing fields
df = df.rename(columns={
    "Record ID": "airtable_id",
    "CRD Number": "crd_number",
    "Firm Name": "firm_name",
    "SEC Registration Type": "registration_type"
})

df["adv_part2_url"] = None
df["adv_part2_text"] = None
df["disclosure_summary"] = None
df["mentions_fiduciary"] = None
df["mentions_fee_only"] = None

# ✅ Clean data
df = df.dropna(subset=["crd_number"])
df["crd_number"] = pd.to_numeric(df["crd_number"], errors="coerce").astype("Int64")

# ✅ Final column order
df = df[[
    "crd_number",
    "firm_name",
    "registration_type",
    "adv_part2_url",
    "adv_part2_text",
    "disclosure_summary",
    "mentions_fiduciary",
    "mentions_fee_only"
]]

# ✅ Drop rows with missing CRD and ensure numeric type
df = df.dropna(subset=["crd_number"])
df["crd_number"] = pd.to_numeric(df["crd_number"], errors="coerce").astype("Int64")

# ✅ Drop duplicate CRD rows (required for Supabase upsert)
df = df.drop_duplicates(subset=["crd_number"])

# ✅ Convert to records
records = df.to_dict(orient="records")
print(f"Total records prepared: {len(records)}")  # Optional: debug log

# ✅ Upload in batches
BATCH_SIZE = 50
for i in range(0, len(records), BATCH_SIZE):
    batch = records[i:i+BATCH_SIZE]
    res = supabase.table("firm_data").upsert(batch, on_conflict=["crd_number"]).execute()
    print(f"Uploaded {len(batch)} records: {i}–{i+len(batch)-1}")

