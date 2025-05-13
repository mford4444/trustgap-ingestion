import os
from pyairtable import Table
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

print(f"✅ Loaded Airtable PAT? {'Yes' if AIRTABLE_PAT else 'No'}")

table = Table(AIRTABLE_PAT, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

def write_firms_to_airtable(firms):
    print(f"📤 Checking Airtable for existing CRDs before writing...")

    existing_crds = set()
    try:
        for record in table.all(fields=["CRD Number"]):
            crd = record.get("fields", {}).get("CRD Number")
            if crd:
                existing_crds.add(str(crd))
    except Exception as e:
        print(f"❌ Failed to fetch existing records from Airtable: {e}")
        return

    new_firms = [f for f in firms if str(f["CRD"]) not in existing_crds]

    print(f"🧮 New firms to write: {len(new_firms)}")

    for firm in new_firms:
        try:
            table.create({
                "Firm Name": firm["Name"],
                "CRD Number": firm["CRD"],
                "SEC Registration Type": firm["FilingType"],
                "FilingDate": firm["FilingDate"].strftime("%Y-%m-%d")
            })
        except Exception as e:
            print(f"❌ Failed to write CRD {firm['CRD']}: {e}")
