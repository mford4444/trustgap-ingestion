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

def write_advisors_to_airtable(advisors):
    from pyairtable import Table
    import os
    from dotenv import load_dotenv
    load_dotenv()

    AIRTABLE_PAT = os.getenv("AIRTABLE_PAT")
    AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
    table = Table(AIRTABLE_PAT, AIRTABLE_BASE_ID, "Advisor Data")

    print(f"🧮 Writing {len(advisors)} advisors to Airtable...")

    existing_crds = set()
    try:
        for record in table.all(fields=["CRD Number"]):
            crd = record.get("fields", {}).get("CRD Number")
            if crd:
                existing_crds.add(str(crd))
    except Exception as e:
        print(f"❌ Failed to fetch existing advisors: {e}")
        return

    new_advisors = [a for a in advisors if str(a["CRD Number"]) not in existing_crds]
    print(f"🧮 New advisors to write: {len(new_advisors)}")

    for advisor in new_advisors:
        try:
            table.create({
                "CRD Number": advisor["CRD Number"],
                "Advisor Name": advisor["Advisor Name"],
                "Firm CRD Number": advisor["Firm CRD Number"],
                "Firm Name": advisor["Firm Name"],
                "Status": advisor["Status"],
                "Has Disclosures": advisor["Has Disclosures"],
                "Disclosures Count": advisor["Disclosures Count"],
                "Last Updated": advisor["Last Updated"]
            })

        except Exception as e:
            print(f"❌ Failed to write CRD {advisor['CRD Number']}: {e}")
