import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv
from storage.write_drp_events_to_supabase import write_drp_events_to_supabase

load_dotenv()

CHECKPOINT_FILE = "drp_checkpoint.json"
BATCH_SIZE = 100

friendly_names = {
    "hasRegAction": "Regulatory Action",
    "hasCriminal": "Criminal Disclosure",
    "hasBankrupt": "Bankruptcy",
    "hasCivilJudc": "Civil Judgment",
    "hasBond": "Bond",
    "hasJudgment": "Judgment Disclosure",
    "hasInvstgn": "Regulatory Investigation",
    "hasCustComp": "Customer Complaint",
    "hasTermination": "Employment Termination",
}

def get_feed_url():
    base_url = "https://reports.adviserinfo.sec.gov/reports/CompilationReports/IA_INDVL_Feed_{}.xml.zip"
    for offset in [0, 1]:
        date_str = (datetime.today() - timedelta(days=offset)).strftime("%m_%d_%Y")
        url = base_url.format(date_str)
        if requests.head(url).status_code == 200:
            print(f"✅ Feed found: {url}")
            return url
    raise Exception("❌ No valid feed found.")

def download_and_extract_xml(url):
    print("📥 Downloading feed...")
    r = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        return [z.read(name) for name in z.namelist() if name.endswith(".xml")]

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"last_crd": None}

def save_checkpoint(crd):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_crd": crd}, f)

def parse_drp_events(xml_contents, resume_from=None):
    print("🔍 Parsing DRP records...")
    drp_records = []
    today = datetime.utcnow().isoformat()
    processed = 0
    skipping = bool(resume_from)

    for xml in xml_contents:
        root = ET.fromstring(xml)
        for indvl in root.iter("Indvl"):
            crd = indvl.find("Info").attrib.get("indvlPK", "N/A")

            if skipping:
                if crd == resume_from:
                    skipping = False
                continue

            drps = indvl.find("DRPs")
            if not drps:
                continue

            for drp in drps.findall("DRP"):
                for flag, val in drp.attrib.items():
                    if val != "Y":
                        continue
                    record = {
                        "crd": crd,
                        "flag_type": flag,
                        "label": friendly_names.get(flag, flag),
                        "event_date": None,
                        "disposition": None,
                        "details": {},
                        "source": "XML",
                        "drp_url": f"https://adviserinfo.sec.gov/individual/summary/{crd}",
                        "created_at": today,
                    }
                    for child in drp:
                        if "date" in child.tag.lower():
                            record["event_date"] = child.text.strip()
                        elif "disposition" in child.tag.lower():
                            record["disposition"] = child.text.strip()
                        else:
                            record["details"][child.tag] = child.text.strip() if child.text else ""
                    drp_records.append(record)

            if len(drp_records) >= BATCH_SIZE:
                yield drp_records, crd
                drp_records = []

    if drp_records:
        yield drp_records, crd

if __name__ == "__main__":
    feed_url = get_feed_url()
    xml_files = download_and_extract_xml(feed_url)
    checkpoint = load_checkpoint()
    last_crd = checkpoint.get("last_crd")

    for batch, last_crd in parse_drp_events(xml_files, resume_from=last_crd):
        print(f"📤 Inserting batch with last CRD = {last_crd}...")
        write_drp_events_to_supabase(batch, batch_size=BATCH_SIZE)
        save_checkpoint(last_crd)
        print("✅ Checkpoint updated.")
