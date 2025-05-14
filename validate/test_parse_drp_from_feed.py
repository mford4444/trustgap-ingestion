import sys
import os
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from storage.write_drp_events_to_supabase import write_drp_events_to_supabase

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

friendly_names = {
    "hasRegAction": "Regulatory Action",
    "hasCriminal": "Criminal Disclosure",
    "hasBankrupt": "Bankruptcy",
    "hasCivilJudc": "Civil Judgment",
    "hasBond": "Bond",
    "hasJudgment": "Judgment Disclosure",
    "hasInvstgn": "Regulatory Investigation",
    "hasCustComp": "Customer Complaint",
    "hasTermination": "Employment Termination"
}

def get_advisor_feed_url():
    base_url = "https://reports.adviserinfo.sec.gov/reports/CompilationReports/IA_INDVL_Feed_{}.xml.zip"
    for offset in [0, 1]:
        date_str = (datetime.today() - timedelta(days=offset)).strftime("%m_%d_%Y")
        url = base_url.format(date_str)
        print(f"🔍 Checking: {url}")
        if requests.head(url).status_code == 200:
            print(f"✅ Using: {url}")
            return url
    raise Exception("❌ No valid SEC feed found for today or yesterday.")

def download_and_extract_xml(zip_url):
    print("📥 Downloading feed...")
    response = requests.get(zip_url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        return [z.read(name) for name in z.namelist() if name.endswith(".xml")]

def parse_drp_events(xml_contents, limit=10):
    print("🔍 Parsing DRP entries...")
    drp_records = []
    today = datetime.utcnow().isoformat()

    for xml in xml_contents:
        root = ET.fromstring(xml)
        for indvl in root.iter("Indvl"):
            crd = indvl.find("Info").attrib.get("indvlPK", "N/A")
            drp_url = f"https://adviserinfo.sec.gov/individual/summary/{crd}"
            drps = indvl.find("DRPs")
            if not drps:
                continue

            for drp in drps.findall("DRP"):
                for flag, v in drp.attrib.items():
                    if v != "Y":
                        continue
                    record = {
                        "crd": crd,
                        "flag_type": flag,
                        "label": friendly_names.get(flag, flag),
                        "event_date": None,
                        "disposition": None,
                        "details": {},
                        "source": "XML",
                        "drp_url": drp_url,
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

            if len(drp_records) >= limit:
                return drp_records[:limit]
    return drp_records

if __name__ == "__main__":
    feed_url = get_advisor_feed_url()
    xml_files = download_and_extract_xml(feed_url)
    test_batch = parse_drp_events(xml_files, limit=10)

    print(f"\n🧪 Previewing {len(test_batch)} DRP entries:")
    for entry in test_batch:
        print(f"CRD: {entry['crd']} | Type: {entry['label']} | URL: {entry['drp_url']}")

    print("\n📤 Sending test records to Supabase...")
    write_drp_events_to_supabase(test_batch, batch_size=10)
