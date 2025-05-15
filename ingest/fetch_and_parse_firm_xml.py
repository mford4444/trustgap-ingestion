import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from storage.firm_cache import load_previous_firms, save_current_firms
from supabase import create_client, Client
from dotenv import load_dotenv
import requests
import xml.etree.ElementTree as ET
import zipfile
import gzip
import io
from datetime import datetime, timedelta
import pandas as pd

# ✅ Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_firm_feed_url(feed_type="SEC"):
    base_url = f"https://reports.adviserinfo.sec.gov/reports/CompilationReports/IA_FIRM_{feed_type}_Feed_{{}}.xml.gz"
    for offset in [0, 1]:
        date_str = (datetime.today() - timedelta(days=offset)).strftime("%m_%d_%Y")
        url = base_url.format(date_str)
        print(f"🔎 Checking availability for feed: {url}")
        response = requests.head(url)
        if response.status_code == 200:
            print(f"✅ Using feed URL: {url}")
            return url
    raise Exception("❌ No valid firm feed found for today or yesterday.")

def download_and_extract_xml(url):
    print("📥 Downloading XML from:", url)
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")

    if url.endswith(".gz"):
        print("📄 Extracting XML from GZIP")
        return gzip.decompress(response.content)
    elif url.endswith(".zip"):
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        xml_filename = [f for f in zip_file.namelist() if f.endswith(".xml")][0]
        print(f"📄 Extracted XML from ZIP: {xml_filename}")
        return zip_file.read(xml_filename)
    else:
        return response.content

def parse_firms(xml_content, registration_type):
    print("🧠 Parsing XML content...")
    root = ET.fromstring(xml_content)
    firms = {}
    for firm in root.find("Firms").findall("Firm"):
        info = firm.find("Info")
        filing = firm.find("Filing")
        if info is None or filing is None:
            continue
        crd = info.attrib.get("FirmCrdNb")
        name = info.attrib.get("BusNm")
        filing_date = filing.attrib.get("Dt")
        if not crd or not filing_date:
            continue
        try:
            filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
        except ValueError:
            continue
        if crd not in firms or firms[crd]["FilingDate"] < filing_dt:
            firms[crd] = {
                "crd_number": int(crd),
                "firm_name": name,
                "registration_type": registration_type,
                "adv_part2_url": None,
                "adv_part2_text": None,
                "disclosure_summary": None,
                "mentions_fiduciary": None,
                "mentions_fee_only": None,
                "filing_date": filing_dt
            }
    print(f"✅ Parsed {len(firms)} unique firms")
    return list(firms.values())

def write_firms_to_supabase(firms):
    df = pd.DataFrame(firms)
    df = df.drop_duplicates(subset=["crd_number"])
    records = df.to_dict(orient="records")
    BATCH_SIZE = 50
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        supabase.table("firm_data").upsert(batch, on_conflict=["crd_number"]).execute()
        print(f"⬆️ Uploaded {len(batch)} firms")

if __name__ == "__main__":
    for feed_type in ["SEC", "STATE"]:
        print(f"\n🚀 Ingesting {feed_type} firm feed")
        FIRM_FEED_URL = get_firm_feed_url(feed_type=feed_type)
        xml_data = download_and_extract_xml(FIRM_FEED_URL)
        parsed_firms = parse_firms(xml_data, registration_type=feed_type)

        previous_firms = load_previous_firms()
        new_or_updated = []
        for firm in parsed_firms:
            crd = str(firm["crd_number"])
            filing_date = firm["filing_date"].strftime("%Y-%m-%d")
            prev_date = previous_firms.get(crd)
            if prev_date is None or filing_date > prev_date:
                new_or_updated.append(firm)

        print(f"📌 New or updated firms: {len(new_or_updated)}")
        write_firms_to_supabase(new_or_updated)
        save_current_firms(parsed_firms)
