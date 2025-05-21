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
import math

# ✅ Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

DRY_RUN = False  # Set to False to enable DB write


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


def sanitize_floats(obj):
    if isinstance(obj, dict):
        return {k: sanitize_floats(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    return obj


def audit_field_completeness(records, fields):
    print("\n🔍 Field completeness audit:")
    total = len(records)
    for field in fields:
        count = sum(1 for r in records if r.get(field) not in [None, ""])
        percent = (count / total * 100) if total else 0
        print(f"  {field:<25}: {count}/{total} populated ({percent:.1f}%)")


def parse_firms(xml_content, registration_type):
    print("🧠 Parsing XML content...")
    root = ET.fromstring(xml_content)
    firms = []

    for firm in root.find("Firms").findall("Firm"):
        info = firm.find("Info")
        filing = firm.find("Filing")
        disclosure = firm.find("Disclosure")
        form = firm.find("FormInfo/Part1A")

        if info is None or filing is None:
            continue

        crd = info.attrib.get("FirmCrdNb")
        name = info.attrib.get("BusNm")
        filing_date = filing.attrib.get("Dt")

        if not crd or not filing_date:
            continue

        try:
            crd_int = int(crd)
            filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
        except ValueError:
            continue

        def safe_text(elem, attr):
            return elem.attrib.get(attr) if elem is not None and attr in elem.attrib else None

        def clean_float(value):
            try:
                return float(str(value).replace(",", "").replace("$", "").strip())
            except:
                return None

        def clean_int(value):
            try:
                return int(str(value).replace(",", "").strip())
            except:
                return None

        item5f = form.find("Item5F") if form is not None else None
        item5a = form.find("Item5A") if form is not None else None
        item5b = form.find("Item5B") if form is not None else None
        item6b = form.find("Item6B") if form is not None else None

        aum = clean_float(safe_text(item5f, "Q5F2C"))
        client_count = clean_int(safe_text(item5f, "Q5F2F"))
        employees = clean_int(safe_text(item5a, "TtlEmp"))
        advisors = clean_int(safe_text(item5b, "Q5B1"))

        address = firm.find("MainAddr")
        if address is None or not address.attrib.get("City"):
            address = firm.find("MailingAddr")

        city = address.attrib.get("City") if address is not None else None
        state = address.attrib.get("State") if address is not None else None
        zip_code = address.attrib.get("PostlCd") if address is not None else None

        q6b1 = safe_text(item6b, "Q6B1")
        dual_reg = (q6b1 == "Y") if q6b1 in ["Y", "N"] else "not reported"

        firm_drp_count = 0
        has_drp_flag = False
        if disclosure is not None:
            try:
                drps = disclosure.findall("DRP")
                firm_drp_count = len(drps)
                has_drp_flag = firm_drp_count > 0
            except:
                pass

        firm_data = {
            "crd_number": crd_int,
            "firm_name": name,
            "registration_type": registration_type,
            "adv_part2_url": None,
            "adv_part2_text": None,
            "disclosure_summary": None,
            "mentions_fiduciary": None,
            "mentions_fee_only": None,
            "filing_date": filing_dt,
            "total_regulatory_aum": aum,
            "total_employees": employees,
            "advisor_count": advisors,
            "client_count": client_count,
            "dual_registrant": dual_reg,
            "firm_drp_count": firm_drp_count,
            "has_drp_flag": has_drp_flag,
            "registration_year": filing_dt.year,
            "office_city": city,
            "office_state": state,
            "office_zip": zip_code,
            "office_country": None
        }

        firms.append(firm_data)

    print(f"✅ Parsed {len(firms)} unique firms with extended fields")
    return firms


def write_firms_to_supabase(firms):
    if DRY_RUN:
        print("🚫 Skipping DB write due to DRY_RUN mode")
        return

    df = pd.DataFrame(firms)
    df = df.drop_duplicates(subset=["crd_number"])
    df["filing_date"] = df["filing_date"].dt.strftime("%Y-%m-%d")
    records = [sanitize_floats(r) for r in df.to_dict(orient="records")]

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

        audit_field_completeness(
            new_or_updated,
            fields=[
                "total_regulatory_aum",
                "total_employees",
                "advisor_count",
                "client_count",
                "office_city",
                "office_state",
                "office_zip",
                "dual_registrant",
                "firm_drp_count",
                "has_drp_flag"
            ]
        )

        write_firms_to_supabase(new_or_updated)
        save_current_firms(parsed_firms)
