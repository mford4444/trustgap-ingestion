import os
import sys
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()  # ✅ This must run BEFORE os.getenv(...)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from storage.write_advisors_to_supabase import write_advisors_to_supabase

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY environment variable.")

def get_advisor_feed_url():
    base_url = "https://reports.adviserinfo.sec.gov/reports/CompilationReports/IA_INDVL_Feed_{}.xml.zip"
    for offset in [0, 1]:
        date_str = (datetime.today() - timedelta(days=offset)).strftime("%m_%d_%Y")
        url = base_url.format(date_str)
        print(f"\U0001F50E Checking availability for feed: {url}")
        response = requests.head(url)
        if response.status_code == 200:
            print(f"✅ Using feed URL: {url}")
            return url
        else:
            print(f"⚠️ Feed not found for {date_str}")
    raise Exception("❌ No valid advisor feed found for today or yesterday.")


def download_and_extract_xml_files(zip_url):
    print("\U0001F4E5 Downloading advisor ZIP feed...")
    response = requests.get(zip_url)
    if response.status_code != 200:
        raise Exception(f"Failed to download ZIP file: {response.status_code}")

    xml_files = []
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for name in z.namelist():
            if name.endswith(".xml"):
                print(f"📄 Extracting {name}")
                xml_files.append(z.read(name))
    return xml_files


def parse_advisors(xml_contents):
    print("\U0001F9E0 Parsing advisor records...")
    advisors = {}
    today_str = datetime.today().strftime("%Y-%m-%d")

    for xml_content in xml_contents:
        root = ET.fromstring(xml_content)
        for rep in root.iter("Indvl"):
            info = rep.find("Info")
            drps = rep.find("DRPs")
            crnt_emps = rep.find("CrntEmps")

            if info is None:
                continue

            crd = info.attrib.get("indvlPK")
            first = info.attrib.get("firstNm", "")
            last = info.attrib.get("lastNm", "")
            mid = info.attrib.get("midNm", "")
            suffix = info.attrib.get("sufNm", "")
            name = " ".join(part for part in [first, mid, last, suffix] if part)

            # Default status
            status = "Inactive"
            firm_crd = None
            firm_name = None

            if crnt_emps is not None:
                first_emp = crnt_emps.find("CrntEmp")
                if first_emp is not None:
                    firm_crd = first_emp.attrib.get("orgPK")
                    firm_name = first_emp.attrib.get("orgNm")
                    status = "Active"

            has_disclosures = False
            disclosure_count = 0
            if drps is not None:
                has_disclosures = any(
                    drps.attrib.get(k, "N") == "Y" for k in drps.attrib
                )
                disclosure_count = sum(
                    1 for k, v in drps.attrib.items() if v == "Y"
                )

            if not crd:
                continue

            advisors[crd] = {
                "CRD Number": crd,
                "Advisor Name": name,
                "Firm CRD Number": firm_crd,
                "Firm Name": firm_name,
                "Status": status,
                "Has Disclosures": has_disclosures,
                "Disclosures Count": disclosure_count,
                "Last Updated": today_str,
            }

    print(f"✅ Total unique advisors parsed: {len(advisors)}")
    return list(advisors.values())


if __name__ == "__main__":
    feed_url = get_advisor_feed_url()
    xml_files = download_and_extract_xml_files(feed_url)
    parsed_advisors = parse_advisors(xml_files)

    print(f"\n📤 Sending {len(parsed_advisors)} advisor records to Supabase...")
    write_advisors_to_supabase(parsed_advisors, batch_size=100, upsert_on="crd_number", resume_from_checkpoint=True)
