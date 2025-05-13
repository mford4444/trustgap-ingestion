import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import requests
import xml.etree.ElementTree as ET
import zipfile
import gzip
import io
from datetime import datetime

from storage.firm_cache import load_previous_firms, save_current_firms

# Change this URL depending on whether you want to test .zip or .gz
FIRM_FEED_URL = "https://reports.adviserinfo.sec.gov/reports/CompilationReports/IA_FIRM_SEC_Feed_05_13_2025.xml.gz"

def download_and_extract_xml(url):
    print("📥 Downloading XML from:", url)
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")

    content_type = response.headers.get("Content-Type", "")
    is_gzip = url.endswith(".gz") or "gzip" in content_type.lower()
    is_zip = url.endswith(".zip") or "zip" in content_type.lower()

    if is_zip:
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        xml_filename = [f for f in zip_file.namelist() if f.endswith(".xml")][0]
        xml_content = zip_file.read(xml_filename)
        print(f"📄 Extracted XML from ZIP: {xml_filename}")
        return xml_content

    elif is_gzip:
        print("📄 Extracting XML from GZIP")
        xml_content = gzip.decompress(response.content)
        return xml_content

    else:
        print("📄 Treating file as plain XML")
        return response.content

def parse_firms(xml_content):
    print("🧠 Parsing XML content...")
    root = ET.fromstring(xml_content)
    firms = {}

    firm_elements = root.find("Firms")
    if firm_elements is None:
        print("❌ Could not find <Firms> in XML.")
        return []

    for firm in firm_elements.findall("Firm"):
        info = firm.find("Info")
        filing = firm.find("Filing")

        if info is None or filing is None:
            continue

        crd = info.attrib.get("FirmCrdNb")
        name = info.attrib.get("BusNm")
        filing_date = filing.attrib.get("Dt")
        filing_type = "SEC"  # This feed is SEC-only, but you could add logic later

        if not crd or not filing_date:
            continue

        try:
            filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
        except ValueError:
            continue

        if crd not in firms or firms[crd]["FilingDate"] < filing_dt:
            firms[crd] = {
                "CRD": crd,
                "Name": name,
                "FilingType": filing_type,
                "FilingDate": filing_dt
            }

    print(f"✅ Total unique firms parsed: {len(firms)}")
    return list(firms.values())

if __name__ == "__main__":
    xml_data = download_and_extract_xml(FIRM_FEED_URL)
    parsed_firms = parse_firms(xml_data)

    print("\n📊 Comparing to previous run...")
    previous_firms = load_previous_firms()
    new_or_updated = []

    for firm in parsed_firms:
        crd = firm["CRD"]
        filing_date = firm["FilingDate"].strftime("%Y-%m-%d")
        prev_date = previous_firms.get(crd)

        if prev_date is None or filing_date > prev_date:
            new_or_updated.append(firm)

    print(f"🔍 New or updated firms: {len(new_or_updated)}")
    for firm in new_or_updated[:5]:
        print(f"{firm['CRD']}: {firm['Name']} | {firm['FilingDate'].date()}")

    # Save current run for tomorrow's comparison
    save_current_firms(parsed_firms)

