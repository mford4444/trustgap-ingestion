import requests
import xml.etree.ElementTree as ET
import zipfile
import io
from datetime import datetime

FIRM_FEED_URL = "https://www.sec.gov/files/FOIA/firm.zip"

def download_and_extract_xml(url):
    print("📥 Downloading firm XML ZIP...")
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")

    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    xml_filename = [f for f in zip_file.namelist() if f.endswith(".xml")][0]
    xml_content = zip_file.read(xml_filename)
    print(f"📄 Found XML file: {xml_filename}")
    return xml_content

def parse_firms(xml_content):
    print("🧠 Parsing XML content...")
    root = ET.fromstring(xml_content)
    firms = {}

    for firm in root.findall(".//Firm"):
        crd = firm.findtext("FirmCRDNumber")
        name = firm.findtext("PrimaryBusinessName")
        filing_date = firm.findtext("FilingDate")
        firm_type = firm.findtext("FilingType")

        if not crd or not filing_date:
            continue

        filing_dt = datetime.strptime(filing_date, "%Y-%m-%d")
        if crd not in firms or firms[crd]["FilingDate"] < filing_dt:
            firms[crd] = {
                "CRD": crd,
                "Name": name,
                "FilingType": firm_type,
                "FilingDate": filing_dt
            }

    print(f"✅ Total unique firms parsed: {len(firms)}")
    return list(firms.values())

if __name__ == "__main__":
    xml_data = download_and_extract_xml(FIRM_FEED_URL)
    parsed_firms = parse_firms(xml_data)

    print("\n🔎 Preview of first 5 firms:")
    for firm in parsed_firms[:5]:
        print(firm)
