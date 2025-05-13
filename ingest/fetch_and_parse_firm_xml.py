import requests
import xml.etree.ElementTree as ET
import zipfile
import gzip
import io
from datetime import datetime

# Change this URL depending on whether you want to test .zip or .gz
FIRM_FEED_URL = "https://reports.adviserinfo.sec.gov/reports/CompilationReports/IA_FIRM_SEC_Feed_05_12_2025.xml.gz"
# Example GZ: https://files.adviserinfo.sec.gov/IAPD/IA_FIRM_SEC_Feed_05_13_2025.xml.gz

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

    for firm in root.findall(".//Firm"):
        crd = firm.findtext("FirmCRDNumber")
        name = firm.findtext("PrimaryBusinessName")
        filing_date = firm.findtext("FilingDate")
        filing_type = firm.findtext("FilingType", "").strip().upper()

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

    print("\n🔎 Preview of first 5 firms:")
    for firm in parsed_firms[:5]:
        print(f"{firm['CRD']}: {firm['Name']} | {firm['FilingType']} | {firm['FilingDate'].date()}")
