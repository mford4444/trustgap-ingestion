import requests, zipfile, io, xml.etree.ElementTree as ET
from collections import defaultdict

# Friendly label map
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

# Disclosure sections and their event tags
event_sections = {
    "CustomerComplaints": "CustomerComplaintEvent",
    "Criminals": "CriminalEvent",
    "RegulatoryActions": "RegulatoryActionEvent",
    "Bankruptcies": "BankruptcyEvent",
    "CivilJudgments": "CivilJudgmentEvent",
    "Bonds": "BondEvent",
    "Judgments": "JudgmentEvent",
    "Investigations": "InvestigationEvent",
    "Terminations": "TerminationEvent"
}

print("📥 Downloading advisor XML...")
url = "https://reports.adviserinfo.sec.gov/reports/CompilationReports/IA_INDVL_Feed_05_13_2025.xml.zip"
response = requests.get(url)
if response.status_code != 200:
    raise Exception("Failed to download feed")

print("📄 Extracting XML...")
xml_contents = []
with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    for name in z.namelist():
        if name.endswith(".xml"):
            xml_contents.append(z.read(name))

print("🔍 Parsing DRP flags + matching event data...")
sample_size = 1000
count = 0
drp_data = defaultdict(list)

for content in xml_contents:
    root = ET.fromstring(content)
    for rep in root.iter("Indvl"):
        info = rep.find("Info")
        crd = info.attrib.get("indvlPK", "N/A") if info is not None else "N/A"

        # Collect DRP flags
        drp_flags = {}
        drps = rep.find("DRPs")
        if drps:
            for drp in drps.findall("DRP"):
                for k, v in drp.attrib.items():
                    if v == "Y":
                        drp_flags[k] = {
                            "label": friendly_names.get(k, k),
                            "details": []
                        }

        # Cross-reference detailed events
        for section, event_tag in event_sections.items():
            container = rep.find(section)
            if container:
                for event in container.findall(event_tag):
                    event_details = {
                        el.tag: el.text.strip()
                        for el in event
                        if el.text and el.tag
                    }
                    if event_details:
                        flag_key = f"has{section[:-1]}" if section.endswith("s") else f"has{section}"
                        if flag_key in drp_flags:
                            drp_flags[flag_key]["details"].append(event_details)

        # Store for display
        for flag_key, data in drp_flags.items():
            drp_data[crd].append({
                "type": flag_key,
                "label": data["label"],
                "details": data["details"]
            })

        count += 1
        if count >= sample_size:
            break
    if count >= sample_size:
        break

# Print results
if not drp_data:
    print("❌ No DRP disclosures found.")
else:
    print(f"\n✅ Found disclosures for {len(drp_data)} advisors:\n")
    for crd, disclosures in drp_data.items():
        print(f"CRD {crd}")
        for d in disclosures:
            print(f"  → {d['label']} ({d['type']})")
            if d["details"]:
                for detail in d["details"]:
                    for k, v in detail.items():
                        print(f"     - {k}: {v}")
            else:
                print("     - No additional commentary")
        print()
