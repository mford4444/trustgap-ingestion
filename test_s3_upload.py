# test_s3_upload.py

import os
from storage.s3_upload import upload_pdf_to_s3
from dotenv import load_dotenv
import requests
import tempfile

load_dotenv()

# Test with a known CRD PDF
test_crd = "1394280"
test_url = f"https://reports.adviserinfo.sec.gov/reports/individual/individual_{test_crd}.pdf"
test_s3_key = f"adv_pdfs/{test_crd}_test.pdf"

# Download the file temporarily
print("📥 Downloading test PDF...")
response = requests.get(test_url)
response.raise_for_status()

with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
    tmp.write(response.content)
    tmp_path = tmp.name

# Upload to S3
print("☁️ Uploading to S3...")
s3_url = upload_pdf_to_s3(tmp_path, test_s3_key)

# Cleanup
os.remove(tmp_path)

# Output
if s3_url:
    print(f"✅ Success! File uploaded to: {s3_url}")
else:
    print("❌ Upload failed.")
