# storage/s3_upload.py

import os
import boto3
import logging
from dotenv import load_dotenv
from botocore.exceptions import BotoCoreError, ClientError

# Load environment
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

if not S3_BUCKET:
    raise ValueError("Missing S3_BUCKET_NAME in environment")

# Create S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

def upload_pdf_to_s3(local_path, s3_key):
    try:
        s3.upload_file(
            Filename=local_path,
            Bucket=S3_BUCKET,
            Key=s3_key,
            ExtraArgs={"ContentType": "application/pdf"},
        )
        s3_url = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        logging.info(f"✅ Uploaded to S3: {s3_url}")
        return s3_url
    except (BotoCoreError, ClientError) as e:
        logging.error(f"❌ S3 upload failed for {s3_key}: {e}")
        return None
