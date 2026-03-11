"""
S3 Uploader
===========
Uploads the final output file to S3.
Credentials are loaded from .env (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
or from a credentials CSV (AWS_CREDENTIALS_CSV path in .env).
"""

import os
from datetime import datetime
import pandas as pd
import boto3
from botocore.exceptions import ClientError

from config import Config


def _get_credentials():
    """Return (access_key_id, secret_access_key) from CSV or .env."""
    csv_path = Config.AWS_CREDENTIALS_CSV
    if csv_path and os.path.exists(csv_path):
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        return df['Access key ID'].iloc[0], df['Secret access key'].iloc[0]
    return Config.AWS_ACCESS_KEY_ID, Config.AWS_SECRET_ACCESS_KEY


def upload_to_s3(file_path: str, object_name: str = None) -> bool:
    """
    Upload a file to the S3 bucket configured in .env.
    Returns True on success, False on failure.
    """
    if not os.path.exists(file_path):
        print(f"S3 upload skipped — file not found: {file_path}")
        return False

    access_key, secret_key = _get_credentials()
    if not access_key or not secret_key:
        print("S3 upload skipped — no AWS credentials configured in .env")
        return False

    if object_name is None:
        filename = os.path.basename(file_path)
        name, ext = os.path.splitext(filename)
        ts = datetime.now().strftime("%Y_%m_%d_%H_%-M_%S")
        filename = f"{name}_({ts}){ext}"
        object_name = f"{Config.S3_FOLDER}/{filename}" if Config.S3_FOLDER else filename

    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=Config.S3_REGION,
    )

    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"\nUploading {file_size_mb:.1f} MB to s3://{Config.S3_BUCKET_NAME}/{object_name} ...")

    try:
        s3.upload_file(file_path, Config.S3_BUCKET_NAME, object_name)
        print(f"Upload complete: s3://{Config.S3_BUCKET_NAME}/{object_name}")
        return True
    except ClientError as e:
        print(f"Upload failed: {e}")
        return False
