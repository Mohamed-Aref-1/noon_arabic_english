"""
Email Notifier
==============
Sends a summary email after scraping completes using Gmail SMTP.
Credentials are loaded from .env (NOTIFY_EMAIL_SENDER / NOTIFY_EMAIL_PASSWORD / NOTIFY_EMAIL_RECIPIENT).
"""

import os
import smtplib
import socket
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Dict

from config import Config


def _get_machine_ip() -> str:
    """Return the public or local IP of this machine."""
    # Try public IP first
    try:
        import urllib.request
        with urllib.request.urlopen('https://api.ipify.org', timeout=5) as resp:
            return resp.read().decode('utf-8').strip()
    except Exception:
        pass
    # Fallback to local IP
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        return 'unknown'


def _build_email_body(
    scrape_results: List[Dict],
    products_processed: int,
    total_records_scrapped: int,
    s3_upload_success: bool,
    s3_path: str,
    machine_ip: str,
    duration_str: str,
) -> str:
    """Build a plain-text email body with the scraping summary."""
    lines = [
        "=" * 60,
        "NOON SCRAPER — RUN SUMMARY",
        "=" * 60,
        "",
        f"Machine IP   : {machine_ip}",
        f"Duration     : {duration_str}",
        f"Output file  : {Config.OUTPUT_FILENAME}.jsonl",
        "",
        "─" * 60,
        "CATEGORY RESULTS",
        "─" * 60,
    ]

    successful = [r for r in scrape_results if r.get('success')]
    failed = [r for r in scrape_results if not r.get('success')]

    for i, r in enumerate(successful, start=1):
        url = r.get('source_url', '')
        records = r.get('number_of_records', 0)
        skipped = ' (already complete — resumed)' if r.get('skipped') else ''
        lines.append(f"  {i:>3}. {url}")
        lines.append(f"       Records: {records:,}{skipped}")

    if failed:
        lines.append("")
        lines.append(f"FAILED ({len(failed)}):")
        for r in failed:
            lines.append(f"  - {r.get('source_url', '')}")

    lines += [
        "",
        "─" * 60,
        "PRODUCT DETAILS",
        "─" * 60,
        f"  Products processed : {products_processed:,}",
        f"  Total records      : {total_records_scrapped:,}",
        "",
        "─" * 60,
        "S3 UPLOAD",
        "─" * 60,
    ]

    if s3_upload_success:
        lines.append(f"  Status : SUCCESS")
        lines.append(f"  Path   : {s3_path}")
    else:
        lines.append(f"  Status : SKIPPED or FAILED")
        lines.append(f"  Path   : {s3_path}")

    lines += [
        "",
        "=" * 60,
        "End of report.",
    ]

    return "\n".join(lines)


def send_summary_email(
    scrape_results: List[Dict],
    products_processed: int,
    total_records_scrapped: int,
    s3_upload_success: bool,
    duration_str: str,
) -> bool:
    """
    Send a summary email using Gmail SMTP.
    Returns True on success, False on failure.
    """
    sender = Config.NOTIFY_EMAIL_SENDER
    password = Config.NOTIFY_EMAIL_PASSWORD
    recipient = Config.NOTIFY_EMAIL_RECIPIENT

    if not sender or not password or not recipient:
        print("Email notification skipped — NOTIFY_EMAIL_SENDER / NOTIFY_EMAIL_PASSWORD / NOTIFY_EMAIL_RECIPIENT not set in .env")
        return False

    machine_ip = _get_machine_ip()
    s3_folder = Config.S3_FOLDER
    filename = f"{Config.OUTPUT_FILENAME}.jsonl"
    s3_path = f"s3://{Config.S3_BUCKET_NAME}/{s3_folder}/{filename}" if s3_folder else f"s3://{Config.S3_BUCKET_NAME}/{filename}"

    body = _build_email_body(
        scrape_results=scrape_results,
        products_processed=products_processed,
        total_records_scrapped=total_records_scrapped,
        s3_upload_success=s3_upload_success,
        s3_path=s3_path,
        machine_ip=machine_ip,
        duration_str=duration_str,
    )

    subject = f"[Noon Scraper] Run complete — {len([r for r in scrape_results if r.get('success')])} categories | {total_records_scrapped:,} products | IP: {machine_ip}"

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        print(f"\nSending summary email to {recipient} ...")
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
        print(f"Email sent successfully.")
        return True
    except Exception as e:
        print(f"Email sending failed: {e}")
        return False
