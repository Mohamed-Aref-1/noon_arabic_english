#!/usr/bin/env python3
"""
Standalone Product Scraper
==========================
Reads category CSV files and fetches full product details from the API.

Input: CSV files from noon_category_dedup_all/
Output: CSV files with product details in product_only/
"""

import os
import sys
import csv
import json
import time
import random
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

try:
    from curl_cffi import requests
except ImportError:
    print("Error: curl_cffi not installed. Run: pip install curl_cffi")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Optional

# ============================================================================
# CONFIGURATION
# ============================================================================

INPUT_FOLDER = "/media/mohamed/5C1C30381C300F8E/scrapping_noon_project/noon_category_dedup_feb_2_1"
OUTPUT_FOLDER = "/media/mohamed/5C1C30381C300F8E/scrapping_noon_project/product_only_feb_2_3"

# API Settings
BASE_API_URL = "https://www.noon.com/_vs/nc/mp-customer-catalog-api/api/v3/u/"
BASE_SITE_URL = "https://www.noon.com/uae-en/"  

# Request Settings
LOCALE = os.getenv('LOCALE', 'en-ae')
COUNTRY = os.getenv('COUNTRY', 'ae')
VISITOR_ID = os.getenv('VISITOR_ID', '8b76a0b9-1549-483d-b87b-2982d89f75a1')
CHROME_IMPERSONATE = 'chrome120'
REQUEST_TIMEOUT = 15
DELAY_MIN = 1.5
DELAY_MAX = 3.0

# ============================================================================
# LOGGING SETUP
# ============================================================================

LOG_FILE = f'product_only_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE)
    ]
)
logger = logging.getLogger(__name__)

# Print to confirm log file location (only message to terminal)
print(f"Logging to: {LOG_FILE}")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def image_key_to_url(image_key: str) -> Optional[str]:
    """Convert noon image key to full URL."""
    if not image_key:
        return None
    return f"https://f.nooncdn.com/p/{image_key}.jpg"


def random_delay():
    """Add random delay between requests."""
    delay = random.uniform(DELAY_MIN, DELAY_MAX)
    time.sleep(delay)


# ============================================================================
# PRODUCT SCRAPER CLASS
# ============================================================================

class ProductScraper:
    """Fetches product details from noon API."""

    def __init__(self):
        self.session = requests.Session()
        self.success_count = 0
        self.fail_count = 0

    def get_product(self, url_slug: str, sku: str) -> Optional[Dict]:
        """Fetch product details by URL slug and SKU."""
        url_slug = url_slug.lstrip('/')
        page_url = f'{BASE_SITE_URL}{url_slug}/{sku}/p/'

        # Visit page first to establish session
        try:
            self.session.get(page_url, impersonate=CHROME_IMPERSONATE, timeout=REQUEST_TIMEOUT)
        except Exception:
            pass

        headers = {
            'accept': 'application/json',
            'x-locale': LOCALE,
            'x-mp-country': COUNTRY,
            'referer': page_url,
            'x-visitor-id': VISITOR_ID,
        }

        api_url = f'{BASE_API_URL}{url_slug}/{sku}/p/'

        try:
            response = self.session.get(
                api_url,
                headers=headers,
                impersonate=CHROME_IMPERSONATE,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                self.success_count += 1
                return response.json()

            logger.warning(f"HTTP {response.status_code} for SKU {sku}")
            self.fail_count += 1
            return None

        except Exception as e:
            logger.error(f"Error fetching {sku}: {e}")
            self.fail_count += 1
            return None

    def extract_product_rows(self, data: Dict, category_data: Dict = None) -> List[Dict]:
        """Extract all variant rows from product API response."""
        products = []

        product = data.get('product', {})
        if not product:
            return products

        # Specifications
        specs = {}
        for spec in product.get('specifications', []):
            specs[spec.get('code', '')] = spec.get('value', '')

        breadcrumb_path = ' > '.join([b['name'] for b in product.get('breadcrumbs', [])])
        features = ' | '.join(product.get('feature_bullets', []))
        images = product.get('image_keys', [])

        # Color variants
        color_variants = []
        for group in product.get('groups', []):
            if group.get('code') == 'color':
                for option in group.get('options', []):
                    color_variants.append({
                        'color': option.get('name'),
                        'sku': option.get('sku'),
                        'available': option.get('is_available') == 1,
                        'url': option.get('url'),
                        'image_url': image_key_to_url(option.get('image_key')),
                    })

        # Frequently bought together
        fbt_products = []
        for fbt in product.get('fbt_offers', []):
            fbt_products.append({
                'sku': fbt.get('sku'),
                'title': fbt.get('title'),
                'brand': fbt.get('brand'),
                'price': fbt.get('price'),
                'sale_price': fbt.get('sale_price'),
                'image_url': image_key_to_url(fbt.get('image_key')),
            })

        # Base info shared across variants
        base_info = {
            'detail_config_sku': product.get('sku', ''),
            'detail_product_title': product.get('product_title', ''),
            'detail_brand': product.get('brand', ''),
            'detail_feature_bullets': features,
            'detail_breadcrumbs': breadcrumb_path,
            'detail_category_code': product.get('category_code', ''),
            'detail_all_specifications_json': json.dumps(specs),
            'detail_image_count': len(images),
            'detail_image_1': image_key_to_url(images[0]) if len(images) > 0 else None,
            'detail_image_2': image_key_to_url(images[1]) if len(images) > 1 else None,
            'detail_image_3': image_key_to_url(images[2]) if len(images) > 2 else None,
            'detail_image_4': image_key_to_url(images[3]) if len(images) > 3 else None,
            'detail_image_5': image_key_to_url(images[4]) if len(images) > 4 else None,
            'detail_all_images_json': json.dumps([image_key_to_url(img) for img in images]),
            'detail_brand_rating': product.get('brand_rating', {}).get('value'),
            'detail_is_collection_eligible': product.get('is_collection_eligible'),
            'detail_available_colors': ', '.join([c['color'] for c in color_variants if c.get('color')]),
            'detail_color_variants_json': json.dumps(color_variants),
            'detail_fbt_count': len(fbt_products),
            'detail_fbt_products_json': json.dumps(fbt_products),
        }

        # Process each variant
        for variant in product.get('variants', []):
            variant_sku = variant.get('sku')
            variant_size = variant.get('variant')
            offers = variant.get('offers', [])

            if not offers:
                # Variant with no offers
                row = {}
                if category_data:
                    row.update(category_data)
                row.update(base_info)
                row['detail_variant_sku'] = variant_sku
                row['detail_size'] = variant_size
                products.append(row)
                continue

            # Create row for each offer
            for offer in offers:
                row = {}

                if category_data:
                    row.update(category_data)
                row.update(base_info)

                row['detail_variant_sku'] = variant_sku
                row['detail_size'] = variant_size
                row['detail_offer_code'] = offer.get('offer_code')
                row['detail_offer_sku'] = offer.get('sku')
                row['detail_price'] = offer.get('price')
                row['detail_currency'] = offer.get('currency', 'AED')
                row['detail_sale_price'] = offer.get('sale_price')
                row['detail_stock'] = offer.get('stock')
                row['detail_is_buyable'] = offer.get('is_buyable')
                row['detail_is_bestseller'] = offer.get('is_bestseller')
                row['detail_store_name'] = offer.get('store_name')
                row['detail_partner_code'] = offer.get('partner_code')

                # Seller ratings
                seller_ratings = offer.get('partner_ratings_sellerlab', {})
                row['detail_seller_rating'] = seller_ratings.get('partner_rating')
                row['detail_seller_rating_count'] = seller_ratings.get('num_of_rating')
                row['detail_seller_positive_rating'] = seller_ratings.get('positive_seller_rating')
                row['detail_seller_as_described_rate'] = seller_ratings.get('as_described_rate')

                # Delivery info
                row['detail_estimated_delivery'] = offer.get('estimated_delivery')
                row['detail_estimated_delivery_date'] = offer.get('estimated_delivery_date')
                row['detail_shipping_fee_message'] = offer.get('shipping_fee_message')

                # Flags
                flags = offer.get('flags', [])
                row['detail_is_marketplace'] = 'marketplace' in flags
                row['detail_is_global'] = 'global' in flags
                row['detail_is_free_delivery'] = 'free_delivery_eligible' in flags
                row['detail_flags_json'] = json.dumps(flags)
                row['detail_bnpl_available'] = bool(offer.get('bnplBanners', []))
                row['detail_cashback_available'] = bool(offer.get('cobrand_cashback_data', {}))

                products.append(row)

        return products


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_csv_file(input_path: str, output_path: str, scraper: ProductScraper,
                     total_records: int = 0, file_num: int = 0, total_files: int = 0):
    """Process a single CSV file and fetch product details."""
    filename = os.path.basename(input_path)
    start_time = datetime.now()

    logger.info(f"Starting to process: {filename}")

    # Read input CSV
    with open(input_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        logger.warning(f"Empty file: {input_path}")
        return

    total_in_file = len(rows)
    logger.info(f"Loaded {total_in_file:,} products from file")

    all_product_rows = []
    processed = 0
    skipped = 0
    success_in_file = 0
    fail_in_file = 0
    last_output_count = 0
    total_saved_to_disk = 0
    fieldnames = None

    for row in rows:
        sku = row.get('sku', '').strip()
        url_slug = row.get('url_slug', '').strip()

        if not sku or not url_slug:
            skipped += 1
            continue

        # Fetch product details
        data = scraper.get_product(url_slug, sku)

        if data:
            product_rows = scraper.extract_product_rows(data, category_data=row)
            all_product_rows.extend(product_rows)
            success_in_file += 1
        else:
            # Keep category data even if product fetch fails
            row['detail_fetch_failed'] = True
            all_product_rows.append(row)
            fail_in_file += 1

        processed += 1

        # Progress log every 25 products
        if processed % 25 == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = processed / elapsed if elapsed > 0 else 0
            eta_seconds = (total_in_file - processed) / rate if rate > 0 else 0
            eta_minutes = eta_seconds / 60

            pct = (processed / total_in_file) * 100
            rows_added = len(all_product_rows) - last_output_count
            last_output_count = len(all_product_rows)

            logger.info(f"  [File {file_num}/{total_files}] {processed:,}/{total_in_file:,} ({pct:.1f}%) | "
                       f"OK:{success_in_file} FAIL:{fail_in_file} | "
                       f"Last 25 products => {rows_added} output rows | "
                       f"Total rows: {total_saved_to_disk + len(all_product_rows):,} | "
                       f"ETA: {eta_minutes:.1f}m")

        # Save to disk every 200 output rows to avoid OOM
        if len(all_product_rows) >= 200:
            # Build fieldnames from all rows
            if fieldnames is None:
                fieldnames = list(all_product_rows[0].keys())
            for r in all_product_rows:
                for key in r.keys():
                    if key not in fieldnames:
                        fieldnames.append(key)

            # Append to file (or create with header if first time)
            file_exists = os.path.exists(output_path)
            with open(output_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                if not file_exists:
                    writer.writeheader()
                writer.writerows(all_product_rows)

            total_saved_to_disk += len(all_product_rows)
            logger.info(f"  >> SAVED {len(all_product_rows)} rows to disk (total on disk: {total_saved_to_disk:,})")
            all_product_rows = []  # Clear memory
            last_output_count = 0

        random_delay()

    # Write remaining rows
    if all_product_rows:
        if fieldnames is None:
            fieldnames = list(all_product_rows[0].keys())
        for r in all_product_rows:
            for key in r.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

        file_exists = os.path.exists(output_path)
        with open(output_path, 'a', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            if not file_exists:
                writer.writeheader()
            writer.writerows(all_product_rows)

        total_saved_to_disk += len(all_product_rows)
        logger.info(f"  >> SAVED final {len(all_product_rows)} rows to disk")

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"FILE COMPLETE: {filename}")
    logger.info(f"  Products processed: {processed:,} | Skipped: {skipped:,}")
    logger.info(f"  API Success: {success_in_file:,} | Failed: {fail_in_file:,}")
    logger.info(f"  Total output rows: {total_saved_to_disk:,}")
    logger.info(f"  Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")


def count_csv_records(filepath: str) -> int:
    """Count records in a CSV file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f) - 1  # Subtract header
    except Exception:
        return 0


def main():
    """Main entry point."""
    logger.info("=" * 70)
    logger.info("STANDALONE PRODUCT SCRAPER - STARTED")
    logger.info("=" * 70)
    logger.info(f"Input folder:  {INPUT_FOLDER}")
    logger.info(f"Output folder: {OUTPUT_FOLDER}")
    logger.info(f"Log file:      {LOG_FILE}")

    # Create output folder
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Get input CSV files (skip audit_table.csv)
    input_files = sorted([
        f for f in os.listdir(INPUT_FOLDER)
        if f.endswith('.csv') and f != 'audit_table.csv'
    ])

    if not input_files:
        logger.error("No CSV files found in input folder")
        return

    total_files = len(input_files)

    # Count total records across all files
    logger.info("-" * 70)
    logger.info("SCANNING INPUT FILES...")
    total_records = 0
    file_record_counts = {}
    for filename in input_files:
        filepath = os.path.join(INPUT_FOLDER, filename)
        count = count_csv_records(filepath)
        file_record_counts[filename] = count
        total_records += count

    logger.info(f"TOTAL FILES TO PROCESS: {total_files}")
    logger.info(f"TOTAL RECORDS TO PROCESS: {total_records:,}")
    logger.info("-" * 70)

    # List all files with record counts
    logger.info("FILES QUEUE:")
    for i, filename in enumerate(input_files, 1):
        logger.info(f"  [{i:2d}/{total_files}] {filename} ({file_record_counts[filename]:,} records)")
    logger.info("-" * 70)

    scraper = ProductScraper()
    files_processed = 0
    files_skipped = 0
    records_processed_total = 0

    for i, filename in enumerate(input_files, 1):
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"FILE {i}/{total_files}: {filename}")
        logger.info(f"Records in file: {file_record_counts[filename]:,}")
        logger.info("=" * 70)

        input_path = os.path.join(INPUT_FOLDER, filename)
        output_filename = filename.replace('dedup_', 'product_details_')
        output_path = os.path.join(OUTPUT_FOLDER, output_filename)

        # Skip if already processed
        if os.path.exists(output_path):
            logger.info(f"SKIPPING (output already exists): {output_filename}")
            files_skipped += 1
            continue

        try:
            process_csv_file(input_path, output_path, scraper, file_record_counts[filename], i, total_files)
            files_processed += 1
            records_processed_total += file_record_counts[filename]
        except Exception as e:
            logger.error(f"ERROR processing {filename}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            continue

        # Summary after each file
        logger.info(f"CUMULATIVE STATS: Files done={files_processed} | Skipped={files_skipped} | "
                   f"Records processed={records_processed_total:,} | "
                   f"API Success={scraper.success_count:,} | API Failed={scraper.fail_count:,}")

    logger.info("")
    logger.info("=" * 70)
    logger.info("SCRAPING COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Total files processed: {files_processed}")
    logger.info(f"Total files skipped:   {files_skipped}")
    logger.info(f"Total records processed: {records_processed_total:,}")
    logger.info(f"API calls - Success: {scraper.success_count:,} | Failed: {scraper.fail_count:,}")
    logger.info(f"Success rate: {(scraper.success_count / max(1, scraper.success_count + scraper.fail_count)) * 100:.1f}%")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()
