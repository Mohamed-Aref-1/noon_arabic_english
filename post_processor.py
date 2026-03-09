#!/usr/bin/env python3
"""
Post Processor for Noon Scraped Data
====================================
Processes scraped product data CSVs and outputs combined_gift_data.csv
with the specified schema.

Features:
- Combines all product detail CSVs from PRODUCT_RAW_FOLDER
- Keeps only specified columns
- Combines all images into a JSON array
- Extracts category hierarchy from breadcrumbs
- Deduplicates by sku
- Removes empty columns

Usage:
    python post_processor.py [--input-dir INPUT_DIR] [--output-file OUTPUT_FILE]

If no arguments provided, uses defaults from Config.
"""

import os
import json
import argparse
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional

from config import Config


# === COLUMNS TO KEEP ===
COLUMNS_TO_KEEP = [
    'sku', 'name', 'brand', 'url_slug', 'product_url',
    'price', 'sale_price', 'discount_percentage',
    'rating_value', 'rating_count', 'is_buyable', 'is_bestseller',
    'deal_tag_text', 'flags', 'estimated_delivery_date',
    'detail_product_title', 'detail_brand', 'detail_long_description', 'detail_feature_bullets',
    'detail_breadcrumbs',  # Needed for category extraction
    'detail_category_code', 'detail_all_specifications_json', 'detail_all_images_json',
    'detail_brand_rating', 'detail_is_collection_eligible',
    'detail_available_colors', 'detail_color_variants_json', 'detail_variant_sku', 'detail_size',
    'detail_fbt_count', 'detail_fbt_products_json',
    'detail_price', 'detail_currency', 'detail_sale_price', 'detail_stock',
    'detail_is_buyable', 'detail_is_bestseller',
    'detail_store_name', 'detail_partner_code', 'detail_seller_rating',
    'detail_seller_rating_count', 'detail_seller_positive_rating',
    'detail_seller_as_described_rate',
    'detail_estimated_delivery', 'detail_estimated_delivery_date',
    'detail_shipping_fee_message', 'detail_is_marketplace',
    'detail_is_global', 'detail_is_free_delivery',
    'detail_flags_json', 'detail_bnpl_available', 'detail_cashback_available',
]

IMAGE_COLUMNS = [
    'image_1', 'image_2', 'image_3', 'image_4', 'image_5',
    'image_6', 'image_7', 'image_8', 'image_9', 'image_10',
    'detail_image_1', 'detail_image_2', 'detail_image_3',
    'detail_image_4', 'detail_image_5'
]


# === FUNCTIONS ===

def split_breadcrumbs(breadcrumb_str) -> Dict[str, Optional[str]]:
    """
    Extract category_1, category_2, category_3, category_4 from breadcrumbs.
    Skips 'Home' (first level).

    Example: 'Home > Baby Products > Nursing & Feeding > Breastfeeding > Pumps'
    Returns: category_1='Baby Products', category_2='Nursing & Feeding',
             category_3='Breastfeeding', category_4='Pumps'
    """
    categories = {'category_1': None, 'category_2': None, 'category_3': None, 'category_4': None}

    if pd.isna(breadcrumb_str) or not str(breadcrumb_str).strip():
        return categories

    parts = [p.strip() for p in str(breadcrumb_str).split('>')]
    parts = parts[1:5]  # Skip 'Home', take next 4

    for i, part in enumerate(parts, start=1):
        categories[f'category_{i}'] = part

    return categories


def combine_images(row, image_cols: List[str]) -> Optional[str]:
    """Combine all image URLs into a JSON array."""
    images = []
    seen = set()

    for col in image_cols:
        if col in row.index and pd.notna(row[col]):
            url = str(row[col]).strip()
            if url and url not in seen:
                images.append(url)
                seen.add(url)

    return json.dumps(images) if images else None


def process_jsonl(filepath: str, image_cols: List[str]) -> pd.DataFrame:
    """Process a single JSONL file."""
    print(f"  Processing: {os.path.basename(filepath)}")

    records = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    df = pd.DataFrame(records)
    print(f"    Rows: {len(df):,}")

    # Keep only columns that exist
    cols = [c for c in COLUMNS_TO_KEEP if c in df.columns]
    df_out = df[cols].copy()

    # Add all_images column
    df_out['all_images'] = df.apply(lambda row: combine_images(row, image_cols), axis=1)

    # Add image_count column
    df_out['image_count'] = df_out['all_images'].apply(
        lambda x: len(json.loads(x)) if pd.notna(x) else 0
    )

    # Add category columns from breadcrumbs
    if 'detail_breadcrumbs' in df.columns:
        cats = pd.DataFrame(df['detail_breadcrumbs'].apply(split_breadcrumbs).tolist())
        df_out['category_1'] = cats['category_1']
        df_out['category_2'] = cats['category_2']
        df_out['category_3'] = cats['category_3']
        df_out['category_4'] = cats['category_4']
    else:
        df_out['category_1'] = None
        df_out['category_2'] = None
        df_out['category_3'] = None
        df_out['category_4'] = None

    return df_out


def finalize_combined_gift_data(output_file: str = None) -> Optional[pd.DataFrame]:
    """
    Final dedup pass on combined_gift_data.csv built incrementally during scraping.
    Much faster than re-processing all raw files — just deduplicates what's already there.
    """
    if output_file is None:
        output_file = os.path.join(Config.PRODUCT_DEDUP_FOLDER, f'{Config.OUTPUT_FILENAME}.jsonl')

    if not os.path.exists(output_file):
        print(f"{os.path.basename(output_file)} not found at {output_file} — nothing to finalize")
        return None

    print("=" * 60)
    print("Finalizing combined_gift_data.jsonl (dedup pass)")
    print("=" * 60)

    records = []
    with open(output_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    df = pd.DataFrame(records)
    print(f"Rows before final dedup: {len(df):,}")

    dedup_col = 'detail_variant_sku' if 'detail_variant_sku' in df.columns else 'sku'
    if dedup_col in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=[dedup_col], keep='first')
        print(f"Duplicates removed: {before - len(df):,}")

    empty_cols = [c for c in df.columns if df[c].notna().sum() == 0]
    if empty_cols:
        df = df.drop(columns=empty_cols)
        print(f"Removed {len(empty_cols)} empty columns")

    with open(output_file, 'w', encoding='utf-8') as f:
        for record in df.to_dict('records'):
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    print(f"Final rows: {len(df):,} | Saved to {output_file}")
    print("=" * 60)
    return df


def run_post_processor(input_dir: str = None, output_file: str = None) -> Optional[pd.DataFrame]:
    """
    Main function to process all CSVs.

    Args:
        input_dir: Directory containing product detail CSVs (defaults to Config.PRODUCT_RAW_FOLDER)
        output_file: Output file path (defaults to 'combined_gift_data.csv' in PRODUCT_DEDUP_FOLDER)

    Returns:
        Combined DataFrame or None if no files found
    """
    # Use defaults from Config if not provided
    if input_dir is None:
        input_dir = Config.PRODUCT_RAW_FOLDER

    if output_file is None:
        output_file = os.path.join(Config.PRODUCT_DEDUP_FOLDER, f'{Config.OUTPUT_FILENAME}.jsonl')

    print("=" * 60)
    print("Noon Post Processor - JSONL Combiner & Transformer")
    print("=" * 60)
    print(f"\nInput directory:  {input_dir}")
    print(f"Output file:      {output_file}")

    # Find all CSV files (excluding audit_table.csv and progress files)
    if not os.path.exists(input_dir):
        print(f"\nERROR: Input directory not found: {input_dir}")
        return None

    jsonl_files = sorted([
        Path(input_dir) / f for f in os.listdir(input_dir)
        if f.endswith('.jsonl')
    ])

    if not jsonl_files:
        print(f"\nNo JSONL files found in {input_dir}")
        return None

    print(f"\nFound {len(jsonl_files)} JSONL file(s):")
    for f in jsonl_files:
        print(f"  - {f.name}")

    # Detect image columns from first file
    with open(jsonl_files[0], 'r', encoding='utf-8') as fh:
        first_line = fh.readline().strip()
    first_record = json.loads(first_line) if first_line else {}
    image_cols = [c for c in IMAGE_COLUMNS if c in first_record]
    print(f"\nDetected {len(image_cols)} image columns")

    # Process all files
    print("\nProcessing files...")
    all_dfs = []
    for f in jsonl_files:
        try:
            df = process_jsonl(str(f), image_cols)
            all_dfs.append(df)
        except Exception as e:
            print(f"  ERROR processing {f.name}: {e}")
            continue

    if not all_dfs:
        print("\nNo data processed!")
        return None

    # Combine all dataframes
    print("\nCombining all data...")
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"Total rows before dedup: {len(combined):,}")

    # Remove duplicates by sku
    if 'sku' in combined.columns:
        before = len(combined)
        has_sku = combined['sku'].notna()
        df_with_sku = combined[has_sku].drop_duplicates(subset=['sku'], keep='first')
        df_without_sku = combined[~has_sku]
        combined = pd.concat([df_with_sku, df_without_sku], ignore_index=True)
        print(f"Duplicates removed (by sku): {before - len(combined):,}")

    # Remove completely empty columns
    empty_cols = [c for c in combined.columns if combined[c].notna().sum() == 0]
    if empty_cols:
        combined = combined.drop(columns=empty_cols)
        print(f"Removed {len(empty_cols)} empty columns: {', '.join(empty_cols[:5])}{'...' if len(empty_cols) > 5 else ''}")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)

    # Save to JSONL
    with open(output_file, 'w', encoding='utf-8') as f:
        for record in combined.to_dict('records'):
            f.write(json.dumps(record, ensure_ascii=False) + '\n')

    print("\n" + "=" * 60)
    print(f"COMPLETE!")
    print(f"  Output file: {output_file}")
    print(f"  Total products: {len(combined):,}")
    print(f"  Total columns: {len(combined.columns)}")
    print("=" * 60)

    # Show column summary
    print("\nColumn Summary:")
    print("-" * 40)
    non_null_counts = combined.notna().sum().sort_values(ascending=False)
    for col, count in non_null_counts.head(20).items():
        pct = count / len(combined) * 100
        print(f"  {col}: {count:,} ({pct:.1f}%)")
    if len(non_null_counts) > 20:
        print(f"  ... and {len(non_null_counts) - 20} more columns")

    return combined


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description='Post-process Noon scraped data')
    parser.add_argument('--input-dir', '-i', type=str, default=None,
                        help=f'Input directory with CSV files (default: {Config.PRODUCT_RAW_FOLDER})')
    parser.add_argument('--output-file', '-o', type=str, default=None,
                        help=f'Output file path (default: {Config.PRODUCT_DEDUP_FOLDER}/combined_gift_data.csv)')

    args = parser.parse_args()

    run_post_processor(
        input_dir=args.input_dir,
        output_file=args.output_file
    )


if __name__ == "__main__":
    main()
