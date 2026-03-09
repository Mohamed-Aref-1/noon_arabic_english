#!/usr/bin/env python3
"""
Noon Scraper V2 - Main Entry Point
==================================

Modular scraper with:
- Environment-based configuration (.env)
- Class-based architecture
- Profiling and timing logs
- Post-processing to combined output with schema transformation

Usage:
    python main.py

Configuration:
    Edit .env file to update cookies and settings

Output:
    - noon_category_raw/          (raw category data)
    - noon_category_dedup/        (deduplicated category data)
    - noon_product_raw/           (raw product details)
    - noon_product_dedup/         (final combined_gift_data.csv)
    - scraper_profiling.log       (timing/profiling)
"""

from datetime import datetime

from scrapers import NoonScraperManager
from utils import logger
from post_processor import run_post_processor, finalize_combined_gift_data
from s3_uploader import upload_to_s3
from email_notifier import send_summary_email
from config import Config


def print_banner():
    """Print startup banner."""
    print("""
================================================================================
     NOON SCRAPER V2 - MODULAR ARCHITECTURE + POST PROCESSOR
================================================================================

  Project Structure:
    config/
      - settings.py       : Environment configuration (.env loading)
    scrapers/
      - category_scraper.py : CategoryListScraper class
      - product_scraper.py  : ProductDetailScraper class (with visitor-id)
      - manager.py          : NoonScraperManager orchestration
    utils/
      - helpers.py        : Utilities and profiling
    main.py               : Entry point (this file)
    post_processor.py     : Transforms output to final schema
    .env                  : Cookies and settings

  Features:
    - CSV input for category URLs
    - Multi-folder output (raw + deduplicated for categories and products)
    - Memory-efficient batch writing (every 500 products)
    - Profiling to .log file
    - Easy cookie updates via .env
    - Post-processing: combines CSVs, dedupes by variant_sku, extracts categories

================================================================================
    """)


def main():
    """Main entry point."""
    print_banner()

    # Log start time
    start_time = datetime.now()
    logger.info(f"Session started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Show current configuration
    logger.info("Loading configuration from .env...")

    scraping_success = False
    manager = None

    try:
        # Initialize and run scraper
        manager = NoonScraperManager()

        # Run with all pages (set max_pages_per_category to limit)
        manager.run(max_pages_per_category=None)
        scraping_success = True

    except KeyboardInterrupt:
        logger.warning("\nScraping interrupted by user (Ctrl+C)")
        # Still try to run post-processor on whatever data we have
        scraping_success = True

    except Exception as e:
        logger.error(f"\nFatal error during scraping: {e}")
        raise

    finally:
        # Log scraping end time
        scrape_end_time = datetime.now()
        scrape_duration = scrape_end_time - start_time
        logger.info(f"\nScraping phase ended at: {scrape_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Scraping duration: {scrape_duration}")

    s3_upload_success = False

    # Run post-processor to combine and transform data
    if scraping_success:
        logger.info("\n" + "=" * 70)
        logger.info("STARTING POST-PROCESSING PHASE")
        logger.info("=" * 70)

        try:
            post_start = datetime.now()
            finalize_combined_gift_data()
            post_end = datetime.now()
            logger.info(f"Post-processing completed in: {post_end - post_start}")

        except Exception as e:
            logger.error(f"Post-processing error: {e}")
            import traceback
            logger.error(traceback.format_exc())

        # Upload final output to S3
        try:
            output_file = f"{Config.PRODUCT_DEDUP_FOLDER}/{Config.OUTPUT_FILENAME}.jsonl"
            logger.info(f"\n{'=' * 70}")
            logger.info("STARTING S3 UPLOAD PHASE")
            logger.info(f"{'=' * 70}")
            s3_upload_success = upload_to_s3(output_file)
        except Exception as e:
            logger.error(f"S3 upload error: {e}")

    # Send summary email
    if scraping_success and manager is not None:
        try:
            end_time_for_email = datetime.now()
            total_duration_for_email = end_time_for_email - start_time
            logger.info(f"\n{'=' * 70}")
            logger.info("SENDING SUMMARY EMAIL")
            logger.info(f"{'=' * 70}")
            send_summary_email(
                scrape_results=manager.scrape_results,
                products_processed=manager.products_processed,
                total_records_scrapped=manager.total_records_scrapped,
                s3_upload_success=s3_upload_success,
                duration_str=str(total_duration_for_email),
            )
        except Exception as e:
            logger.error(f"Email notification error: {e}")

    # Final summary
    end_time = datetime.now()
    total_duration = end_time - start_time
    logger.info(f"\n{'=' * 70}")
    logger.info(f"TOTAL SESSION COMPLETE")
    logger.info(f"Started:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Ended:    {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Duration: {total_duration}")
    logger.info(f"{'=' * 70}")


if __name__ == "__main__":
    main()
