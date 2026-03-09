"""
Scraper Manager Module
======================
Orchestrates the entire scraping workflow with INDEPENDENT scrapers:
- Category scraper: scrapes categories, deduplicates every 1000 records → noon_category_dedup
- Product scraper: reads from noon_category_dedup in chunks of 100, processes independently
"""

import os
import json
import time
import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Optional
import threading
from queue import Queue, Empty as QueueEmpty

from config import Config
from utils import (
    profiler,
    profile_step,
    profile_function,
    ensure_directories,
    read_categories_from_csv,
    calculate_data_schema,
    extract_filename_from_url,
    read_jsonl,
    append_jsonl,
)


# Generate timestamp for log files (once at module load)
_log_timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')


def setup_category_logger():
    """Setup logger for category API with timestamped log file."""
    log_logger = logging.getLogger('category_scraper')
    log_logger.setLevel(logging.INFO)
    log_logger.handlers = []

    log_filename = f'category_scraper_{_log_timestamp}.log'
    file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - [CATEGORY] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(file_format)

    log_logger.addHandler(file_handler)
    log_logger.addHandler(console_handler)
    return log_logger


def setup_product_details_logger():
    """Setup logger for product details API with timestamped log file."""
    log_logger = logging.getLogger('product_details_scraper')
    log_logger.setLevel(logging.INFO)
    log_logger.handlers = []

    log_filename = f'product_scraper_{_log_timestamp}.log'
    file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - [PRODUCT] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(file_format)

    log_logger.addHandler(file_handler)
    log_logger.addHandler(console_handler)
    return log_logger


category_logger = setup_category_logger()
product_details_logger = setup_product_details_logger()

from .category_scraper import CategoryListScraper
from .product_scraper import ProductDetailScraper


class NoonScraperManager:
    """
    Manager with INDEPENDENT category and product scrapers.

    Category Scraper:
    - Scrapes categories from CSV
    - Saves raw data every 500 records to noon_category_raw
    - Deduplicates every 1000 records and appends to noon_category_dedup

    Product Scraper:
    - Runs in parallel, reads from noon_category_dedup
    - Processes 100 products at a time
    - Tracks progress to know which products have been processed
    """

    CATEGORY_DEDUP_BATCH_SIZE = 1000  # Deduplicate every 1000 records
    PRODUCT_CHUNK_SIZE = 100  # Read 100 products at a time from dedup
    PRODUCT_BATCH_SIZE = 500  # Save product details every 500 records

    def __init__(self, input_csv: Optional[str] = None):
        self.input_csv = input_csv or Config.INPUT_CSV
        self.category_scraper = CategoryListScraper()
        self.product_scraper = ProductDetailScraper()
        self.scrape_results: List[dict] = []

        # Category scraper state
        self.current_category_index = 0
        self.total_categories = 0
        self.current_category_name = ""
        self.category_dedup_buffer: List[Dict] = []  # Buffer for dedup
        self.current_dedup_file: Optional[str] = None
        self.dedup_header_written: Dict[str, bool] = {}
        # Track ALL SKUs written to dedup file across all batches (for cross-batch dedup)
        self.dedup_skus_written: Dict[str, set] = {}  # {filename: set of SKUs}
        # Lock for dedup file writing (prevents read/write race conditions)
        self.dedup_file_lock = threading.Lock()

        # Product scraper state - INCREASED queue size to prevent blocking
        self.product_queue = Queue(maxsize=10000)  # Much larger queue to prevent loss
        self.product_processing_thread = None
        self.product_file_reader_thread = None
        self.product_file_monitor_thread = None  # Monitor thread for BOTH mode
        self.processing_active = False
        self.products_processed = 0
        self.total_records_scrapped = 0
        self.current_product_details_file: Optional[str] = None
        # Lock for thread-safe batch writing
        self.batch_write_lock = threading.Lock()

        # Progress tracking file (to know which products have been processed)
        self.progress_file = os.path.join(Config.PRODUCT_RAW_FOLDER, 'progress_tracker.csv')

        # Flag to indicate when all categories are complete
        self.categories_complete = False
        self.products_being_processed = 0  # Track number of products currently being processed

        # Shutdown coordination
        self.shutdown_event = threading.Event()

        # Incremental output file tracking (filename controlled by OUTPUT_FILENAME in .env)
        self._combined_output_file = os.path.join(
            Config.PRODUCT_DEDUP_FOLDER, f'{Config.OUTPUT_FILENAME}.jsonl'
        )
        self._combined_skus_written: set = set()
        self._combined_header_written: bool = False
        self._combined_write_lock = threading.Lock()
        self._records_since_last_upload: int = 0

        ensure_directories()
        self._initialize_audit_tables()

        category_logger.info("NoonScraperManager initialized")
        category_logger.info(f"  - Category batch size: {Config.BATCH_SIZE}")
        category_logger.info(f"  - Category dedup batch: {self.CATEGORY_DEDUP_BATCH_SIZE}")
        category_logger.info(f"  - Product chunk size: {self.PRODUCT_CHUNK_SIZE}")
        category_logger.info(f"  - Product save batch: {self.PRODUCT_BATCH_SIZE}")
        category_logger.info(f"  - Category log: category_scraper_{_log_timestamp}.log")
        category_logger.info(f"  - Product log: product_scraper_{_log_timestamp}.log")

    def _initialize_audit_tables(self):
        """Initialize empty audit tables at startup."""
        audit_columns = ['filename', 'source_url', 'number_of_records', 'data_schema', 'status', 'last_updated']
        empty_df = pd.DataFrame(columns=audit_columns)

        for folder in [Config.CATEGORY_RAW_FOLDER, Config.CATEGORY_DEDUP_FOLDER,
                       Config.PRODUCT_RAW_FOLDER, Config.PRODUCT_DEDUP_FOLDER]:
            audit_path = os.path.join(folder, 'audit_table.csv')
            empty_df.to_csv(audit_path, index=False, encoding='utf-8')

    # =========================================================================
    # CHECKPOINT - Resume support
    # =========================================================================

    def _get_checkpoint_path(self) -> str:
        return os.path.join(Config.CATEGORY_RAW_FOLDER, 'checkpoint.json')

    def _load_checkpoint(self) -> dict:
        """Load checkpoint file. Returns empty dict if none exists."""
        path = self._get_checkpoint_path()
        if not os.path.exists(path):
            return {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            category_logger.info(f"Checkpoint loaded: {len(data)} categories tracked")
            return data
        except Exception as e:
            category_logger.warning(f"Could not load checkpoint: {e} — starting fresh")
            return {}

    def _save_checkpoint(self, checkpoint: dict):
        """Persist the checkpoint dict to disk atomically."""
        path = self._get_checkpoint_path()
        tmp_path = path + '.tmp'
        try:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, indent=2)
            os.replace(tmp_path, path)
        except Exception as e:
            category_logger.warning(f"Could not save checkpoint: {e}")

    def _get_product_details_filename(self, category_filename: str) -> str:
        """Generate product details filename from category filename."""
        if category_filename.startswith('dedup_'):
            name = 'details_' + category_filename[6:]  # Remove 'dedup_' prefix
        elif category_filename.startswith('noon_'):
            name = 'details_' + category_filename
        else:
            name = 'details_' + category_filename
        # Always store product details as JSONL regardless of source extension
        if name.endswith('.csv'):
            name = name[:-4] + '.jsonl'
        return name

    # =========================================================================
    # CATEGORY SCRAPER - Deduplicates every 1000 records
    # =========================================================================

    def _on_category_batch_written(self, products: List[Dict], raw_output_path: str):
        """
        Callback when category scraper writes a batch of 500 products.
        Accumulates products and deduplicates every 1000 records.
        """
        if not products:
            return

        # Add to dedup buffer
        self.category_dedup_buffer.extend(products)

        category_logger.info(
            f"[Category {self.current_category_index}/{self.total_categories}] "
            f"Raw batch written: {len(products)} products | Dedup buffer: {len(self.category_dedup_buffer)}"
        )

        # Deduplicate when buffer reaches 1000
        if len(self.category_dedup_buffer) >= self.CATEGORY_DEDUP_BATCH_SIZE:
            self._flush_dedup_buffer()

    def _sanitize_for_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitize DataFrame values for CSV writing.
        Removes/replaces characters that can break CSV parsing.
        """
        for col in df.columns:
            if df[col].dtype == 'object':  # String columns
                df[col] = df[col].apply(lambda x: self._sanitize_value(x) if isinstance(x, str) else x)
        return df

    def _sanitize_value(self, value: str) -> str:
        """Sanitize a single string value for CSV."""
        if not value:
            return value
        # Replace newlines and carriage returns with spaces
        value = value.replace('\n', ' ').replace('\r', ' ')
        # Replace tabs with spaces
        value = value.replace('\t', ' ')
        # Remove null bytes and other control characters
        value = ''.join(char for char in value if ord(char) >= 32 or char in ' ')
        # Replace double quotes with single quotes to prevent CSV issues
        value = value.replace('"', "'")
        # Normalize multiple spaces to single space
        while '  ' in value:
            value = value.replace('  ', ' ')
        return value.strip()

    def _flush_dedup_buffer(self):
        """Deduplicate and write the buffer to dedup file with CROSS-BATCH deduplication."""
        if not self.category_dedup_buffer or not self.current_dedup_file:
            return

        df = pd.DataFrame(self.category_dedup_buffer)
        original_count = len(df)

        # Filter out rows with empty/null SKU first (these can't be deduplicated properly)
        df = df[df['sku'].notna() & (df['sku'] != '')]
        after_null_filter = len(df)
        null_skus_removed = original_count - after_null_filter

        if null_skus_removed > 0:
            category_logger.warning(
                f"[Category {self.current_category_index}/{self.total_categories}] "
                f"Removed {null_skus_removed} products with empty/null SKU"
            )

        # Deduplicate within the buffer first
        df_dedup = df.drop_duplicates(subset=['sku'], keep='first')
        dedup_within_batch = len(df_dedup)

        # Initialize tracking set for this file if not exists
        if self.current_dedup_file not in self.dedup_skus_written:
            self.dedup_skus_written[self.current_dedup_file] = set()

        # CROSS-BATCH DEDUPLICATION: Filter out SKUs already written to file
        already_written = self.dedup_skus_written[self.current_dedup_file]
        new_skus_mask = ~df_dedup['sku'].isin(already_written)
        df_new = df_dedup[new_skus_mask]
        final_count = len(df_new)
        cross_batch_dups = dedup_within_batch - final_count

        if cross_batch_dups > 0:
            category_logger.info(
                f"[Category {self.current_category_index}/{self.total_categories}] "
                f"Filtered {cross_batch_dups} cross-batch duplicate SKUs"
            )

        if final_count == 0:
            category_logger.info(
                f"[Category {self.current_category_index}/{self.total_categories}] "
                f"No new unique SKUs to write after cross-batch dedup"
            )
            self.category_dedup_buffer = []
            return

        # Track newly written SKUs
        new_skus = set(df_new['sku'].tolist())
        self.dedup_skus_written[self.current_dedup_file].update(new_skus)

        # Use lock to prevent read/write race conditions
        with self.dedup_file_lock:
            file_mode = 'w' if not self.dedup_header_written.get(self.current_dedup_file, False) else 'a'
            append_jsonl(df_new.to_dict('records'), self.current_dedup_file, mode=file_mode)
            self.dedup_header_written[self.current_dedup_file] = True

        category_logger.info(
            f"[Category {self.current_category_index}/{self.total_categories}] "
            f"DEDUP: {original_count} → {final_count} records (within-batch: {dedup_within_batch}, cross-batch dups: {cross_batch_dups}) | "
            f"Total unique in file: {len(self.dedup_skus_written[self.current_dedup_file])} | "
            f"Saved to {os.path.basename(self.current_dedup_file)}"
        )

        # Clear buffer
        self.category_dedup_buffer = []

    # =========================================================================
    # PRODUCT SCRAPER - Reads from dedup files independently
    # =========================================================================

    def _get_processed_skus(self, dedup_file: str) -> set:
        """Get set of SKUs that have already been processed for this file."""
        details_filename = self._get_product_details_filename(os.path.basename(dedup_file))
        details_path = os.path.join(Config.PRODUCT_RAW_FOLDER, details_filename)

        if not os.path.exists(details_path):
            return set()

        try:
            skus = set()
            with open(details_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            sku = json.loads(line).get('sku')
                            if sku:
                                skus.add(sku)
                        except json.JSONDecodeError:
                            pass
            return skus
        except Exception as e:
            product_details_logger.error(f"Error reading processed SKUs: {e}")
            return set()

    def _product_file_reader(self, dedup_file: str, category_index: int, total_categories: int):
        """
        Background thread that reads from dedup file in chunks of 100.
        Adds products to the queue for processing.
        """
        try:
            # Wait a bit for some dedup data to be written
            time.sleep(5)

            category_name = os.path.basename(dedup_file).replace('.jsonl', '')
            product_details_logger.info(f"\n{'='*60}")
            product_details_logger.info(f"[Category {category_index}/{total_categories}] PRODUCT READER STARTED")
            product_details_logger.info(f"Reading from: {dedup_file}")
            product_details_logger.info(f"{'='*60}")

            # Set up output file
            details_filename = self._get_product_details_filename(os.path.basename(dedup_file))
            self.current_product_details_file = os.path.join(Config.PRODUCT_RAW_FOLDER, details_filename)

            last_read_position = 0
            processed_skus = self._get_processed_skus(dedup_file)
            product_details_logger.info(f"[Category {category_index}/{total_categories}] Already processed: {len(processed_skus)} SKUs")

            while self.processing_active:
                # Check if file exists and has new data
                if not os.path.exists(dedup_file):
                    time.sleep(2)
                    continue

                try:
                    # Read the entire file (it's being appended to)
                    df = read_jsonl(dedup_file)
                    total_rows = len(df)

                    if total_rows <= last_read_position:
                        # No new data, wait and check again
                        time.sleep(2)
                        continue

                    # Get new rows
                    new_rows = df.iloc[last_read_position:].to_dict('records')

                    # Filter out already processed SKUs
                    unprocessed = [r for r in new_rows if r.get('sku') not in processed_skus]

                    if unprocessed:
                        product_details_logger.info(
                            f"[Category {category_index}/{total_categories}] "
                            f"Found {len(unprocessed)} new products (rows {last_read_position+1}-{total_rows})"
                        )

                        # Add to queue in chunks of 100
                        for i in range(0, len(unprocessed), self.PRODUCT_CHUNK_SIZE):
                            chunk = unprocessed[i:i + self.PRODUCT_CHUNK_SIZE]
                            for product in chunk:
                                queue_item = {
                                    'url_slug': product.get('url_slug', ''),
                                    'sku': product.get('sku', ''),
                                    'category_data': product,
                                    'output_file': self.current_product_details_file,
                                    'category_index': category_index,
                                    'total_categories': total_categories,
                                    'category_name': category_name,
                                }
                                self.product_queue.put(queue_item)
                                processed_skus.add(product.get('sku'))

                            product_details_logger.info(
                                f"[Category {category_index}/{total_categories}] "
                                f"Queued chunk of {len(chunk)} products | Queue: {self.product_queue.qsize()}"
                            )

                            # Wait for queue to be processed before adding more
                            while self.product_queue.qsize() > 10 and self.processing_active:
                                time.sleep(1)

                    last_read_position = total_rows

                except Exception as e:
                    product_details_logger.error(f"Error reading dedup file: {e}")
                    time.sleep(2)

                # Small delay before checking for more data
                time.sleep(1)

        except Exception as e:
            product_details_logger.error(f"Product file reader error: {e}")

    def _product_processor(self):
        """
        Background thread that processes products from the queue.
        Ensures ALL items are processed before shutdown.
        """
        product_batches = {}
        file_headers_written = {}

        product_details_logger.info("Product processor thread started - waiting for items...")

        while True:
            # Check for shutdown signal - but ALWAYS drain the queue first
            if self.shutdown_event.is_set() and self.product_queue.empty():
                product_details_logger.info("Shutdown signal received and queue is empty - exiting processor")
                break

            try:
                # Use shorter timeout when shutting down to be more responsive
                timeout = 1 if self.shutdown_event.is_set() else 2
                item = self.product_queue.get(timeout=timeout)

                if item is None:
                    # Sentinel value - drain remaining items before exiting
                    product_details_logger.info("Received sentinel value - checking for remaining items...")
                    if self.product_queue.empty():
                        break
                    else:
                        continue  # Keep processing remaining items

                url_slug = item['url_slug']
                sku = item['sku']
                category_data = item['category_data']
                output_file = item['output_file']
                category_index = item.get('category_index', 0)
                total_categories = item.get('total_categories', 0)

                if not url_slug or not sku:
                    self.product_queue.task_done()
                    continue

                # Increment counter for products being processed
                self.products_being_processed += 1

                try:
                    # Process the product
                    detail_rows = self.product_scraper.get_all_product_rows(url_slug, sku, category_data)

                    if detail_rows:
                        if output_file not in product_batches:
                            product_batches[output_file] = []

                        product_batches[output_file].extend(detail_rows)
                        self.products_processed += 1
                        self.total_records_scrapped += len(detail_rows)

                        # Log progress every 10 products
                        if self.products_processed % 10 == 0:
                            product_details_logger.info(
                                f"[Category {category_index}/{total_categories}] "
                                f"Processed: {self.products_processed} | Records: {self.total_records_scrapped} | "
                                f"Queue: {self.product_queue.qsize()} | Pending batches: {sum(len(b) for b in product_batches.values())}"
                            )

                        # Save batch when it reaches size
                        if len(product_batches[output_file]) >= self.PRODUCT_BATCH_SIZE:
                            with self.batch_write_lock:
                                header_written = file_headers_written.get(output_file, False)
                                self._write_product_batch(product_batches[output_file], output_file, not header_written)
                                file_headers_written[output_file] = True
                                product_batches[output_file] = []

                            product_details_logger.info(
                                f"[Category {category_index}/{total_categories}] "
                                f"BATCH SAVED: {self.PRODUCT_BATCH_SIZE} records to {os.path.basename(output_file)}"
                            )
                    else:
                        product_details_logger.warning(f"No detail rows returned for SKU: {sku}")

                except Exception as e:
                    product_details_logger.error(f"Error processing product {sku}: {e}")

                # Decrement counter for products being processed
                self.products_being_processed -= 1
                self.product_queue.task_done()

            except Exception as e:
                # Queue.Empty exception - this is normal during shutdown (its str() is empty!)
                if not isinstance(e, QueueEmpty):
                    product_details_logger.error(f"Product processor error: {type(e).__name__}: {e}")
                # Check if we should exit
                if self.shutdown_event.is_set() and self.product_queue.empty():
                    break
                continue

        # CRITICAL: Flush ALL remaining batches before exit
        product_details_logger.info(f"Processor exiting - flushing {len(product_batches)} remaining batch files...")
        total_flushed = 0
        for file_path, batch in product_batches.items():
            if batch:
                with self.batch_write_lock:
                    header_written = file_headers_written.get(file_path, False)
                    self._write_product_batch(batch, file_path, not header_written)
                    file_headers_written[file_path] = True
                total_flushed += len(batch)
                product_details_logger.info(f"FINAL BATCH: {len(batch)} records written to {os.path.basename(file_path)}")

        product_details_logger.info(f"Processor thread completed - flushed {total_flushed} remaining records")

    def _write_product_batch(self, batch, file_path, write_header):
        """Write a batch of products to JSONL file and append to combined_gift_data.jsonl."""
        if not batch or not file_path:
            return

        mode = 'w' if write_header else 'a'
        with open(file_path, mode, encoding='utf-8') as f:
            for record in batch:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')

        # Incrementally append transformed records to combined_gift_data.jsonl
        self._append_to_combined_gift_data(pd.DataFrame(batch))

    def _append_to_combined_gift_data(self, df: pd.DataFrame):
        """
        Apply post-processor transformation to a batch and append new records
        to combined_gift_data.jsonl. Skips SKUs already written (cross-batch dedup).
        """
        from post_processor import COLUMNS_TO_KEEP, IMAGE_COLUMNS, combine_images, split_breadcrumbs
        import json as _json

        try:
            # Filter to new SKUs only
            if 'detail_variant_sku' in df.columns:
                dedup_col = 'detail_variant_sku'
            elif 'sku' in df.columns:
                dedup_col = 'sku'
            else:
                dedup_col = None

            with self._combined_write_lock:
                if dedup_col:
                    new_mask = ~df[dedup_col].astype(str).isin(self._combined_skus_written)
                    df = df[new_mask]

                if df.empty:
                    return

                # Keep only available columns from the schema
                cols = [c for c in COLUMNS_TO_KEEP if c in df.columns]
                df_out = df[cols].copy()

                # Detect image columns present in this batch
                img_cols = [c for c in IMAGE_COLUMNS if c in df.columns]

                # Combined images JSON
                df_out['all_images'] = df.apply(
                    lambda row: combine_images(row, img_cols), axis=1
                )
                df_out['image_count'] = df_out['all_images'].apply(
                    lambda x: len(_json.loads(x)) if pd.notna(x) else 0
                )

                # Category hierarchy from breadcrumbs
                if 'detail_breadcrumbs' in df.columns:
                    cats = pd.DataFrame(df['detail_breadcrumbs'].apply(split_breadcrumbs).tolist())
                    for col in ['category_1', 'category_2', 'category_3', 'category_4']:
                        df_out[col] = cats[col]
                else:
                    for col in ['category_1', 'category_2', 'category_3', 'category_4']:
                        df_out[col] = None

                mode = 'w' if not self._combined_header_written else 'a'
                os.makedirs(os.path.dirname(self._combined_output_file), exist_ok=True)
                with open(self._combined_output_file, mode, encoding='utf-8') as f:
                    for record in df_out.to_dict('records'):
                        f.write(json.dumps(record, ensure_ascii=False) + '\n')
                self._combined_header_written = True

                # Track written SKUs
                if dedup_col and dedup_col in df.columns:
                    self._combined_skus_written.update(df[dedup_col].astype(str).tolist())

                self._records_since_last_upload += len(df_out)

                product_details_logger.info(
                    f"combined_gift_data.jsonl +{len(df_out)} records "
                    f"(total unique: {len(self._combined_skus_written)})"
                )

                # Incremental S3 upload every N records if configured
                upload_every = Config.S3_UPLOAD_EVERY
                if upload_every > 0 and self._records_since_last_upload >= upload_every:
                    from s3_uploader import upload_to_s3
                    product_details_logger.info(
                        f"S3 incremental upload triggered ({self._records_since_last_upload} new records)"
                    )
                    upload_to_s3(self._combined_output_file)
                    self._records_since_last_upload = 0

        except Exception as e:
            product_details_logger.error(f"Error appending to combined_gift_data.jsonl: {e}")

    def _start_product_scraper(self, dedup_file: str, category_index: int, total_categories: int):
        """Start the product scraper threads for a category."""
        self.processing_active = True

        # Start the file reader thread
        self.product_file_reader_thread = threading.Thread(
            target=self._product_file_reader,
            args=(dedup_file, category_index, total_categories)
        )
        self.product_file_reader_thread.daemon = True
        self.product_file_reader_thread.start()

        # Start the processor thread (if not already running)
        if self.product_processing_thread is None or not self.product_processing_thread.is_alive():
            self.product_processing_thread = threading.Thread(target=self._product_processor)
            self.product_processing_thread.daemon = True
            self.product_processing_thread.start()
            product_details_logger.info("Product processor thread started")

    def _stop_product_scraper(self):
        """
        Stop the product scraper threads GRACEFULLY.
        Ensures ALL queued items are processed before shutdown.
        WAITS INDEFINITELY for all products to be processed - no data loss.
        """
        product_details_logger.info("="*60)
        product_details_logger.info("INITIATING GRACEFUL SHUTDOWN")
        product_details_logger.info(f"Queue size: {self.product_queue.qsize()}")
        product_details_logger.info(f"Products being processed: {self.products_being_processed}")
        product_details_logger.info("="*60)

        # Signal that no more items will be added
        self.processing_active = False
        self.categories_complete = True

        # Wait for queue to drain AND all in-flight products to complete
        # NO TIMEOUT - we wait until everything is done to prevent data loss
        if not self.product_queue.empty() or self.products_being_processed > 0:
            product_details_logger.info("Waiting for ALL remaining products to be processed...")
            product_details_logger.info("(No timeout - will wait until complete to prevent data loss)")
            drain_start = time.time()
            last_log_time = 0

            while not self.product_queue.empty() or self.products_being_processed > 0:
                elapsed = time.time() - drain_start

                # Log progress every 30 seconds
                if int(elapsed) - last_log_time >= 30:
                    last_log_time = int(elapsed)
                    queue_size = self.product_queue.qsize()
                    product_details_logger.info(
                        f"Still processing... Queue: {queue_size} | In-flight: {self.products_being_processed} | "
                        f"Processed: {self.products_processed} | Elapsed: {elapsed:.0f}s"
                    )

                time.sleep(1)

            product_details_logger.info(f"All products processed! Drain completed in {time.time() - drain_start:.1f}s")
        else:
            product_details_logger.info("Queue is already empty and no products in-flight")

        # Signal shutdown event
        self.shutdown_event.set()

        # Add sentinel values to wake up processor thread
        for _ in range(3):
            try:
                self.product_queue.put(None, timeout=1)
            except:
                pass

        # Wait for file monitor thread to finish (if it exists - not used in sequential mode)
        if hasattr(self, 'product_file_monitor_thread') and self.product_file_monitor_thread:
            if self.product_file_monitor_thread.is_alive():
                product_details_logger.info("Waiting for file monitor thread...")
                self.product_file_monitor_thread.join(timeout=60)
                if self.product_file_monitor_thread.is_alive():
                    product_details_logger.warning("File monitor thread did not finish in time")

        # Wait for processor thread to finish flushing final batches
        # Give it plenty of time - 10 minutes should be enough for flushing
        if self.product_processing_thread:
            product_details_logger.info("Waiting for processor thread to finish flushing final batches...")
            self.product_processing_thread.join(timeout=600)  # 10 minutes for final flush
            if self.product_processing_thread.is_alive():
                product_details_logger.warning("Processor thread still running after 10 minutes - forcing continue")
            else:
                product_details_logger.info("Processor thread completed successfully")

        product_details_logger.info("="*60)
        product_details_logger.info("PRODUCT SCRAPER SHUTDOWN COMPLETE")
        product_details_logger.info(f"Total products processed: {self.products_processed}")
        product_details_logger.info(f"Total records scraped: {self.total_records_scrapped}")
        product_details_logger.info("="*60)

    def _safe_read_dedup_csv(self, file_path: str, max_retries: int = 3) -> Optional[pd.DataFrame]:
        """Safely read a dedup JSONL file with retry logic."""
        for attempt in range(max_retries):
            try:
                with self.dedup_file_lock:
                    return read_jsonl(file_path)
            except Exception as e:
                product_details_logger.warning(
                    f"Error reading {os.path.basename(file_path)} (attempt {attempt + 1}/{max_retries}): {e}"
                )
                time.sleep(1)

        product_details_logger.error(f"Failed to read {file_path} after {max_retries} attempts")
        return None

    def _product_file_monitor(self, categories):
        """
        Monitors all dedup files and feeds products to the queue as they become available.
        This runs in a separate thread and monitors all categories simultaneously.
        ENSURES all products are queued before exiting.

        NOTE: Dynamically discovers dedup files in the folder rather than predicting
        filenames, since each category is scraped at different times with different timestamps.
        """
        product_details_logger.info(f"Product file monitor started - will monitor {Config.CATEGORY_DEDUP_FOLDER} for dedup files")
        product_details_logger.info(f"Expected categories: {len(categories)}")

        # Track processed SKUs for each file (SKUs that have been queued)
        queued_skus_by_file = {}
        # Track last file sizes to detect new data
        last_file_sizes = {}
        # Track files we've already logged as discovered
        discovered_files = set()

        final_scan_done = False

        while True:
            # Check if we should exit
            if self.shutdown_event.is_set():
                product_details_logger.info("Monitor received shutdown signal - exiting")
                break

            # After categories are complete, do one final scan to ensure all products are queued
            if self.categories_complete and not self.processing_active and not final_scan_done:
                product_details_logger.info("Categories complete - performing final scan of all dedup files...")
                final_scan_done = True

            items_queued_this_round = 0

            # DYNAMICALLY discover all dedup files in the folder (not predicted filenames)
            try:
                all_dedup_files = [
                    os.path.join(Config.CATEGORY_DEDUP_FOLDER, f)
                    for f in os.listdir(Config.CATEGORY_DEDUP_FOLDER)
                    if f.startswith('dedup_') and f.endswith('.jsonl')
                ]
            except Exception as e:
                product_details_logger.error(f"Error listing dedup folder: {e}")
                all_dedup_files = []

            # Log newly discovered files
            for f in all_dedup_files:
                if f not in discovered_files:
                    discovered_files.add(f)
                    product_details_logger.info(f"Discovered new dedup file: {os.path.basename(f)}")

            for dedup_file in all_dedup_files:
                if self.shutdown_event.is_set():
                    break

                if not os.path.exists(dedup_file):
                    continue

                try:
                    # Check if file has changed since last read
                    current_size = os.path.getsize(dedup_file)
                    last_size = last_file_sizes.get(dedup_file, 0)

                    # Skip if file hasn't changed (unless doing final scan)
                    if current_size == last_size and not final_scan_done:
                        continue

                    last_file_sizes[dedup_file] = current_size

                    # Initialize tracking for this file
                    if dedup_file not in queued_skus_by_file:
                        # Load already processed SKUs from existing details file
                        queued_skus_by_file[dedup_file] = self._get_processed_skus(dedup_file)
                        product_details_logger.info(
                            f"Initialized tracking for {os.path.basename(dedup_file)}: "
                            f"{len(queued_skus_by_file[dedup_file])} already processed"
                        )

                    # Read the dedup file safely with retry logic
                    df = self._safe_read_dedup_csv(dedup_file)
                    if df is None:
                        continue  # Skip this file if reading failed

                    total_rows = len(df)

                    # Get all products and filter to unqueued ones
                    all_products = df.to_dict('records')
                    unqueued = [r for r in all_products
                                if r.get('sku') and r.get('sku') not in queued_skus_by_file[dedup_file]]

                    if unqueued:
                        category_name = os.path.basename(dedup_file).replace('.jsonl', '').replace('dedup_', '')

                        product_details_logger.info(
                            f"Monitor found {len(unqueued)} new products in {os.path.basename(dedup_file)} "
                            f"(total in file: {total_rows}, already queued: {len(queued_skus_by_file[dedup_file])})"
                        )

                        # Queue ALL unqueued products
                        for product in unqueued:
                            sku = product.get('sku', '')
                            if not sku:
                                continue

                            # Determine which category this belongs to for logging
                            category_idx = 1
                            total_cats = len(categories)

                            for idx, cat_url in enumerate(categories, 1):
                                cat_filename = extract_filename_from_url(cat_url)
                                if cat_filename in dedup_file:
                                    category_idx = idx
                                    break

                            queue_item = {
                                'url_slug': product.get('url_slug', ''),
                                'sku': sku,
                                'category_data': product,
                                'output_file': os.path.join(
                                    Config.PRODUCT_RAW_FOLDER,
                                    self._get_product_details_filename(os.path.basename(dedup_file))
                                ),
                                'category_index': category_idx,
                                'total_categories': total_cats,
                                'category_name': category_name,
                            }

                            # Queue the item (blocking put to ensure it's added)
                            self.product_queue.put(queue_item)
                            queued_skus_by_file[dedup_file].add(sku)
                            items_queued_this_round += 1

                        product_details_logger.info(
                            f"Queued {len(unqueued)} products from {os.path.basename(dedup_file)} | "
                            f"Queue size: {self.product_queue.qsize()}"
                        )

                except Exception as e:
                    product_details_logger.error(f"Error monitoring file {dedup_file}: {e}")
                    import traceback
                    product_details_logger.error(traceback.format_exc())

            # If final scan is done and no new items were queued in this round, check if we should exit
            if final_scan_done and items_queued_this_round == 0:
                # Check if there are still files with unprocessed products
                has_unprocessed = False
                for dedup_file in all_dedup_files:
                    if not os.path.exists(dedup_file):
                        continue
                    try:
                        df = self._safe_read_dedup_csv(dedup_file)
                        if df is not None and len(df) > 0:
                            # Check if there are products not yet queued
                            all_products = df.to_dict('records')
                            unqueued = [r for r in all_products
                                      if r.get('sku') and r.get('sku') not in queued_skus_by_file.get(dedup_file, set())]
                            if unqueued:
                                has_unprocessed = True
                                product_details_logger.info(
                                    f"Still have {len(unqueued)} unprocessed products in {os.path.basename(dedup_file)}"
                                )
                                break
                    except:
                        continue  # Skip if file can't be read

                if not has_unprocessed:
                    product_details_logger.info("Final scan complete - all products queued")
                    # Log final statistics
                    total_queued = sum(len(skus) for skus in queued_skus_by_file.values())
                    product_details_logger.info(f"Total products queued across all files: {total_queued}")
                    product_details_logger.info(f"Files processed: {len(queued_skus_by_file)}")
                    break

            # Small delay to prevent excessive CPU usage
            time.sleep(2)

        product_details_logger.info("Product file monitor exiting")

    # =========================================================================
    # MAIN RUN METHODS
    # =========================================================================

    @profile_function
    def run(self, max_pages_per_category: Optional[int] = None):
        """Main execution method - routes based on SCRAPER_MODE."""
        mode = Config.SCRAPER_MODE

        if mode == 'CATEGORY_ONLY':
            category_logger.info("Running in CATEGORY_ONLY mode")
            self.run_category_only(max_pages_per_category)
        elif mode == 'PRODUCTS_ONLY':
            category_logger.info("Running in PRODUCTS_ONLY mode")
            self.run_products_only()
        else:
            category_logger.info("Running in BOTH mode (independent scrapers)")
            self.run_both(max_pages_per_category)

    def run_both(self, max_pages_per_category: Optional[int] = None):
        """
        Run both scrapers INDEPENDENTLY:
        - Category scraper runs through ALL categories without waiting
        - Product scraper runs in background, processes products as they become available
        - No waiting between categories - maximizes efficiency
        """
        profiler.start_session()

        category_logger.info("\n" + "=" * 70)
        category_logger.info("NOON SCRAPER - BOTH MODE (INDEPENDENT SCRAPERS)")
        category_logger.info("=" * 70)
        category_logger.info("Category scraper will NOT wait for product scraper!")
        Config.print_config()

        categories = read_categories_from_csv(self.input_csv)
        if not categories:
            category_logger.error("No categories found. Exiting.")
            profiler.end_session()
            return

        # Reset all counters and state for fresh run
        self.products_processed = 0
        self.total_records_scrapped = 0
        self.categories_complete = False
        self.dedup_skus_written = {}  # Reset cross-batch dedup tracking
        self.dedup_header_written = {}
        self.shutdown_event.clear()

        self.total_categories = len(categories)
        category_logger.info(f"\nProcessing {len(categories)} categories...\n")

        # Start the product processor thread (runs continuously for all categories)
        self.processing_active = True
        self.product_processing_thread = threading.Thread(target=self._product_processor)
        self.product_processing_thread.daemon = True
        self.product_processing_thread.start()
        product_details_logger.info("Product processor thread started")

        # Start the product file monitor thread (monitors all dedup files)
        self.product_file_monitor_thread = threading.Thread(
            target=self._product_file_monitor,
            args=(categories,)
        )
        self.product_file_monitor_thread.daemon = True
        self.product_file_monitor_thread.start()
        product_details_logger.info("Product file monitor thread started")

        # Load checkpoint to support resume from interruption
        checkpoint = self._load_checkpoint()
        if checkpoint:
            completed = [u for u, v in checkpoint.items() if v.get('status') == 'complete']
            in_progress = [u for u, v in checkpoint.items() if v.get('status') == 'in_progress']
            category_logger.info(f"Checkpoint: {len(completed)} complete, {len(in_progress)} in-progress")

        # Process each category: scrape ALL pages WITHOUT waiting for product processing
        for i, category_url in enumerate(categories, 1):
            self.current_category_index = i
            self.current_category_name = category_url.split('/')[-2] if category_url.endswith('/') else category_url.split('/')[-1]

            cat_cp = checkpoint.get(category_url, {})

            # Skip already-completed categories
            if cat_cp.get('status') == 'complete':
                category_logger.info(f"\n[Category {i}/{len(categories)}] Already complete — skipping: {category_url}")
                # Still need a synthetic result so product scraper can pick up the dedup file
                self.scrape_results.append({
                    'success': True,
                    'filename': cat_cp.get('raw_file', ''),
                    'source_url': category_url,
                    'number_of_records': cat_cp.get('records', 0),
                    'resumed': True,
                    'skipped': True,
                })
                continue

            # Determine resume position if in-progress
            start_page = 1
            output_path_override = None
            if cat_cp.get('status') == 'in_progress':
                start_page = cat_cp.get('last_page', 1)
                output_path_override = cat_cp.get('raw_file') or None
                category_logger.info(
                    f"\n[Category {i}/{len(categories)}] Resuming from page {start_page}: {category_url}"
                )
            else:
                category_logger.info(f"\n{'='*70}")
                category_logger.info(f"[Category {i}/{len(categories)}] STARTING CATEGORY SCRAPING")
                category_logger.info(f"URL: {category_url}")
                category_logger.info(f"{'='*70}")

            # Set up dedup file for this category
            category_filename = extract_filename_from_url(category_url)
            dedup_filename = f"dedup_{category_filename}"
            self.current_dedup_file = os.path.join(Config.CATEGORY_DEDUP_FOLDER, dedup_filename)
            self.category_dedup_buffer = []

            # Checkpoint callback: saves progress after every batch write
            def _on_page_scraped(page, total_pages, raw_file, _url=category_url):
                checkpoint[_url] = {
                    'status': 'in_progress',
                    'last_page': page,
                    'total_pages': total_pages,
                    'raw_file': raw_file,
                }
                self._save_checkpoint(checkpoint)

            # ================================================================
            # STEP 1: Complete ALL category scraping for this category
            # ================================================================
            with profile_step(f"Scrape category {i}"):
                result = self.category_scraper.scrape_category(
                    category_url=category_url,
                    output_folder=Config.CATEGORY_RAW_FOLDER,
                    max_pages=max_pages_per_category,
                    on_batch_written=self._on_category_batch_written,
                    start_page=start_page,
                    output_path_override=output_path_override,
                    on_page_scraped=_on_page_scraped,
                )

            self.scrape_results.append(result)

            # Flush any remaining dedup buffer
            if self.category_dedup_buffer:
                self._flush_dedup_buffer()

            category_logger.info(f"\n[Category {i}/{len(categories)}] CATEGORY SCRAPING COMPLETE")
            category_logger.info(f"[Category {i}/{len(categories)}] Product scraper will continue in background...")

            # Mark category as complete in checkpoint
            checkpoint[category_url] = {
                'status': 'complete',
                'last_page': checkpoint.get(category_url, {}).get('total_pages', 0),
                'total_pages': checkpoint.get(category_url, {}).get('total_pages', 0),
                'raw_file': checkpoint.get(category_url, {}).get('raw_file', ''),
                'records': result.get('number_of_records', 0),
            }
            self._save_checkpoint(checkpoint)

            # Update audit tables for category
            self._update_audit_tables(result)

            if i < len(categories):
                category_logger.info(f"[Category {i}/{len(categories)}] Moving to next category in 3s...")
                time.sleep(3)

        # All categories done - signal completion
        category_logger.info("\n" + "=" * 70)
        category_logger.info("ALL CATEGORIES SCRAPED - Signaling product scraper to finish...")
        category_logger.info("=" * 70)

        self.categories_complete = True

        # Give some time for the monitor to process any remaining items
        time.sleep(5)

        # Stop product scraper
        self._stop_product_scraper()

        # Update any remaining product audit tables
        self._update_remaining_product_audits()

        profiler.end_session()
        self._print_summary()

    def _queue_products_from_dedup_file(self, dedup_file: str, category_index: int, total_categories: int) -> int:
        """
        Queue ALL products from a completed dedup file for processing.
        Only called after category scraping is 100% complete.
        Returns the number of products queued.
        """
        # Get already processed SKUs (from previous runs)
        processed_skus = self._get_processed_skus(dedup_file)
        product_details_logger.info(
            f"[Category {category_index}/{total_categories}] "
            f"Already processed from previous runs: {len(processed_skus)} SKUs"
        )

        # Read the completed dedup file (no race condition - file is done)
        try:
            df = read_jsonl(dedup_file)
            total_in_file = len(df)
            product_details_logger.info(
                f"[Category {category_index}/{total_categories}] "
                f"Total products in dedup file: {total_in_file}"
            )
        except Exception as e:
            product_details_logger.error(f"Error reading dedup file: {e}")
            return 0

        # Filter to unprocessed products with valid SKUs
        df_valid = df[(df['sku'].notna()) & (df['sku'] != '')]
        df_unprocessed = df_valid[~df_valid['sku'].isin(processed_skus)]
        unprocessed_count = len(df_unprocessed)

        if unprocessed_count == 0:
            product_details_logger.info(
                f"[Category {category_index}/{total_categories}] "
                f"All products already processed. Skipping."
            )
            return 0

        product_details_logger.info(
            f"[Category {category_index}/{total_categories}] "
            f"Unprocessed products to queue: {unprocessed_count}"
        )

        # Set up output file
        details_filename = self._get_product_details_filename(os.path.basename(dedup_file))
        output_file = os.path.join(Config.PRODUCT_RAW_FOLDER, details_filename)
        self.current_product_details_file = output_file
        category_name = os.path.basename(dedup_file).replace('.jsonl', '').replace('dedup_', '')

        # Queue all unprocessed products
        products = df_unprocessed.to_dict('records')
        queued = 0

        for product in products:
            sku = product.get('sku', '')
            if not sku:
                continue

            queue_item = {
                'url_slug': product.get('url_slug', ''),
                'sku': sku,
                'category_data': product,
                'output_file': output_file,
                'category_index': category_index,
                'total_categories': total_categories,
                'category_name': category_name,
            }
            self.product_queue.put(queue_item)
            queued += 1

            # Log progress every 500 products queued
            if queued % 500 == 0:
                product_details_logger.info(
                    f"[Category {category_index}/{total_categories}] "
                    f"Queued {queued}/{unprocessed_count} products | Queue size: {self.product_queue.qsize()}"
                )

        return queued

    def _wait_for_queue_to_drain(self, category_index: int, total_categories: int):
        """Wait for the product queue to be fully processed."""
        product_details_logger.info(
            f"[Category {category_index}/{total_categories}] "
            f"Waiting for all products to be processed..."
        )

        last_log_time = time.time()
        while not self.product_queue.empty():
            # Log progress every 30 seconds
            if time.time() - last_log_time > 30:
                product_details_logger.info(
                    f"[Category {category_index}/{total_categories}] "
                    f"Queue: {self.product_queue.qsize()} | Processed: {self.products_processed} | "
                    f"Records: {self.total_records_scrapped}"
                )
                last_log_time = time.time()
            time.sleep(1)

        # Wait a bit more for any in-flight processing
        time.sleep(2)

        product_details_logger.info(
            f"[Category {category_index}/{total_categories}] "
            f"All products processed for this category"
        )

    def run_category_only(self, max_pages_per_category: Optional[int] = None):
        """Run only the category scraper."""
        profiler.start_session()

        category_logger.info("\n" + "=" * 70)
        category_logger.info("NOON SCRAPER - CATEGORY ONLY MODE")
        category_logger.info("=" * 70)
        Config.print_config()

        categories = read_categories_from_csv(self.input_csv)
        if not categories:
            category_logger.error("No categories found. Exiting.")
            profiler.end_session()
            return

        # Reset dedup tracking for fresh run
        self.dedup_skus_written = {}
        self.dedup_header_written = {}

        self.total_categories = len(categories)

        for i, category_url in enumerate(categories, 1):
            self.current_category_index = i
            self.current_category_name = category_url.split('/')[-2] if category_url.endswith('/') else category_url.split('/')[-1]

            category_logger.info(f"\n{'='*70}")
            category_logger.info(f"[Category {i}/{len(categories)}] STARTING")
            category_logger.info(f"URL: {category_url}")
            category_logger.info(f"{'='*70}")

            # Set up dedup file
            category_filename = extract_filename_from_url(category_url)
            dedup_filename = f"dedup_{category_filename}"
            self.current_dedup_file = os.path.join(Config.CATEGORY_DEDUP_FOLDER, dedup_filename)
            self.category_dedup_buffer = []

            with profile_step(f"Scrape category {i}"):
                result = self.category_scraper.scrape_category(
                    category_url=category_url,
                    output_folder=Config.CATEGORY_RAW_FOLDER,
                    max_pages=max_pages_per_category,
                    on_batch_written=self._on_category_batch_written
                )

            self.scrape_results.append(result)

            # Flush remaining dedup buffer
            if self.category_dedup_buffer:
                self._flush_dedup_buffer()

            category_logger.info(f"[Category {i}/{len(categories)}] COMPLETED")

            if i < len(categories):
                time.sleep(3)

        profiler.end_session()
        self._print_category_only_summary()

    def run_products_only(self):
        """Run only the product scraper on existing dedup files."""
        profiler.start_session()

        product_details_logger.info("\n" + "=" * 70)
        product_details_logger.info("NOON SCRAPER - PRODUCTS ONLY MODE")
        product_details_logger.info("=" * 70)
        Config.print_config()

        # Reset all counters and state for fresh run
        self.products_processed = 0
        self.total_records_scrapped = 0

        # Find dedup files that haven't been fully processed
        dedup_folder = Config.CATEGORY_DEDUP_FOLDER
        if not os.path.exists(dedup_folder):
            product_details_logger.error(f"Dedup folder not found: {dedup_folder}")
            profiler.end_session()
            return

        dedup_files = [f for f in os.listdir(dedup_folder)
                       if f.endswith('.jsonl')]

        if not dedup_files:
            product_details_logger.info("No dedup files found to process.")
            profiler.end_session()
            return

        self.total_categories = len(dedup_files)
        product_details_logger.info(f"\nFound {len(dedup_files)} dedup files to process:\n")

        # Reset shutdown event for new run
        self.shutdown_event.clear()
        self.processing_active = True

        # Start processor thread (NOT daemon - we want it to complete)
        self.product_processing_thread = threading.Thread(target=self._product_processor)
        self.product_processing_thread.daemon = False  # Don't kill on main thread exit
        self.product_processing_thread.start()

        total_queued = 0
        total_to_process = 0

        for i, filename in enumerate(dedup_files, 1):
            self.current_category_index = i
            dedup_path = os.path.join(dedup_folder, filename)

            product_details_logger.info(f"\n{'='*60}")
            product_details_logger.info(f"[Category {i}/{len(dedup_files)}] Processing: {filename}")
            product_details_logger.info(f"{'='*60}")

            # Set up output file
            details_filename = self._get_product_details_filename(filename)
            self.current_product_details_file = os.path.join(Config.PRODUCT_RAW_FOLDER, details_filename)

            # Get already processed SKUs
            processed_skus = self._get_processed_skus(dedup_path)
            product_details_logger.info(f"[Category {i}/{len(dedup_files)}] Already processed: {len(processed_skus)} SKUs")

            # Read and process in chunks
            try:
                df = read_jsonl(dedup_path)
                total_products = len(df)
                product_details_logger.info(f"[Category {i}/{len(dedup_files)}] Total products in file: {total_products}")

                # Filter unprocessed - also filter out empty SKUs
                df_unprocessed = df[(~df['sku'].isin(processed_skus)) & (df['sku'].notna()) & (df['sku'] != '')]
                unprocessed_count = len(df_unprocessed)
                total_to_process += unprocessed_count
                product_details_logger.info(f"[Category {i}/{len(dedup_files)}] Unprocessed products: {unprocessed_count}")

                if unprocessed_count == 0:
                    product_details_logger.info(f"[Category {i}/{len(dedup_files)}] All products already processed. Skipping.")
                    continue

                # Queue ALL products for processing
                products = df_unprocessed.to_dict('records')
                for product in products:
                    sku = product.get('sku', '')
                    if not sku:
                        continue

                    queue_item = {
                        'url_slug': product.get('url_slug', ''),
                        'sku': sku,
                        'category_data': product,
                        'output_file': self.current_product_details_file,
                        'category_index': i,
                        'total_categories': len(dedup_files),
                        'category_name': filename,
                    }
                    self.product_queue.put(queue_item)
                    total_queued += 1

                product_details_logger.info(
                    f"[Category {i}/{len(dedup_files)}] Queued {unprocessed_count} products | "
                    f"Total queued: {total_queued} | Queue size: {self.product_queue.qsize()}"
                )

            except Exception as e:
                product_details_logger.error(f"Error processing {filename}: {e}")
                import traceback
                product_details_logger.error(traceback.format_exc())

        product_details_logger.info(f"\nAll files processed - queued {total_queued} products total")
        product_details_logger.info(f"Waiting for processor to complete...")

        # Stop processing (this will wait for queue to drain)
        self._stop_product_scraper()
        profiler.end_session()

        product_details_logger.info("\n" + "=" * 70)
        product_details_logger.info("PRODUCTS ONLY MODE COMPLETE")
        product_details_logger.info(f"Total products to process: {total_to_process}")
        product_details_logger.info(f"Total products queued: {total_queued}")
        product_details_logger.info(f"Total products processed: {self.products_processed}")
        product_details_logger.info(f"Total records scraped: {self.total_records_scrapped}")
        if total_queued > 0:
            product_details_logger.info(f"Processing rate: {self.products_processed / total_queued * 100:.1f}%")
        product_details_logger.info("=" * 70)

    def _update_audit_tables(self, result: Dict):
        """Update audit tables after processing a category."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        filename = result.get('filename', '')
        source_url = result.get('source_url', '')

        # Update category raw audit
        raw_path = os.path.join(Config.CATEGORY_RAW_FOLDER, filename)
        if os.path.exists(raw_path):
            self._append_to_audit_table(Config.CATEGORY_RAW_FOLDER, filename, source_url, raw_path, timestamp)

        # Update category dedup audit
        if self.current_dedup_file and os.path.exists(self.current_dedup_file):
            dedup_filename = os.path.basename(self.current_dedup_file)
            self._append_to_audit_table(Config.CATEGORY_DEDUP_FOLDER, dedup_filename, source_url,
                                        self.current_dedup_file, timestamp)

        # Update product raw audit
        if self.current_product_details_file and os.path.exists(self.current_product_details_file):
            details_filename = os.path.basename(self.current_product_details_file)
            self._append_to_audit_table(Config.PRODUCT_RAW_FOLDER, details_filename, source_url,
                                        self.current_product_details_file, timestamp)

    def _append_to_audit_table(self, folder: str, filename: str, source_url: str, file_path: str, timestamp: str):
        """Append entry to audit table."""
        audit_path = os.path.join(folder, 'audit_table.csv')
        try:
            if file_path.endswith('.jsonl'):
                df = read_jsonl(file_path)
            else:
                df = pd.read_csv(file_path, on_bad_lines='skip', encoding='utf-8')
            new_row = pd.DataFrame([{
                'filename': filename,
                'source_url': source_url,
                'number_of_records': len(df),
                'data_schema': calculate_data_schema(df),
                'status': 'completed',
                'last_updated': timestamp
            }])
            new_row.to_csv(audit_path, mode='a', header=False, index=False, encoding='utf-8', quoting=1)
        except Exception as e:
            category_logger.error(f"Error updating audit table: {e}")

    def _update_remaining_product_audits(self):
        """Update audit tables for any product detail files that were created."""
        try:
            # Look for all product detail files that were created
            product_raw_dir = Config.PRODUCT_RAW_FOLDER
            if os.path.exists(product_raw_dir):
                for filename in os.listdir(product_raw_dir):
                    if filename.startswith('details_') and filename.endswith('.jsonl'):
                        filepath = os.path.join(product_raw_dir, filename)
                        if os.path.isfile(filepath):
                            # Create a dummy result for the audit update
                            dummy_result = {
                                'filename': filename,
                                'source_url': 'multiple_categories',
                                'success': True
                            }
                            # Update the audit for this product file
                            self._append_to_audit_table(
                                Config.PRODUCT_RAW_FOLDER,
                                filename,
                                'multiple_categories',
                                filepath,
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            )
        except Exception as e:
            category_logger.error(f"Error updating remaining product audits: {e}")

    def _print_summary(self):
        """Print final summary."""
        category_logger.info("\n" + "=" * 70)
        category_logger.info("NOON SCRAPER - COMPLETE")
        category_logger.info("=" * 70)

        total_records = sum(r.get('number_of_records', 0) for r in self.scrape_results)
        successful = sum(1 for r in self.scrape_results if r.get('success'))

        category_logger.info(f"\nCategory Scraper Summary:")
        category_logger.info(f"  - Categories processed: {len(self.scrape_results)}")
        category_logger.info(f"  - Successful: {successful}")
        category_logger.info(f"  - Total records: {total_records}")

        product_details_logger.info(f"\nProduct Scraper Summary:")
        product_details_logger.info(f"  - Products processed: {self.products_processed}")
        product_details_logger.info(f"  - Total records: {self.total_records_scrapped}")

    def _print_category_only_summary(self):
        """Print summary for category only mode."""
        category_logger.info("\n" + "=" * 70)
        category_logger.info("CATEGORY ONLY MODE COMPLETE")
        category_logger.info("=" * 70)

        total_records = sum(r.get('number_of_records', 0) for r in self.scrape_results)
        successful = sum(1 for r in self.scrape_results if r.get('success'))

        category_logger.info(f"\nSummary:")
        category_logger.info(f"  - Categories processed: {len(self.scrape_results)}")
        category_logger.info(f"  - Successful: {successful}")
        category_logger.info(f"  - Total records: {total_records}")
        category_logger.info(f"  - Raw folder: {Config.CATEGORY_RAW_FOLDER}/")
        category_logger.info(f"  - Dedup folder: {Config.CATEGORY_DEDUP_FOLDER}/")
