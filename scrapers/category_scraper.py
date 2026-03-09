"""
Category List Scraper Module
============================
Scrapes products from category listing API.
"""

import os
import time
import random
from typing import Dict, List, Optional, Tuple, Callable
from curl_cffi import requests

from config import Config
from utils import (
    logger,
    profile_step,
    profile_function,
    extract_filename_from_url,
    extract_category_path_from_url,
    extract_country_prefix_from_url,
    image_key_to_url,
    append_jsonl,
)


class CategoryListScraper:
    """
    Scraper for category listing API.
    Gets product data from the category/search pages.
    """

    def __init__(self):
        self.base_url = Config.BASE_API_URL
        self.session = requests.Session()
        self.visitor_id = Config.VISITOR_ID
        self.base_cookies = Config.get_base_cookies()
        # Set per scrape_category call based on detected country
        self._country_cfg = Config.get_country_config('uae-en')
        self._country_prefix = 'uae-en'

        logger.info("CategoryListScraper initialized")
        logger.debug(f"Using VISITOR_ID: {self.visitor_id}")

    @profile_function
    def get_fresh_session(self) -> bool:
        """Establish fresh session with cookies for the current country."""
        try:
            site_url = self._country_cfg['site_url']
            logger.info(f"Getting fresh session for {site_url}...")

            with profile_step("HTTP request: session init"):
                self.session.get(
                    site_url,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    },
                    cookies=Config.get_base_cookies(self._country_cfg),
                    impersonate=Config.CHROME_IMPERSONATE,
                    timeout=15
                )

            logger.info(f"Session established with {len(self.session.cookies)} cookies")
            time.sleep(2)
            return True

        except Exception as e:
            logger.warning(f"Session failed: {e}")
            return False

    def build_product_url(self, url_slug: str, sku: str, offer_code: str) -> str:
        """Build full product URL."""
        if url_slug and sku:
            return f"{self._country_cfg['site_url']}{url_slug}/{sku}/p/?o={offer_code}"
        return ''

    def extract_all_attributes(self, product: Dict) -> Dict:
        """
        Extract ALL attributes from a product in the category listing.
        """
        flat = {}

        # Basic Info
        flat['sku'] = product.get('sku', '')
        flat['catalog_sku'] = product.get('catalog_sku', '')
        flat['offer_code'] = product.get('offer_code', '')
        flat['name'] = product.get('name', '')
        flat['brand'] = product.get('brand', '')
        flat['url_slug'] = product.get('url', '')

        # Build full product URL
        flat['product_url'] = self.build_product_url(
            flat['url_slug'],
            flat['sku'],
            flat['offer_code']
        )

        # Pricing
        flat['price'] = product.get('price', '')
        flat['sale_price'] = product.get('sale_price', '')
        flat['was_price'] = product.get('was_price', '')

        # Calculate discount percentage
        if flat['price'] and flat['sale_price']:
            try:
                discount = ((flat['price'] - flat['sale_price']) / flat['price']) * 100
                flat['discount_percentage'] = round(discount, 1)
            except Exception:
                flat['discount_percentage'] = ''
        else:
            flat['discount_percentage'] = ''

        # Images (up to 10)
        image_keys = product.get('image_keys', [])
        image_key = product.get('image_key', '')

        all_image_keys = []
        if image_keys:
            all_image_keys.extend(image_keys)
        elif image_key:
            all_image_keys.append(image_key)

        for i in range(10):
            if i < len(all_image_keys):
                img_key = all_image_keys[i]
                flat[f'image_{i+1}'] = image_key_to_url(img_key)
                flat[f'image_{i+1}_key'] = img_key
            else:
                flat[f'image_{i+1}'] = ''
                flat[f'image_{i+1}_key'] = ''

        # Ratings & Reviews
        flat['rating'] = product.get('rating', '')
        flat['reviews'] = product.get('reviews', '')

        product_rating = product.get('product_rating', {})
        if product_rating:
            flat['rating_value'] = product_rating.get('value', '')
            flat['rating_count'] = product_rating.get('count', '')
        else:
            flat['rating_value'] = ''
            flat['rating_count'] = ''

        # Stock & Availability
        flat['availability'] = product.get('availability', '')
        flat['is_buyable'] = product.get('is_buyable', '')
        flat['is_out_of_stock'] = product.get('is_out_of_stock', '')
        flat['stock_quantity'] = product.get('stock_quantity', '')

        # Badges & Flags
        flat['is_bestseller'] = product.get('is_bestseller', '')
        flat['is_express'] = product.get('is_express', '')
        flat['is_fashion'] = product.get('is_fashion', '')

        flags = product.get('flags', [])
        flat['flags'] = '|'.join(flags) if flags else ''
        flat['flags_count'] = len(flags)

        # Deals & Discounts
        deal_tag = product.get('deal_tag', {})
        if deal_tag:
            flat['deal_tag_text'] = deal_tag.get('text', '')
            flat['deal_tag_color'] = deal_tag.get('color', '')
        else:
            flat['deal_tag_text'] = ''
            flat['deal_tag_color'] = ''

        flat['discount_tag_code'] = product.get('discount_tag_code', '')
        flat['discount_tag_title'] = product.get('discount_tag_title', '')

        # Nudges
        nudges = product.get('nudges', [])
        if nudges:
            nudge_texts = [n.get('text', '') for n in nudges if n.get('text')]
            flat['nudges'] = '|'.join(nudge_texts)
            flat['nudges_count'] = len(nudges)

            if len(nudges) > 0:
                flat['nudge_1_text'] = nudges[0].get('text', '')
                flat['nudge_1_type'] = nudges[0].get('type', '')
        else:
            flat['nudges'] = ''
            flat['nudges_count'] = 0
            flat['nudge_1_text'] = ''
            flat['nudge_1_type'] = ''

        # Delivery
        flat['delivery_label'] = product.get('delivery_label', '')
        flat['estimated_delivery_date'] = product.get('estimated_delivery_date', '')
        flat['express_delivery'] = product.get('express_delivery', '')

        # Seller Info
        flat['seller_code'] = product.get('seller_code', '')
        flat['seller_name'] = product.get('seller_name', '')
        flat['sold_by'] = product.get('sold_by', '')

        # Category Info
        flat['category'] = product.get('category', '')
        flat['subcategory'] = product.get('subcategory', '')
        flat['catalog_key'] = product.get('catalog_key', '')

        # Product Specs
        flat['model_number'] = product.get('model_number', '')
        flat['model_name'] = product.get('model_name', '')
        flat['item_type'] = product.get('item_type', '')

        # Attributes (variants)
        attributes = product.get('attributes', [])
        if attributes:
            for attr in attributes:
                attr_name = attr.get('name', '').lower().replace(' ', '_')
                attr_value = attr.get('value', '')
                if attr_name and attr_value:
                    flat[f'attr_{attr_name}'] = attr_value

        # Variants
        variants = product.get('variants', [])
        flat['has_variants'] = len(variants) > 0
        flat['variant_count'] = len(variants)

        # Additional Fields
        flat['position'] = product.get('position', '')
        flat['rank'] = product.get('rank', '')
        flat['boost'] = product.get('boost', '')
        flat['parent_sku'] = product.get('parent_sku', '')
        flat['product_type'] = product.get('product_type', '')

        # Sponsor/Ad Info
        flat['is_sponsored'] = product.get('is_sponsored', '')
        flat['sponsor_id'] = product.get('sponsor_id', '')

        # Metadata
        flat['created_at'] = product.get('created_at', '')
        flat['updated_at'] = product.get('updated_at', '')

        # Catch remaining fields
        for key, value in product.items():
            if key not in flat and not isinstance(value, (dict, list)):
                flat[f'extra_{key}'] = value

        return flat

    def scrape_page(self, category_path: str, page: int) -> Dict:
        """Scrape a single page."""
        referer = f"{self._country_cfg['site_url']}{category_path}?page={page}"
        headers = Config.get_request_headers(referer, self._country_cfg)

        try:
            with profile_step(f"HTTP request: page {page}"):
                response = self.session.get(
                    f"{self.base_url}{category_path}",
                    params={'page': str(page)},
                    headers=headers,
                    impersonate=Config.CHROME_IMPERSONATE,
                    timeout=Config.REQUEST_TIMEOUT
                )

            if response.status_code != 200:
                return {
                    'success': False,
                    'error': f'HTTP {response.status_code}',
                    'page': page
                }

            with profile_step(f"JSON parse: page {page}"):
                data = response.json()

            with profile_step(f"Extract attributes: page {page}"):
                products = []
                for hit in data.get('hits', []):
                    product = self.extract_all_attributes(hit)
                    product['country_prefix'] = self._country_prefix
                    products.append(product)

            return {
                'success': True,
                'page': page,
                'products': products,
                'total_hits': data.get('nbHits', 0),
                'total_pages': data.get('nbPages', 0),
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'page': page
            }

    def scrape_category(
        self,
        category_url: str,
        output_folder: str,
        max_pages: Optional[int] = None,
        delay: Optional[Tuple[float, float]] = None,
        on_batch_written: Optional[Callable[[List[Dict], str], None]] = None,
        start_page: int = 1,
        output_path_override: Optional[str] = None,
        on_page_scraped: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict:
        """
        Scrape a category and save to CSV with batch writing.

        Args:
            start_page: Page to resume from (default 1). When > 1, page 1 is
                        fetched only to obtain total_pages, its products are skipped.
            output_path_override: Write to this file instead of generating a new
                                  timestamped filename (used when resuming).
            on_page_scraped: Called after every successful page with
                             (page, total_pages, output_path) for checkpoint saving.

        Returns dict with filename, record_count, and success status.
        """
        if delay is None:
            delay = Config.get_delay_range()

        # Detect country from URL and set country-specific config
        self._country_prefix = extract_country_prefix_from_url(category_url)
        self._country_cfg = Config.get_country_config(self._country_prefix)

        category_path = extract_category_path_from_url(category_url)
        filename = extract_filename_from_url(category_url)
        output_path = output_path_override or os.path.join(output_folder, filename)

        resuming = start_page > 1
        logger.info("=" * 70)
        logger.info(f"{'Resuming' if resuming else 'Scraping'}: {category_url}")
        if resuming:
            logger.info(f"Resuming from page {start_page}")
        logger.info(f"Country: {self._country_prefix} → {self._country_cfg['site_url']}")
        logger.info(f"Output: {output_path}")
        logger.debug(f"Category path: {category_path}")
        logger.info("=" * 70)

        # Get fresh session
        self.get_fresh_session()

        # Always fetch page 1 to get total_pages (even when resuming)
        logger.info("Analyzing category...")

        with profile_step("First page analysis"):
            first_page = self.scrape_page(category_path, 1)

        if not first_page['success']:
            logger.error(f"Failed: {first_page.get('error')}")
            return {
                'success': False,
                'filename': filename,
                'source_url': category_url,
                'number_of_records': 0,
                'error': first_page.get('error')
            }

        total_pages = first_page['total_pages']
        total_products_expected = first_page['total_hits']

        if max_pages:
            total_pages = min(total_pages, max_pages)

        logger.info(f"Found {total_products_expected} products across {total_pages} pages")

        # When resuming, skip page 1 products (already saved) and treat header as written
        if resuming:
            batch_products = []
            total_products_scraped = 0
            header_written = True  # File already exists from previous run
            logger.info(f"Skipping page 1 products (resuming from page {start_page})")
        else:
            batch_products = first_page['products']
            total_products_scraped = len(batch_products)
            header_written = False

            # Check if we need to write first batch
            if len(batch_products) >= Config.BATCH_SIZE:
                with profile_step("Write batch to CSV"):
                    self._write_batch(batch_products, output_path, write_header=True)
                header_written = True
                if on_batch_written:
                    on_batch_written(batch_products.copy(), output_path)
                if on_page_scraped:
                    on_page_scraped(1, total_pages, output_path)
                batch_products = []

            logger.info(f"Page 1/{total_pages} - {len(first_page['products'])} products (Total: {total_products_scraped})")

        failed_pages = []

        # HTTP/2 connections are recycled by the server after ~128 streams.
        # Refresh the session every 100 pages to avoid hitting this limit mid-run.
        SESSION_REFRESH_INTERVAL = 100

        # Scrape remaining pages (start from start_page if resuming, else from 2)
        loop_start = max(2, start_page)
        for page in range(loop_start, total_pages + 1):
            # Proactively refresh session every SESSION_REFRESH_INTERVAL pages
            if (page - loop_start) % SESSION_REFRESH_INTERVAL == 0 and page > loop_start:
                logger.info(f"Refreshing session at page {page} (every {SESSION_REFRESH_INTERVAL} pages)")
                self.get_fresh_session()

            # Random delay
            wait = random.uniform(*delay)
            logger.debug(f"Waiting {wait:.2f}s before page {page}")
            time.sleep(wait)

            result = self.scrape_page(category_path, page)

            # If HTTP/2 stream error, refresh session and retry once
            if not result['success'] and 'curl: (92)' in str(result.get('error', '')):
                logger.warning(f"Page {page}/{total_pages} - HTTP/2 stream reset, refreshing session and retrying...")
                self.get_fresh_session()
                time.sleep(random.uniform(*delay))
                result = self.scrape_page(category_path, page)

            # If timeout, wait longer and retry once
            elif not result['success'] and 'curl: (28)' in str(result.get('error', '')):
                logger.warning(f"Page {page}/{total_pages} - Timeout, waiting 15s and retrying...")
                time.sleep(15)
                result = self.scrape_page(category_path, page)

            if result['success']:
                batch_products.extend(result['products'])
                total_products_scraped += len(result['products'])
                logger.info(f"Page {page}/{total_pages} - {len(result['products'])} products (Total: {total_products_scraped})")

                # Write batch if threshold reached
                if len(batch_products) >= Config.BATCH_SIZE:
                    with profile_step("Write batch to CSV"):
                        self._write_batch(batch_products, output_path, write_header=not header_written)
                    header_written = True
                    if on_batch_written:
                        on_batch_written(batch_products.copy(), output_path)
                    if on_page_scraped:
                        on_page_scraped(page, total_pages, output_path)
                    batch_products = []
                    logger.info(f"Batch written to disk ({Config.BATCH_SIZE} products)")
                elif on_page_scraped:
                    on_page_scraped(page, total_pages, output_path)
            else:
                failed_pages.append(page)
                logger.error(f"Page {page}/{total_pages} - Failed: {result.get('error')}")

        # Write remaining products
        if batch_products:
            with profile_step("Write final batch to CSV"):
                self._write_batch(batch_products, output_path, write_header=not header_written)
            if on_batch_written:
                on_batch_written(batch_products.copy(), output_path)
            if on_page_scraped:
                on_page_scraped(total_pages, total_pages, output_path)
            logger.info(f"Final batch written ({len(batch_products)} products)")

        logger.info("=" * 70)
        logger.info(f"COMPLETE: {total_products_scraped} products scraped")
        if failed_pages:
            logger.warning(f"Failed pages: {failed_pages}")
        logger.info("=" * 70)

        return {
            'success': True,
            'filename': filename,
            'source_url': category_url,
            'number_of_records': total_products_scraped,
            'failed_pages': failed_pages
        }

    def _write_batch(self, products: List[Dict], output_path: str, write_header: bool):
        """Write a batch of products to JSONL file."""
        if not products:
            return

        mode = 'w' if write_header else 'a'
        append_jsonl(products, output_path, mode=mode)

        logger.debug(f"Wrote {len(products)} products to {output_path}")
