"""
Product Detail Scraper Module
=============================
Scrapes detailed product information via SKU from product detail API.
Provides enrichment data to merge with category listing data.

Integrated from standalone product_scraper.py with:
- x-visitor-id header for better success rate
- Success/fail counting for statistics
"""

import json
import time
from typing import Dict, List, Optional
from curl_cffi import requests

from config import Config
from utils import logger, image_key_to_url


class ProductDetailScraper:
    """
    Scraper for individual product details via SKU.
    Gets comprehensive product data from the product detail API.
    Used to ENRICH category listing data with additional attributes.

    Features (integrated from standalone scraper):
    - Uses x-visitor-id header for better API success rate
    - Tracks success/fail counts for statistics
    """

    # Refresh session every N products (2 requests each) to stay under
    # the HTTP/2 ~128 streams-per-connection limit imposed by the server.
    _SESSION_REFRESH_EVERY = 60

    def __init__(self):
        self.session = requests.Session()
        # Success/fail tracking (from standalone scraper)
        self.success_count = 0
        self.fail_count = 0
        self._request_count = 0
        logger.info("ProductDetailScraper initialized (with visitor-id support)")

    def get_product(self, url_slug: str, sku: str, category_data: dict = None) -> Optional[Dict]:
        """Fetch product details by URL slug and SKU."""
        # Auto-detect full country config from country_prefix stored in category_data
        country_prefix = (category_data or {}).get('country_prefix', 'uae-en')
        country_cfg = Config.get_country_config(country_prefix)
        site_url = country_cfg['site_url']

        # Remove leading slash if present
        url_slug = url_slug.lstrip('/')

        page_url = f'{site_url}{url_slug}/{sku}/p/'

        logger.debug(f"Fetching product details: {sku} (country={country_prefix})")
        logger.debug(f"Page URL: {page_url}")

        # Proactively recycle the session every N products to stay under
        # the HTTP/2 stream limit (~128 streams per TCP connection).
        self._request_count += 1
        if self._request_count % self._SESSION_REFRESH_EVERY == 0:
            logger.debug(f"Recycling product session at request {self._request_count}")
            self.session = requests.Session()

        try:
            # First visit the page to establish session
            page_resp = self.session.get(page_url, impersonate=Config.CHROME_IMPERSONATE_DETAIL, timeout=Config.REQUEST_TIMEOUT)
        except Exception as e:
            logger.warning(f"Failed to visit page for {sku}: {e}")
            # If HTTP/2 stream error, recycle session and retry
            if 'curl: (92)' in str(e):
                logger.warning(f"HTTP/2 stream reset, recycling session for {sku}")
                self.session = requests.Session()
                self._request_count = 0
                try:
                    self.session.get(page_url, impersonate=Config.CHROME_IMPERSONATE_DETAIL, timeout=Config.REQUEST_TIMEOUT)
                except Exception:
                    pass

        # Headers using auto-detected country config
        headers = {
            'accept': 'application/json',
            'x-locale': country_cfg['locale'],
            'x-mp-country': country_cfg['country'],
            'referer': page_url,
            'x-visitor-id': Config.VISITOR_ID,
        }

        api_url = f'{Config.BASE_API_URL}{url_slug}/{sku}/p/'
        logger.debug(f"API URL: {api_url}")

        try:
            response = self.session.get(
                api_url,
                headers=headers,
                impersonate=Config.CHROME_IMPERSONATE_DETAIL,
                timeout=Config.REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                self.success_count += 1
                logger.debug(f"Successfully fetched product details {sku}")
                return response.json()

            logger.warning(f"Failed to fetch product {sku}: HTTP {response.status_code}")
            self.fail_count += 1
            return None

        except Exception as e:
            # If HTTP/2 stream error, recycle session and retry once
            if 'curl: (92)' in str(e):
                logger.warning(f"HTTP/2 stream reset on API call, recycling session for {sku}")
                self.session = requests.Session()
                self._request_count = 0
                try:
                    response = self.session.get(
                        api_url,
                        headers=headers,
                        impersonate=Config.CHROME_IMPERSONATE_DETAIL,
                        timeout=Config.REQUEST_TIMEOUT
                    )
                    if response.status_code == 200:
                        self.success_count += 1
                        return response.json()
                except Exception:
                    pass
            # If timeout, wait longer and retry once
            if 'curl: (28)' in str(e):
                logger.warning(f"Timeout on API call for {sku}, waiting 15s and retrying...")
                time.sleep(15)
                try:
                    response = self.session.get(
                        api_url,
                        headers=headers,
                        impersonate=Config.CHROME_IMPERSONATE_DETAIL,
                        timeout=Config.REQUEST_TIMEOUT
                    )
                    if response.status_code == 200:
                        self.success_count += 1
                        return response.json()
                except Exception:
                    pass
            logger.error(f"Failed to fetch product {sku}: {e}")
            self.fail_count += 1
            return None

    def get_stats(self) -> Dict[str, int]:
        """Get success/fail statistics."""
        total = self.success_count + self.fail_count
        success_rate = (self.success_count / total * 100) if total > 0 else 0
        return {
            'success_count': self.success_count,
            'fail_count': self.fail_count,
            'total': total,
            'success_rate': success_rate
        }

    def extract_all_product_rows(self, data: Dict, category_data: Dict = None) -> List[Dict]:
        """
        Extract ALL product variant rows from product details API.
        Returns multiple rows (one per variant+offer combination).
        Each row includes category data merged with product details.

        Args:
            data: Raw API response from product details
            category_data: Original data from category API to merge with each row

        Returns:
            List of dicts, one per variant/size/offer combination
        """
        products = []

        product = data.get('product', {})
        if not product:
            logger.warning("No product data found in response")
            return products

        # Get specifications
        specs = {}
        for spec in product.get('specifications', []):
            specs[spec.get('code', '')] = spec.get('value', '')

        breadcrumb_path = ' > '.join([b['name'] for b in product.get('breadcrumbs', [])])
        features = ' | '.join(product.get('feature_bullets', []))
        images = product.get('image_keys', [])

        # Extract color variants
        color_variants = []
        groups = product.get('groups', [])
        for group in groups:
            if group.get('code') == 'color':
                for option in group.get('options', []):
                    color_variants.append({
                        'color': option.get('name'),
                        'sku': option.get('sku'),
                        'available': option.get('is_available') == 1,
                        'url': option.get('url'),
                        'image_url': image_key_to_url(option.get('image_key')),
                    })

        # Extract Frequently Bought Together
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

        # Base info from product details (shared across all variants)
        base_info = {
            'detail_config_sku': product.get('sku', ''),
            'detail_product_title': product.get('product_title', ''),
            'detail_brand': product.get('brand', ''),
            'detail_long_description': product.get('long_description', ''),
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

        # Process each variant and offer
        for variant in product.get('variants', []):
            variant_sku = variant.get('sku')
            variant_size = variant.get('variant')

            offers = variant.get('offers', [])

            if not offers:
                # Variant with no offers
                variant_row = {}
                # Add category data first
                if category_data:
                    variant_row.update(category_data)
                # Add base product details
                variant_row.update(base_info)
                variant_row['detail_variant_sku'] = variant_sku
                variant_row['detail_size'] = variant_size
                products.append(variant_row)
                continue

            # Create a row for each offer
            for offer in offers:
                variant_row = {}

                # Add category data first (original attributes)
                if category_data:
                    variant_row.update(category_data)

                # Add base product details
                variant_row.update(base_info)

                # Add variant-specific info
                variant_row['detail_variant_sku'] = variant_sku
                variant_row['detail_size'] = variant_size
                variant_row['detail_offer_code'] = offer.get('offer_code')
                variant_row['detail_offer_sku'] = offer.get('sku')
                variant_row['detail_price'] = offer.get('price')
                variant_row['detail_currency'] = offer.get('currency', 'AED')
                variant_row['detail_sale_price'] = offer.get('sale_price')
                variant_row['detail_stock'] = offer.get('stock')
                variant_row['detail_is_buyable'] = offer.get('is_buyable')
                variant_row['detail_is_bestseller'] = offer.get('is_bestseller')
                variant_row['detail_store_name'] = offer.get('store_name')
                variant_row['detail_partner_code'] = offer.get('partner_code')

                # Seller ratings
                seller_ratings = offer.get('partner_ratings_sellerlab', {})
                variant_row['detail_seller_rating'] = seller_ratings.get('partner_rating')
                variant_row['detail_seller_rating_count'] = seller_ratings.get('num_of_rating')
                variant_row['detail_seller_positive_rating'] = seller_ratings.get('positive_seller_rating')
                variant_row['detail_seller_as_described_rate'] = seller_ratings.get('as_described_rate')

                # Delivery info
                variant_row['detail_estimated_delivery'] = offer.get('estimated_delivery')
                variant_row['detail_estimated_delivery_date'] = offer.get('estimated_delivery_date')
                variant_row['detail_shipping_fee_message'] = offer.get('shipping_fee_message')

                # Flags
                flags = offer.get('flags', [])
                variant_row['detail_is_marketplace'] = 'marketplace' in flags
                variant_row['detail_is_global'] = 'global' in flags
                variant_row['detail_is_free_delivery'] = 'free_delivery_eligible' in flags
                variant_row['detail_flags_json'] = json.dumps(flags)
                variant_row['detail_bnpl_available'] = bool(offer.get('bnplBanners', []))
                variant_row['detail_cashback_available'] = bool(offer.get('cobrand_cashback_data', {}))

                products.append(variant_row)

        logger.debug(f"Extracted {len(products)} variant rows")
        return products

    def get_all_product_rows(self, url_slug: str, sku: str, category_data: Dict = None) -> List[Dict]:
        """
        Fetch product details and extract ALL variant rows.
        Each row contains category data + product detail data.

        Args:
            url_slug: Product URL slug (from category data)
            sku: Product SKU
            category_data: Original data from category API to merge

        Returns:
            List of dicts, one per variant/size/offer combination
        """
        data = self.get_product(url_slug, sku, category_data)
        if data:
            return self.extract_all_product_rows(data, category_data)
        return []
