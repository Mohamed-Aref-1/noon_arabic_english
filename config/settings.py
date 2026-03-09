"""
Settings Module
===============
Loads configuration from .env file for easy cookie/config updates.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration class that loads all settings from .env"""

    # Visitor IDs (update in .env when expired)
    VISITOR_ID: str = os.getenv('VISITOR_ID', '8b76a0b9-1549-483d-b87b-2982d89f75a1')
    VISITOR_ID_ALT: str = os.getenv('VISITOR_ID_ALT', '18329ffc-072d-4f43-b878-1674a68b278c')

    # Locale and Region
    LOCALE: str = os.getenv('LOCALE', 'en-ae')
    REGION: str = os.getenv('REGION', 'ecom')
    COUNTRY: str = os.getenv('COUNTRY', 'ae')

    # Location Headers
    LAT: str = os.getenv('LAT', '251998495')
    LNG: str = os.getenv('LNG', '552715985')
    ZONE_CODE: str = os.getenv('ZONE_CODE', 'AE_DXB-S14')
    ROCKET_ZONE_CODE: str = os.getenv('ROCKET_ZONE_CODE', 'W00068765A')

    # Scraper Mode: BOTH (default), CATEGORY_ONLY, PRODUCTS_ONLY
    SCRAPER_MODE: str = os.getenv('SCRAPER_MODE', 'BOTH').upper()

    # Scraper Settings
    BATCH_SIZE: int = int(os.getenv('BATCH_SIZE', '500'))
    MAX_QUEUE_SIZE: int = int(os.getenv('MAX_QUEUE_SIZE', '1000'))
    REQUEST_DELAY_MIN: float = float(os.getenv('REQUEST_DELAY_MIN', '2'))
    REQUEST_DELAY_MAX: float = float(os.getenv('REQUEST_DELAY_MAX', '4'))
    REQUEST_TIMEOUT: int = int(os.getenv('REQUEST_TIMEOUT', '20'))

    # Chrome Impersonation
    CHROME_IMPERSONATE: str = os.getenv('CHROME_IMPERSONATE', 'chrome110')
    CHROME_IMPERSONATE_DETAIL: str = os.getenv('CHROME_IMPERSONATE_DETAIL', 'chrome120')

    # Output Folders - Category files
    CATEGORY_RAW_FOLDER: str = os.getenv('CATEGORY_RAW_FOLDER', 'noon_category_raw')
    CATEGORY_DEDUP_FOLDER: str = os.getenv('CATEGORY_DEDUP_FOLDER', 'noon_category_dedup')

    # Output Folders - Product details files
    PRODUCT_RAW_FOLDER: str = os.getenv('PRODUCT_RAW_FOLDER', 'noon_product_raw')
    PRODUCT_DEDUP_FOLDER: str = os.getenv('PRODUCT_DEDUP_FOLDER', 'noon_product_dedup')

    # Input/Output Files
    INPUT_CSV: str = os.getenv('INPUT_CSV', 'categories_to_scrape.csv')
    LOG_FILE: str = os.getenv('LOG_FILE', 'scraper_profiling.log')

    # API Base URLs
    BASE_API_URL: str = "https://www.noon.com/_vs/nc/mp-customer-catalog-api/api/v3/u/"
    BASE_SITE_URL: str = "https://www.noon.com/uae-en/"

    # Full per-country config auto-detected from URL prefix.
    # locale/country/zone/lat/lng are all set here — no need to touch .env when switching countries.
    # Zone codes confirmed from browser network inspection (update if noon changes them).
    COUNTRY_CONFIG: dict = {
        'uae-en': {
            'locale': 'en-ae', 'country': 'ae',
            'zone_code': 'AE_DXB-S14', 'rocket_zone': 'W00068765A',
            'lat': '251998495', 'lng': '552715985',
            'site_url': 'https://www.noon.com/uae-en/',
        },
        'uae-ar': {
            'locale': 'ar-ae', 'country': 'ae',
            'zone_code': 'AE_DXB-S14', 'rocket_zone': 'W00068765A',
            'lat': '251998495', 'lng': '552715985',
            'site_url': 'https://www.noon.com/uae-ar/',
        },
        'saudi-en': {
            'locale': 'en-sa', 'country': 'sa',
            'zone_code': 'SA-RUH-S17', 'rocket_zone': 'W00083496A',
            'lat': '2473113820', 'lng': '466700814',
            'site_url': 'https://www.noon.com/saudi-en/',
        },
        'saudi-ar': {
            'locale': 'ar-sa', 'country': 'sa',
            'zone_code': 'SA-RUH-S17', 'rocket_zone': 'W00083496A',
            'lat': '2473113820', 'lng': '466700814',
            'site_url': 'https://www.noon.com/saudi-ar/',
        },
        'egypt-en': {
            'locale': 'en-eg', 'country': 'eg',
            'zone_code': 'EG_CAI-S1', 'rocket_zone': 'W00068820A',
            'lat': '300522430', 'lng': '312357000',
            'site_url': 'https://www.noon.com/egypt-en/',
        },
        'egypt-ar': {
            'locale': 'ar-eg', 'country': 'eg',
            'zone_code': 'EG_CAI-S1', 'rocket_zone': 'W00068820A',
            'lat': '300522430', 'lng': '312357000',
            'site_url': 'https://www.noon.com/egypt-ar/',
        },
        'jordan-en': {
            'locale': 'en-jo', 'country': 'jo',
            'zone_code': 'JO-AMM-S1', 'rocket_zone': 'W00083500A',
            'lat': '319539000', 'lng': '359106000',
            'site_url': 'https://www.noon.com/jordan-en/',
        },
        'jordan-ar': {
            'locale': 'ar-jo', 'country': 'jo',
            'zone_code': 'JO-AMM-S1', 'rocket_zone': 'W00083500A',
            'lat': '319539000', 'lng': '359106000',
            'site_url': 'https://www.noon.com/jordan-ar/',
        },
    }

    @classmethod
    def get_country_config(cls, url_prefix: str) -> dict:
        """Return full country config for a given URL prefix (e.g. 'saudi-ar').
        Falls back to uae-en if prefix is unknown."""
        return cls.COUNTRY_CONFIG.get(url_prefix, cls.COUNTRY_CONFIG['uae-en'])

    # Raw cookie string from browser (paste full cookie header value here)
    COOKIES_RAW: str = os.getenv('COOKIES_RAW', '')

    @classmethod
    def parse_raw_cookies(cls) -> dict:
        """Parse COOKIES_RAW string into a dict."""
        if not cls.COOKIES_RAW:
            return {}
        cookies = {}
        for part in cls.COOKIES_RAW.split(';'):
            part = part.strip()
            if '=' in part:
                k, _, v = part.partition('=')
                cookies[k.strip()] = v.strip()
        return cookies

    # Akamai Bot Manager cookies — these are IP/fingerprint-bound and must not be
    # forwarded from a different session, or Akamai sends RST_STREAM INTERNAL_ERROR.
    _AKAMAI_COOKIE_KEYS = {'bm_mi', 'ak_bmsc', 'bm_sv', 'bm_sz'}

    @classmethod
    def get_base_cookies(cls, country_cfg: dict = None) -> dict:
        """Get base cookies, using country_cfg locale when provided."""
        locale = country_cfg['locale'] if country_cfg else cls.LOCALE
        base = {
            'visitor_id': cls.VISITOR_ID,
            'visitorId': cls.VISITOR_ID_ALT,
            'x-available-ae': cls.REGION,
            'nloc': locale,
        }
        # Merge raw browser cookies (they override base, then restore locale-specific nloc)
        # Strip Akamai bot-management cookies — they are IP/fingerprint-bound and cause
        # HTTP/2 RST_STREAM INTERNAL_ERROR when sent from a different session.
        raw = cls.parse_raw_cookies()
        if raw:
            raw = {k: v for k, v in raw.items() if k not in cls._AKAMAI_COOKIE_KEYS}
            base.update(raw)
            base['nloc'] = locale
        return base

    @classmethod
    def get_request_headers(cls, referer: str = '', country_cfg: dict = None) -> dict:
        """Get standard request headers, using country_cfg values when provided."""
        cfg = country_cfg or {}
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache, max-age=0, must-revalidate, no-store',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not A=Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-locale': cfg.get('locale', cls.LOCALE),
            'x-mp-country': cfg.get('country', cls.COUNTRY),
            'x-platform': 'web',
            'x-lat': cfg.get('lat', cls.LAT),
            'x-lng': cfg.get('lng', cls.LNG),
            'x-ecom-zonecode': cfg.get('zone_code', cls.ZONE_CODE),
            'x-rocket-enabled': 'true',
            'x-rocket-zonecode': cfg.get('rocket_zone', cls.ROCKET_ZONE_CODE),
            'x-border-enabled': 'true',
            'x-visitor-id': cls.VISITOR_ID,
        }

        if referer:
            headers['referer'] = referer

        return headers

    @classmethod
    def get_delay_range(cls) -> tuple:
        """Get delay range tuple."""
        return (cls.REQUEST_DELAY_MIN, cls.REQUEST_DELAY_MAX)

    @classmethod
    def print_config(cls):
        """Print current configuration for debugging."""
        print("\n" + "=" * 50)
        print("CURRENT CONFIGURATION")
        print("=" * 50)
        print(f"SCRAPER_MODE: {cls.SCRAPER_MODE}")
        print(f"VISITOR_ID: {cls.VISITOR_ID}")
        print(f"VISITOR_ID_ALT: {cls.VISITOR_ID_ALT}")
        print(f"BATCH_SIZE: {cls.BATCH_SIZE}")
        print(f"REQUEST_TIMEOUT: {cls.REQUEST_TIMEOUT}s")
        print(f"DELAY: {cls.REQUEST_DELAY_MIN}-{cls.REQUEST_DELAY_MAX}s")
        print(f"CATEGORY_RAW_FOLDER: {cls.CATEGORY_RAW_FOLDER}")
        print(f"CATEGORY_DEDUP_FOLDER: {cls.CATEGORY_DEDUP_FOLDER}")
        print(f"PRODUCT_RAW_FOLDER: {cls.PRODUCT_RAW_FOLDER}")
        print(f"PRODUCT_DEDUP_FOLDER: {cls.PRODUCT_DEDUP_FOLDER}")
        print(f"COUNTRY: auto-detected per URL (supported: {', '.join(cls.COUNTRY_CONFIG.keys())})")
        print("=" * 50 + "\n")
