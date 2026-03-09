"""
Helper Functions Module
=======================
Contains utility functions, profiling tools, and logging setup.
"""

import os
import csv
import time
import logging
import functools
import pandas as pd
from typing import List, Callable, Any
from urllib.parse import urlparse
from contextlib import contextmanager

from config import Config


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging() -> logging.Logger:
    """
    Setup dual logging: console + file for profiling.
    Returns the configured logger.
    """
    log_logger = logging.getLogger('noon_scraper')
    log_logger.setLevel(logging.DEBUG)

    # Clear existing handlers
    log_logger.handlers = []

    # Console handler (INFO level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_format)

    # File handler (DEBUG level - includes profiling)
    file_handler = logging.FileHandler(Config.LOG_FILE, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)

    log_logger.addHandler(console_handler)
    log_logger.addHandler(file_handler)

    return log_logger


# Global logger instance
logger = setup_logging()


# =============================================================================
# PROFILING UTILITIES
# =============================================================================

class Profiler:
    """
    Profiler class to track timing of operations.
    Outputs timing data to the log file.
    """

    def __init__(self):
        self.timings = {}
        self.start_time = None

    def start_session(self):
        """Mark the start of a scraping session."""
        self.start_time = time.time()
        self.timings = {}
        logger.debug("=" * 80)
        logger.debug("PROFILING SESSION STARTED")
        logger.debug("=" * 80)

    def end_session(self):
        """End session and output summary."""
        if self.start_time:
            total_time = time.time() - self.start_time
            logger.debug("=" * 80)
            logger.debug("PROFILING SESSION SUMMARY")
            logger.debug("=" * 80)
            logger.debug(f"Total session time: {total_time:.2f}s ({total_time/60:.2f} minutes)")

            if self.timings:
                logger.debug("\nTiming breakdown:")
                sorted_timings = sorted(self.timings.items(), key=lambda x: x[1], reverse=True)
                for operation, duration in sorted_timings:
                    percentage = (duration / total_time) * 100
                    logger.debug(f"  {operation}: {duration:.2f}s ({percentage:.1f}%)")

            logger.debug("=" * 80)

    def record(self, operation: str, duration: float):
        """Record timing for an operation."""
        if operation in self.timings:
            self.timings[operation] += duration
        else:
            self.timings[operation] = duration


# Global profiler instance
profiler = Profiler()


@contextmanager
def profile_step(step_name: str):
    """
    Context manager to profile a step.

    Usage:
        with profile_step("Scraping page 1"):
            # code here
    """
    start = time.time()
    logger.debug(f"[PROFILE START] {step_name}")

    try:
        yield
    finally:
        duration = time.time() - start
        profiler.record(step_name, duration)
        logger.debug(f"[PROFILE END] {step_name} - {duration:.3f}s")


def profile_function(func: Callable) -> Callable:
    """
    Decorator to profile a function.

    Usage:
        @profile_function
        def my_function():
            pass
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start = time.time()
        func_name = func.__name__
        logger.debug(f"[PROFILE START] {func_name}")

        try:
            result = func(*args, **kwargs)
            return result
        finally:
            duration = time.time() - start
            profiler.record(func_name, duration)
            logger.debug(f"[PROFILE END] {func_name} - {duration:.3f}s")

    return wrapper


# =============================================================================
# FILE & PATH UTILITIES
# =============================================================================

KNOWN_COUNTRY_PREFIXES = {'uae-en', 'uae-ar', 'saudi-en', 'saudi-ar', 'egypt-en', 'egypt-ar', 'jordan-en', 'jordan-ar'}


def extract_country_prefix_from_url(url: str) -> str:
    """
    Extract the country prefix from a noon URL.

    Example: https://www.noon.com/saudi-en/category/ → 'saudi-en'
    Falls back to 'uae-en' if no known prefix found.
    """
    parsed = urlparse(url)
    parts = parsed.path.strip('/').split('/')
    if parts and parts[0] in KNOWN_COUNTRY_PREFIXES:
        return parts[0]
    return 'uae-en'


def extract_filename_from_url(url: str) -> str:
    """
    Extract filename from category URL with timestamp.

    Example URL: https://www.noon.com/saudi-en/fashion/men-31225/clothing-16204/t-shirts-and-polos/
    Result: noon_fashion_men_31225_clothing_16204_t_shirts_and_polos_(2026-01-25_14-30-45).csv
    """
    from datetime import datetime

    parsed = urlparse(url)
    path = parsed.path.strip('/')

    # Remove country prefix (uae-en, saudi-en, egypt-en, etc.)
    parts = path.split('/')
    if parts and parts[0] in KNOWN_COUNTRY_PREFIXES:
        parts = parts[1:]

    # Remove empty parts
    parts = [p for p in parts if p]

    # Clean up parts - replace dashes with underscores
    cleaned_parts = [part.replace('-', '_') for part in parts]

    # Generate timestamp with date and time (hours, minutes, seconds)
    timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')

    filename = 'noon_' + '_'.join(cleaned_parts) + f'_({timestamp}).csv'
    return filename


def extract_category_path_from_url(url: str) -> str:
    """
    Extract the category path for API calls from a full URL.

    Example URL: https://www.noon.com/saudi-en/fashion/women-31229/shoes-16238/womens-slides/
    Result: fashion/women-31229/shoes-16238/womens-slides/
    """
    parsed = urlparse(url)
    path = parsed.path.strip('/')

    parts = path.split('/')
    if parts and parts[0] in KNOWN_COUNTRY_PREFIXES:
        parts = parts[1:]

    return '/'.join(parts) + '/'


def ensure_directories():
    """Create output directories if they don't exist."""
    with profile_step("Creating output directories"):
        os.makedirs(Config.CATEGORY_RAW_FOLDER, exist_ok=True)
        os.makedirs(Config.CATEGORY_DEDUP_FOLDER, exist_ok=True)
        os.makedirs(Config.PRODUCT_RAW_FOLDER, exist_ok=True)
        os.makedirs(Config.PRODUCT_DEDUP_FOLDER, exist_ok=True)
        logger.info(f"Category folders: {Config.CATEGORY_RAW_FOLDER}/, {Config.CATEGORY_DEDUP_FOLDER}/")
        logger.info(f"Product folders: {Config.PRODUCT_RAW_FOLDER}/, {Config.PRODUCT_DEDUP_FOLDER}/")


def read_categories_from_csv(csv_path: str) -> List[str]:
    """Read category URLs from CSV file."""
    categories = []

    with profile_step("Reading categories CSV"):
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return categories

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get('categories_to_scrape', '').strip()
                if url:
                    categories.append(url)

        logger.info(f"Loaded {len(categories)} categories from {csv_path}")

    return categories


# =============================================================================
# DATA UTILITIES
# =============================================================================

def calculate_data_schema(df: pd.DataFrame) -> str:
    """
    Calculate the data schema showing non-null counts for each column.
    Format: column1:count|column2:count|...
    """
    schema_parts = []
    for col in df.columns:
        non_null_count = df[col].notna().sum()
        schema_parts.append(f"{col}:{non_null_count}")
    return '|'.join(schema_parts)


def get_priority_columns() -> List[str]:
    """Get list of priority columns for CSV ordering."""
    return [
        'name', 'brand', 'sku', 'product_url',
        'price', 'sale_price', 'discount_percentage',
        'image_1', 'image_2', 'image_3', 'image_4', 'image_5',
        'rating_value', 'rating_count', 'availability'
    ]


def reorder_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder DataFrame columns with priority columns first."""
    priority_cols = get_priority_columns()
    all_cols = df.columns.tolist()

    ordered_cols = [c for c in priority_cols if c in all_cols]
    ordered_cols.extend([c for c in all_cols if c not in priority_cols])

    return df[ordered_cols]


def image_key_to_url(image_key: str) -> str:
    """Convert image key to full URL."""
    if not image_key:
        return ''
    return f"https://f.nooncdn.com/p/{image_key}.jpg"
