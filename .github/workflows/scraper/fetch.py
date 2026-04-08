"""
Cobb County, GA — Motivated Seller Lead Scraper
Fetches clerk filings for the last 7 days, enriches with parcel data,
scores leads, and writes JSON output for the dashboard.
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import traceback

import requests
from bs4 import BeautifulSoup

# ── Playwright (async) ──────────────────────────────────────────────────────
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
except ImportError:
    print("playwright not installed – run: pip install playwright && python -m playwright install chromium")
    sys.exit(1)

# ── DBF reader ──────────────────────────────────────────────────────────────
try:
    from dbfread import DBF
except ImportError:
    DBF = None
    print("dbfread not installed – parcel lookups will be skipped")

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("cobb_scraper")

# ── Constants ───────────────────────────────────────────────────────────────
CLERK_BASE = "https://superiorcourtclerk.cobbcounty.gov/records-search"
PARCEL_BASE = "https://gis.cobbcountyga.gov"
LOOK_BACK_DAYS = 7
MAX_RETRIES = 3
RETRY_DELAY = 3  # seconds

REPO_ROOT = Path(__file__).parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
DATA_DIR = REPO_ROOT / "data"
for d in (DASHBOARD_DIR, DATA_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ── Document type catalo
