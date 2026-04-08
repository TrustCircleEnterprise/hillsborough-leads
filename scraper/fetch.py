"""
Cobb County, GA — Motivated Seller Lead Scraper
Playwright stealth + direct API approach.
"""

import asyncio
import csv
import io
import json
import logging
import re
import sys
import time
import zipfile
import urllib3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("playwright not installed")
    sys.exit(1)

try:
    from dbfread import DBF
except ImportError:
    DBF = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("cobb_scraper")

LANDMARK_BASE  = "https://superiorcourtclerk.cobbcounty.gov/LandmarkWeb"
LOOK_BACK_DAYS = 7
MAX_RETRIES    = 3
RETRY_DELAY    = 3

REPO_ROOT     = Path(__file__).parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
DATA_DIR      = REPO_ROOT / "data"
for d in (DASHBOARD_DIR, DATA_DIR):
    d.mkdir(parents=True, exist_ok=True)

TARGET_TYPES = {
    "LP", "NOFC", "TAXDEED", "JUD", "CCJ", "DRJUD",
    "LNCORPTX", "LNIRS", "LNFED", "LN", "LNMECH",
    "LNHOA", "MEDLN", "PRO", "NOC", "RELLP",
}

DOC_TYPE_MAP = {
    "LP":       ("Lis Pendens",            "lis_pendens"),
    "NOFC":     ("Notice of Foreclosure",  "foreclosure"),
    "TAXDEED":  ("Tax Deed",               "tax_deed"),
    "JUD":      ("Judgment",               "judgment"),
    "CCJ":      ("Certified Judgment",     "judgment"),
    "DRJUD":    ("Domestic Judgment",      "judgment"),
    "LNCORPTX": ("Corp Tax Lien",          "tax_lien"),
    "LNIRS":    ("IRS Lien",               "tax_lien"),
    "LNFED":    ("Federal Lien",           "tax_lien"),
    "LN":       ("Lien",                   "lien"),
    "LNMECH":   ("Mechanic's Lien",        "lien"),
    "LNHOA":    ("HOA Lien",               "lien"),
    "MEDLN":    ("Medicaid Lien",          "lien"),
    "PRO":      ("Probate",                "probate"),
    "NOC":      ("Notice of Commencement", "noc"),
    "RELLP":    ("Release Lis Pendens",    "release"),
}

_parcel_index: dict[str, dict] = {}


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().upper()

def _col(row: dict, *names: str) -> str:
    for n in names:
        v = row.get(n, "")
        if v:
            return str(v).strip()
    return ""

def _name_variants(owner_raw: str) -> list[str]:
    parts = re.split(r",\s*", owner_raw.strip(), maxsplit=1)
    if len(parts) == 2:
        last, first = parts
        return list({_norm(owner_raw), _norm(f"{first} {last}"), _norm(f"{last} {first}")})
    return [_norm(owner_raw)]

def lookup_parcel(owner: str) -> Optional[dict]:
    return _parcel_index.get(_norm(owner))

def build_parcel_index(dbf_path: Path) -> dict:
    if DBF is None:
        return {}
    idx: dict[str, dict] = {}
    try:
        table = DBF(str(dbf_path), encoding="latin-1", ignore_missing_memofile=True)
        for row in table:
            try:
                owner_raw = _col(row, "OWNER", "OWN1", "OWNERNAME")
                if not owner_raw:
                    continue
                rec = {
                    "prop_address": _col(row, "SITE_ADDR", "SITEADDR"),
                    "prop_city":    _col(row, "SITE_CITY", "SITECITY"),
                    "prop_state":   "GA",
                    "prop_zip":     _col(row, "SITE_ZIP", "SITEZIP"),
                    "mail_address": _col(row, "ADDR_1", "MAILADR1"),
                    "mail_city":    _col(row, "CITY", "MAILCITY"),
                    "mail_state":   _col(row, "STATE", "MAILSTATE"),
                    "mail_zip":     _col(row, "ZIP", "MAILZIP"),
                }
                for v in _name_variants(owner_raw):
                    if v:
                        idx[v] = rec
            except Exception:
                pass
    except Exception as e:
        log.warning(f"DBF read error: {e}")
    log.info(f"Parcel index: {len(idx):,} variants")
    return idx

def download_parcel_dbf() -> Optional[Path]:
    cache_dir = REPO_ROOT / ".cache"
    cache_dir.mkdir(exist_ok=True)
    dbf_path = cache_dir / "parcels.dbf"
    if dbf_path.exists() and (time.time() - dbf_path.stat().st_mtime) < 86400:
        return dbf_path
    urls = [
        "https://gis.cobbcountyga.gov/download/parcels.zip",
        "https://gis.cobbcountyga.gov/download/Cobb_Parcels.zip",
        "https://www.cobbcountyga.gov/images/gis/data/parcels.zip",
    ]
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    for url in urls:
        try:
            r = session.get(url, timeout=60, verify=False, stream=True)
            if r.status_code == 200:
                zdata = b"".join(r.iter_content(65536))
                with zipfile.ZipFile(io.BytesIO(zdata)) as zf:
                    dbf_files = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                    if dbf_files:
                        dbf_path.write_bytes(zf.read(dbf_files[0]))
                        log.info(f"Parcel DBF from {url}")
                        return dbf_path
        except Exception as e:
            log.warning(f"Parcel download failed {url}: {e}")
    log.warning("Parcel data unavailable")
    return None


def clean(s) -> str:
    if s is None:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(s))
    text = re.sub(r"\\u[0-9a-fA-F]{4}", lambda m: chr(int(m.group()[2:], 16)), text)
    return re.sub(r"\s+", " ", text).strip()


def _norm_date(raw: str) -> str:
    raw = re.sub(r"<[^>]+>", "", raw).strip()
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()


async def scrape_with_playwright(date_from: str, date_to: str) -> list[dict]:
    """Use Playwright to load the page, then intercept the API responses."""
    all_rows = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-web-security",
                "--allow-running-insecure-content",
            ]
        )

        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            ignore_https_errors=True,
            java_script_enabled=True,
            locale="en-US",
            timezone_id="America/New_York",
        )

        # Stealth: remove webdriver property
        await ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            window.chrome = {runtime: {}};
            Object.defineProperty(navigator, 'permissions', {
                get: () => ({query: () => Promise.resolve({state: 'granted'})})
            });
        """)

        page = await ctx.new_page()

        # Intercept GetSearchResults API calls
        api_responses = []

        async def handle_response(response):
            if "GetSearchResults" in response.url and response.status == 200:
                try:
                    body = await response.body()
                    if body:
                        data = json.loads(body)
                        rows = data.get("data", [])
                        if rows:
                            log.info(f"Intercepted {len(rows)} rows from GetSearchResults")
                            api_responses.extend(rows)
                except Exception as e:
                    log.warning(f"Response intercept error: {e}")

        page.on("response", handle_response)

        search_url = f"{LANDMARK_BASE}/search/index?theme=.blue&section=searchCriteriaRecordDate&quickSearchSelection="

        for attempt in range(MAX_RETRIES):
            try:
                log.info(f"Loading page (attempt {attempt+1})")
                await page.goto(search_url, timeout=60000, wait_until="networkidle")
                await asyncio.sleep(3)

                log.info(f"Page URL: {page.url}")
                log.info(f"Page title: {await page.title()}")

                # Check if we got the real page
                title = await page.title()
                if "expired" in title.lower() or "error" in title.lower() or "redirect" in title.lower():
                    log.warning(f"Got wrong page: {title} — retrying")
                    await asyncio.sleep(5)
                    continue

                # Look for date inputs using JavaScript
                inputs = await page.evaluate("""() => {
                    return Array.from(document.querySelectorAll('input')).map(i => ({
                        id: i.id, name: i.name, type: i.type,
                        placeholder: i.placeholder, value: i.value
                    }));
                }""")
                log.info(f"Found {len(inputs)} inputs on page")
                for inp in inputs[:10]:
                    log.info(f"  Input: {inp}")

                # Fill dates
                filled = False
                for begin_sel, end_sel in [
                    ("#beginDate", "#endDate"),
                    ("input[name='beginDate']", "input[name='endDate']"),
                    ("input[id*='begin']", "input[id*='end']"),
                    ("input[id*='Begin']", "input[id*='End']"),
                ]:
                    try:
                        b = page.locator(begin_sel).first
                        e = page.locator(end_sel).first
                        bc = await b.count()
                        ec = await e.count()
                        if bc > 0 and ec > 0:
                            await b.triple_click()
                            await b.fill(date_from)
                            await e.triple_click()
                            await e.fill(date_to)
                            log.info(f"Filled dates via {begin_sel} / {end_sel}")
                            filled = True
                            break
                    except Exception:
                        pass

                if not filled:
                    log.warning("Could not fill date fields")
                    # Try clicking submit anyway to trigger interceptor
                
                # Click submit
                for sel in ["input[value='Submit']", "button:has-text('Submit')", "#btnSearch"]:
                    try:
                        btn = page.locator(sel).first
                        if await btn.count() > 0:
                            await btn.click()
                            log.info(f"Clicked submit: {sel}")
                            break
                    except Exception:
                        pass

                # Wait for API response to be intercepted
                await asyncio.sleep(8)

                if api_responses:
                    log.info(f"Got {len(api_responses)} rows via interception")
                    break
                else:
                    log.warning("No API responses intercepted")
                    # Log page content for debug
                    content = await page.content()
                    log.info(f"Page content preview: {content[:500]}")

            except Exception as e:
                log.warning(f"Playwright attempt {attempt+1} error: {e}")
                await asyncio.sleep(RETRY_DELAY)

        await browser.close()

    # Parse intercepted rows
    records = []
    for row in api_responses:
        try:
            rec = parse_row(row)
            if rec:
                records.append(rec)
        except Exception as e:
            log.warning(f"Row parse error: {e}")

    return records


def parse_row(row: dict) -> Optional[dict]:
    grantor  = clean(row.get("5", ""))
    grantee  = clean(row.get("6", ""))
    filed    = clean(row.get("7", ""))
    raw_type = clean(row.get("8", "")).upper().strip()
    doc_num  = clean(row.get("12", ""))
    legal    = clean(row.get("15", ""))
    row_id   = str(row.get("DT_RowId", ""))

    clerk_url = ""
    if row_id:
        doc_id = row_id.replace("doc_", "").split("_")[0]
        clerk_url = f"{LANDMARK_BASE}/document/index?theme=.blue&id={doc_id}"

    matched_type = None
    for t in TARGET_TYPES:
        if t == raw_type or raw_type.startswith(t):
            matched_type = t
            break
    if not matched_type:
        return None

    label, cat = DOC_TYPE_MAP.get(matched_type, (raw_type, "other"))

    return {
        "doc_num":      doc_num.replace("nobreak_", ""),
        "doc_type":     matched_type,
        "filed":        _norm_date(filed.replace("nobreak_", "")),
        "cat":          cat,
        "cat_label":    label,
        "owner":        grantor,
        "grantee":      grantee,
        "amount":       0.0,
        "legal":        legal,
        "clerk_url":    clerk_url,
        "prop_address": "", "prop_city": "", "prop_state": "GA", "prop_zip": "",
        "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
        "flags": [], "score": 0,
    }


WEEK_AGO = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")


def compute_flags_and_score(rec: dict, all_records: list[dict]) -> tuple[list[str], int]:
    flags: list[str] = []
    score = 30
    dt    = rec.get("doc_type", "")
    cat   = rec.get("cat", "")
    owner = rec.get("owner", "")
    filed = rec.get("filed", "")

    if dt in ("LP", "RELLP"):   flags.append("Lis pendens")
    if dt == "NOFC":             flags.append("Pre-foreclosure")
    if cat == "judgment":        flags.append("Judgment lien")
    if cat == "tax_lien":        flags.append("Tax lien")
    if dt == "LNMECH":           flags.append("Mechanic lien")
    if cat == "probate":         flags.append("Probate / estate")
    if owner and re.search(r"\b(LLC|INC|CORP|LTD|TRUST|ESTATE)\b", owner.upper()):
        flags.append("LLC / corp owner")
    if filed >= WEEK_AGO:        flags.append("New this week")

    score += len(flags) * 10
    owner_key  = _norm(owner)
    owner_docs = {r["doc_type"] for r in all_records if _norm(r.get("owner","")) == owner_key}
    if "LP" in owner_docs and "NOFC" in owner_docs:
        score += 20
    amt = rec.get("amount", 0) or 0
    if amt > 100_000:   score += 15
    elif amt > 50_000:  score += 10
    if filed >= WEEK_AGO: score += 5
    if rec.get("prop_address") or rec.get("mail_address"): score += 5
    return flags, min(score, 100)


def enrich_record(rec: dict) -> dict:
    parcel = lookup_parcel(rec.get("owner", ""))
    if parcel:
        for k, v in parcel.items():
            if v:
                rec[k] = v
    return rec


def _split_name(full: str) -> tuple[str, str]:
    full = full.strip()
    if "," in full:
        p = full.split(",", 1)
        return p[1].strip().title(), p[0].strip().title()
    p = full.split()
    return (p[0].title(), " ".join(p[1:]).title()) if len(p) >= 2 else (full.title(), "")


def write_outputs(records: list[dict], date_from: str, date_to: str):
    with_address = sum(1 for r in records if r.get("prop_address") or r.get("mail_address"))
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Cobb County Superior Court Clerk",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(records),
        "with_address": with_address,
        "records":      records,
    }
    for path in [DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"]:
        path.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"Wrote {len(records)} records → {path}")

    ghl_path = DATA_DIR / "ghl_export.csv"
    fieldnames = [
        "First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
        "Seller Score","Motivated Seller Flags","Source","Public Records URL",
    ]
    with ghl_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            first, last = _split_name(r.get("owner",""))
            writer.writerow({
                "First Name": first, "Last Name": last,
                "Mailing Address": r.get("mail_address",""),
                "Mailing City":    r.get("mail_city",""),
                "Mailing State":   r.get("mail_state",""),
                "Mailing Zip":     r.get("mail_zip",""),
                "Property Address":r.get("prop_address",""),
                "Property City":   r.get("prop_city",""),
                "Property State":  r.get("prop_state","GA"),
                "Property Zip":    r.get("prop_zip",""),
                "Lead Type":       r.get("cat_label",""),
                "Document Type":   r.get("doc_type",""),
                "Date Filed":      r.get("filed",""),
                "Document Number": r.get("doc_num",""),
                "Amount/Debt Owed":r.get("amount",""),
                "Seller Score":    r.get("score",0),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":          "Cobb County Superior Court Clerk",
                "Public Records URL": r.get("clerk_url",""),
            })
    log.info(f"GHL CSV → {ghl_path}")


async def main():
    global _parcel_index

    today     = datetime.now()
    date_to   = today.strftime("%m/%d/%Y")
    date_from = (today - timedelta(days=LOOK_BACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Scraping {date_from} → {date_to}")

    dbf_path = download_parcel_dbf()
    if dbf_path and dbf_path.exists():
        _parcel_index = build_parcel_index(dbf_path)

    records = await scrape_with_playwright(date_from, date_to)
    log.info(f"Total matching records: {len(records)}")

    for rec in records:
        try:
            enrich_record(rec)
            flags, score = compute_flags_and_score(rec, records)
            rec["flags"] = flags
            rec["score"] = score
        except Exception as e:
            log.warning(f"Scoring error: {e}")
            rec["flags"] = []
            rec["score"] = 30

    records.sort(key=lambda r: r.get("score", 0), reverse=True)
    write_outputs(records, date_from, date_to)
    log.info("✅ Done")


if __name__ == "__main__":
    asyncio.run(main())
