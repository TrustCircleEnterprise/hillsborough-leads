"""
Cobb County, GA — Motivated Seller Lead Scraper
Calls the LandmarkWeb API directly using requests.
"""

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


def make_session() -> requests.Session:
    """Create a requests session with a valid ASP.NET session cookie."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })

    # Visit home page to get session cookie
    search_url = f"{LANDMARK_BASE}/search/index?theme=.blue&section=searchCriteriaRecordDate&quickSearchSelection="
    for attempt in range(MAX_RETRIES):
        try:
            r = s.get(search_url, timeout=30, verify=False)
            log.info(f"Session init status: {r.status_code}, cookies: {dict(s.cookies)}")
            if "ASP.NET_SessionId" in s.cookies:
                log.info("Got valid session cookie")
                return s
            # Try home page if search page didn't give session
            r2 = s.get(f"{LANDMARK_BASE}/home/index", timeout=30, verify=False)
            log.info(f"Home page status: {r2.status_code}, cookies: {dict(s.cookies)}")
            if s.cookies:
                return s
        except Exception as e:
            log.warning(f"Session init attempt {attempt+1} failed: {e}")
            time.sleep(RETRY_DELAY)

    return s


def get_verification_token(s: requests.Session) -> str:
    """Extract ASP.NET request verification token from the search page."""
    try:
        url = f"{LANDMARK_BASE}/search/index?theme=.blue&section=searchCriteriaRecordDate&quickSearchSelection="
        r = s.get(url, timeout=30, verify=False)
        soup = BeautifulSoup(r.text, "lxml")
        token = soup.find("input", {"name": "__RequestVerificationToken"})
        if token:
            val = token.get("value", "")
            log.info(f"Got verification token: {val[:20]}...")
            return val
        # Also check meta tag
        meta = soup.find("meta", {"name": "__RequestVerificationToken"})
        if meta:
            return meta.get("content", "")
    except Exception as e:
        log.warning(f"Could not get verification token: {e}")
    return ""


def search_by_date(s: requests.Session, date_from: str, date_to: str) -> list[dict]:
    """Call RecordDateSearch then GetSearchResults API."""
    records = []

    # Step 1: Set the date search parameters
    token = get_verification_token(s)

    search_data = {
        "beginDate": date_from,
        "endDate": date_to,
        "exclude": "false",
        "ReturnIndexGroups": "false",
        "recordCount": "500",
        "townName": "",
        "mobileHomesOnly": "false",
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{LANDMARK_BASE}/search/index?theme=.blue&section=searchCriteriaRecordDate&quickSearchSelection=",
        "Origin": "https://superiorcourtclerk.cobbcounty.gov",
    }

    if token:
        search_data["__RequestVerificationToken"] = token

    for attempt in range(MAX_RETRIES):
        try:
            log.info(f"Calling RecordDateSearch (attempt {attempt+1})")
            r = s.post(
                f"{LANDMARK_BASE}/Search/RecordDateSearch",
                data=search_data,
                headers=headers,
                timeout=60,
                verify=False,
            )
            log.info(f"RecordDateSearch status: {r.status_code}, length: {len(r.text)}")
            if r.status_code == 200:
                break
        except Exception as e:
            log.warning(f"RecordDateSearch attempt {attempt+1} failed: {e}")
            time.sleep(RETRY_DELAY)

    # Step 2: Get the actual results
    start = 0
    page_size = 500

    while True:
        results_data = {
            "draw": "1",
            "start": str(start),
            "length": str(page_size),
            "search[value]": "",
            "search[regex]": "false",
        }
        # Add column definitions
        for i in range(8):
            results_data[f"columns[{i}][data]"] = str(i)
            results_data[f"columns[{i}][searchable]"] = "true"
            results_data[f"columns[{i}][orderable]"] = "true" if i > 2 else "false"
            results_data[f"columns[{i}][search][value]"] = ""
            results_data[f"columns[{i}][search][regex]"] = "false"

        try:
            log.info(f"Calling GetSearchResults start={start}")
            r = s.post(
                f"{LANDMARK_BASE}/Search/GetSearchResults",
                data=results_data,
                headers=headers,
                timeout=60,
                verify=False,
            )
            log.info(f"GetSearchResults status: {r.status_code}, length: {len(r.text)}")

            if r.status_code != 200:
                break

            data = r.json()
            rows = data.get("data", [])
            total = data.get("recordsTotal", 0)
            log.info(f"Got {len(rows)} rows, total={total}")

            if not rows:
                break

            for row in rows:
                try:
                    rec = parse_api_row(row)
                    if rec:
                        records.append(rec)
                except Exception as e:
                    log.warning(f"Row parse error: {e}")

            start += page_size
            if start >= total:
                break

        except Exception as e:
            log.warning(f"GetSearchResults error: {e}")
            # Try parsing as HTML if JSON fails
            try:
                html_recs = parse_html_results(r.text)
                records.extend(html_recs)
            except Exception:
                pass
            break

    return records


def parse_api_row(row) -> Optional[dict]:
    """Parse a row from the JSON API response."""
    # Row can be a list or dict
    if isinstance(row, list):
        # Typical columns: status, grantor, grantee, filing_date, doc_type, book_type, book, page
        if len(row) < 5:
            return None
        raw_type = str(row[4]).upper().strip() if len(row) > 4 else ""
        grantor  = str(row[1]).strip() if len(row) > 1 else ""
        grantee  = str(row[2]).strip() if len(row) > 2 else ""
        filed    = str(row[3]).strip() if len(row) > 3 else ""
        doc_num  = str(row[7]).strip() if len(row) > 7 else ""
        link     = ""
    elif isinstance(row, dict):
        raw_type = str(row.get("DocType", row.get("documentType", row.get("4", "")))).upper().strip()
        grantor  = str(row.get("Grantor", row.get("grantor", row.get("1", "")))).strip()
        grantee  = str(row.get("Grantee", row.get("grantee", row.get("2", "")))).strip()
        filed    = str(row.get("FilingDate", row.get("filingDate", row.get("3", "")))).strip()
        doc_num  = str(row.get("DocNum", row.get("docNum", row.get("7", "")))).strip()
        link     = str(row.get("DocLinks", row.get("docLinks", ""))).strip()
    else:
        return None

    # Strip HTML tags from all fields
    for field in [raw_type, grantor, grantee, filed, doc_num]:
        field = re.sub(r"<[^>]+>", "", field).strip()

    raw_type = re.sub(r"<[^>]+>", "", raw_type).strip()
    grantor  = re.sub(r"<[^>]+>", "", grantor).strip()
    grantee  = re.sub(r"<[^>]+>", "", grantee).strip()
    filed    = re.sub(r"<[^>]+>", "", filed).strip()
    doc_num  = re.sub(r"<[^>]+>", "", doc_num).strip()

    # Match doc type
    matched_type = None
    for t in TARGET_TYPES:
        if t == raw_type or t in raw_type:
            matched_type = t
            break

    if not matched_type:
        return None

    label, cat = DOC_TYPE_MAP.get(matched_type, (raw_type, "other"))

    # Extract link from HTML if present
    if link:
        soup = BeautifulSoup(link, "lxml")
        a = soup.find("a", href=True)
        if a:
            href = a["href"]
            if not href.startswith("http"):
                href = f"{LANDMARK_BASE}{href}"
            link = href

    return {
        "doc_num":      doc_num,
        "doc_type":     matched_type,
        "filed":        _norm_date(filed),
        "cat":          cat,
        "cat_label":    label,
        "owner":        grantor,
        "grantee":      grantee,
        "amount":       0.0,
        "legal":        "",
        "clerk_url":    link,
        "prop_address": "", "prop_city": "", "prop_state": "GA", "prop_zip": "",
        "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
        "flags": [], "score": 0,
    }


def parse_html_results(html: str) -> list[dict]:
    """Fallback: parse HTML table if JSON fails."""
    soup = BeautifulSoup(html, "lxml")
    records = []
    table = None
    for t in soup.find_all("table"):
        if len(t.find_all("tr")) >= 2:
            table = t
            break
    if not table:
        return records

    rows = table.find_all("tr")
    headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th","td"])]
    log.info(f"HTML fallback headers: {headers}")

    def ci(*names):
        for n in names:
            for i, h in enumerate(headers):
                if n in h:
                    return i
        return None

    idx_type    = ci("type", "doc type")
    idx_grantor = ci("grantor", "owner", "name")
    idx_grantee = ci("grantee")
    idx_filed   = ci("filing", "date", "recorded")
    idx_docnum  = ci("clerk", "file", "doc", "number")

    for row in rows[1:]:
        cells = row.find_all(["td","th"])
        if not cells:
            continue
        def cell(i):
            if i is None or i >= len(cells):
                return ""
            return cells[i].get_text(strip=True)

        raw_type = cell(idx_type).upper()
        matched_type = None
        for t in TARGET_TYPES:
            if t == raw_type or t in raw_type:
                matched_type = t
                break
        if not matched_type:
            continue

        label, cat = DOC_TYPE_MAP.get(matched_type, (raw_type, "other"))
        link_tag = row.find("a", href=True)
        href = ""
        if link_tag:
            href = link_tag["href"]
            if not href.startswith("http"):
                href = f"{LANDMARK_BASE}{href}"

        records.append({
            "doc_num":      cell(idx_docnum),
            "doc_type":     matched_type,
            "filed":        _norm_date(cell(idx_filed)),
            "cat":          cat,
            "cat_label":    label,
            "owner":        cell(idx_grantor),
            "grantee":      cell(idx_grantee),
            "amount":       0.0,
            "legal":        "",
            "clerk_url":    href,
            "prop_address": "", "prop_city": "", "prop_state": "GA", "prop_zip": "",
            "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
            "flags": [], "score": 0,
        })
    return records


def _norm_date(raw: str) -> str:
    raw = re.sub(r"<[^>]+>", "", raw).strip()
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()


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


def main():
    global _parcel_index

    today     = datetime.now()
    date_to   = today.strftime("%m/%d/%Y")
    date_from = (today - timedelta(days=LOOK_BACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Scraping {date_from} → {date_to}")

    dbf_path = download_parcel_dbf()
    if dbf_path and dbf_path.exists():
        _parcel_index = build_parcel_index(dbf_path)

    session = make_session()
    records = search_by_date(session, date_from, date_to)
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
    main()
