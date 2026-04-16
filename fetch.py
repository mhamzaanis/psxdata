"""
Pakistan Stock Exchange (PSX) - PDF Closing Rate Parser & Supabase Uploader
============================================================================
Downloads the official PSX "Closing Rate Summary" PDF for each trading day,
parses every equity/fund/bond row, and upserts the data into Supabase.

PDF URL pattern:
  https://dps.psx.com.pk/download/closing_rates/{DDMMMYYYY}.pdf
  e.g.  https://dps.psx.com.pk/download/closing_rates/16APR2026.pdf

GitHub Actions usage:
  - Set SUPABASE_URL and SUPABASE_SERVICE_KEY as repository secrets.
    - The workflow runs frequently on weekdays; it can also be run manually.

Run locally:
  pip install -r requirements.txt
    SUPABASE_URL=... SUPABASE_SERVICE_KEY=... python fetch.py
"""

from __future__ import annotations

import io
import logging
import os
import re
import sys
import time
from datetime import date, timedelta

import pdfplumber
import requests
from postgrest.exceptions import APIError
from supabase import create_client, Client

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

SUPABASE_URL: str         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY: str = os.environ["SUPABASE_SERVICE_KEY"]

TABLE_NAME    = "psx_daily_prices"
START_DATE    = date.fromisoformat(os.getenv("START_DATE", "2015-01-01"))

MAX_RETRIES      = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY      = float(os.getenv("RETRY_DELAY", "10"))
REQUEST_DELAY    = float(os.getenv("REQUEST_DELAY", "1.5"))
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "200"))
MAX_DAYS_PER_RUN = int(os.getenv("MAX_DAYS_PER_RUN", "0"))  # 0 = no cap

PDF_URL_TEMPLATE = (
    "https://dps.psx.com.pk/download/closing_rates/{day_str}.pdf"
)

# Sections to skip (futures, bonds, defaulters)
SKIP_SECTION_KEYWORDS = {
    "FUTURE CONTRACTS",
    "STOCK INDEX FUTURE",
    "BONDS",
    "DEFAULTER",
}

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


class FatalConfigError(RuntimeError):
    """Non-retryable setup/configuration error."""


def _is_missing_table_error(exc: Exception) -> bool:
    msg = str(exc)
    return "PGRST205" in msg and TABLE_NAME in msg


# ──────────────────────────────────────────────────────────────────────────────
# Supabase helpers
# ──────────────────────────────────────────────────────────────────────────────

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def get_last_stored_date(sb: Client) -> date | None:
    """Return the most recent trade_date stored, or None."""
    try:
        res = (
            sb.table(TABLE_NAME)
            .select("trade_date")
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
        )
    except APIError as exc:
        if _is_missing_table_error(exc):
            raise FatalConfigError(
                f"Supabase table '{TABLE_NAME}' was not found. "
                "Run supabaseschema.sql in your Supabase SQL Editor, then retry."
            ) from exc
        raise

    if res.data:
        return date.fromisoformat(res.data[0]["trade_date"])
    return None


def upsert_rows(sb: Client, rows: list[dict]) -> None:
    if not rows:
        return
    try:
        sb.table(TABLE_NAME).upsert(rows, on_conflict="ticker,trade_date").execute()
    except APIError as exc:
        if _is_missing_table_error(exc):
            raise FatalConfigError(
                f"Supabase table '{TABLE_NAME}' was not found. "
                "Run supabaseschema.sql in your Supabase SQL Editor, then retry."
            ) from exc
        raise

    log.info("    Upserted %d rows", len(rows))


# ──────────────────────────────────────────────────────────────────────────────
# PDF download
# ──────────────────────────────────────────────────────────────────────────────

def day_str(d: date) -> str:
    """16APR2026"""
    return d.strftime("%d%b%Y").upper()


def download_pdf(d: date) -> bytes | None:
    """
    Download the closing-rate PDF for date *d*.
    Returns PDF bytes on success, None for non-trading days.
    """
    url = PDF_URL_TEMPLATE.format(day_str=day_str(d))
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 404:
                log.debug("  %s -> 404 (holiday/weekend)", d)
                return None
            if resp.status_code == 200:
                # PSX may return an HTML error page with 200 status
                if resp.content[:4] == b"%PDF" or "pdf" in resp.headers.get("Content-Type", ""):
                    return resp.content
                log.debug("  %s -> 200 but non-PDF response, skipping", d)
                return None
            log.warning("  %s -> HTTP %d (attempt %d/%d)", d, resp.status_code, attempt, MAX_RETRIES)
        except requests.RequestException as exc:
            log.warning("  %s -> network error (attempt %d/%d): %s", d, attempt, MAX_RETRIES, exc)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    log.warning("  %s -> all %d attempts failed, skipping.", d, MAX_RETRIES)
    return None


# ──────────────────────────────────────────────────────────────────────────────
# PDF parsing
# ──────────────────────────────────────────────────────────────────────────────

# Matches a data row, e.g.:
#   LUCK Lucky Cement 2920671 435.77 439.99 444 430.15 435.5 -0.27
#   KEL  K-Electric Ltd. 53102169 7.77 7.86 7.97 7.78 7.81 0.04
_ROW_RE = re.compile(
    r"^([A-Z][A-Z0-9]*)\s+"   # ticker
    r"(.+?)\s+"                # company name (lazy, may contain spaces)
    r"(\d[\d,]*)\s+"           # turnover
    r"([\d.]+|-)\s+"           # prev rate
    r"([\d.]+|-)\s+"           # open rate
    r"([\d.]+|-)\s+"           # highest
    r"([\d.]+|-)\s+"           # lowest
    r"([\d.]+|-)\s+"           # last rate
    r"(-?[\d.]+)$"             # diff (can be negative)
)

# Section header: ***COMMERCIAL BANKS***
_SECTION_RE = re.compile(r"\*{3}\s*(.+?)\s*\*{3}")


def _float(s: str) -> float | None:
    if not s or s == "-":
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def _int(s: str) -> int | None:
    if not s or s == "-":
        return None
    try:
        return int(s.replace(",", ""))
    except ValueError:
        return None


def parse_pdf(pdf_bytes: bytes, trade_date: date) -> list[dict]:
    """Parse all pages; return list of row dicts ready for Supabase."""
    rows: list[dict] = []
    current_section = "UNKNOWN"
    skip_section    = False

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue

                # Detect section header
                m_sec = _SECTION_RE.search(line)
                if m_sec:
                    current_section = m_sec.group(1).strip().upper()
                    skip_section = any(kw in current_section for kw in SKIP_SECTION_KEYWORDS)
                    continue

                if skip_section:
                    continue

                # Try to parse a data row
                m = _ROW_RE.match(line)
                if not m:
                    continue

                (ticker, name, turnover,
                 prev_rate, open_rate, high, low, last, diff) = m.groups()

                rows.append({
                    "ticker":       ticker,
                    "company_name": name.strip(),
                    "sector":       current_section,
                    "trade_date":   trade_date.isoformat(),
                    "turnover":     _int(turnover),
                    "prev_rate":    _float(prev_rate),
                    "open_rate":    _float(open_rate),
                    "high":         _float(high),
                    "low":          _float(low),
                    "close":        _float(last),
                    "change":       _float(diff),
                })

    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    today = date.today()
    sb    = get_supabase()

    last = get_last_stored_date(sb)
    if last:
        fetch_from = last + timedelta(days=1)
        log.info("Resuming from %s  (last stored date: %s)", fetch_from, last)
    else:
        fetch_from = START_DATE
        log.info("No existing data – fetching from %s", fetch_from)

    if fetch_from > today:
        log.info("Already up to date.")
        return

    end_date = today
    if MAX_DAYS_PER_RUN > 0:
        capped_end = fetch_from + timedelta(days=MAX_DAYS_PER_RUN - 1)
        end_date = min(today, capped_end)

    log.info("Processing range %s -> %s", fetch_from, end_date)

    trading_days = skipped_days = 0
    current = fetch_from

    while current <= end_date:
        log.info("── %s", current)

        pdf_bytes = download_pdf(current)
        if pdf_bytes is None:
            # Weekend, holiday or future date
            skipped_days += 1
            current += timedelta(days=1)
            continue

        rows = parse_pdf(pdf_bytes, current)
        log.info("  Parsed %d rows", len(rows))

        for i in range(0, len(rows), BATCH_SIZE):
            upsert_rows(sb, rows[i : i + BATCH_SIZE])

        trading_days += 1
        current += timedelta(days=1)
        time.sleep(REQUEST_DELAY)

    log.info(
        "Done. %d trading days stored, %d non-trading days skipped.",
        trading_days, skipped_days,
    )

    if end_date < today:
        log.info(
            "Stopped at %s due to MAX_DAYS_PER_RUN=%d. Remaining dates will be picked up by the next run.",
            end_date,
            MAX_DAYS_PER_RUN,
        )


if __name__ == "__main__":
    try:
        main()
    except FatalConfigError as exc:
        log.error("%s", exc)
        sys.exit(2)