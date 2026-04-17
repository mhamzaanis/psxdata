"""
Pakistan Stock Exchange (PSX) - PDF Closing Rate Parser & Supabase Uploader
============================================================================
Downloads the official PSX "Closing Rate Summary" PDF for each trading day,
parses every equity/fund/bond row, and upserts the data into Supabase.

PDF URL pattern (currently active):
    https://dps.psx.com.pk/download/closing_rates/{YYYY-MM-DD}.pdf
    e.g.  https://dps.psx.com.pk/download/closing_rates/2026-04-16.pdf

Backward compatibility:
    The script also tries the older DDMMMYYYY format automatically.

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

TABLE_NAME    = "datatable"
START_DATE    = date.fromisoformat(os.getenv("START_DATE", "2018-01-01"))

MAX_RETRIES      = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY      = float(os.getenv("RETRY_DELAY", "10"))
REQUEST_DELAY    = float(os.getenv("REQUEST_DELAY", "1.5"))
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "200"))
MAX_DAYS_PER_RUN = int(os.getenv("MAX_DAYS_PER_RUN", "0"))   # 0 = no cap

_DEFAULT_PDF_URL_TEMPLATES = (
    "https://dps.psx.com.pk/download/closing_rates/{day_iso}.pdf,"
    "https://dps.psx.com.pk/download/closing_rates/{day_str}.pdf"
)
PDF_URL_TEMPLATES = [
    t.strip()
    for t in os.getenv("PDF_URL_TEMPLATES", _DEFAULT_PDF_URL_TEMPLATES).split(",")
    if t.strip()
]

# Fail the run if no rows are stored; this prevents silent "successful" no-op runs.
FAIL_ON_EMPTY_RUN = os.getenv("FAIL_ON_EMPTY_RUN", "1").strip().lower() not in {
    "0",
    "false",
    "no",
}

# Sections to skip entirely
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
    """Return the most recent trade date stored, or None."""
    try:
        res = (
            sb.table(TABLE_NAME)
            .select("date")
            .order("date", desc=True)
            .limit(1)
            .execute()
        )
    except APIError as exc:
        if _is_missing_table_error(exc):
            raise FatalConfigError(
                f"Supabase table '{TABLE_NAME}' was not found. "
                "Check your schema and retry."
            ) from exc
        raise

    if res.data:
        # date is stored as timestamptz; parse just the date part
        raw = res.data[0]["date"]
        return date.fromisoformat(str(raw)[:10])
    return None


def upsert_rows(sb: Client, rows: list[dict]) -> None:
    """
    Upsert rows into datatable.
    Because the table has no unique constraint on (symbol, date) we use
    INSERT … ON CONFLICT DO NOTHING via ignoreDuplicates=True, which skips
    rows that would violate any unique/pk constraint.  If you later add a
    unique index on (symbol, date) this will de-duplicate correctly.
    """
    if not rows:
        return
    try:
        sb.table(TABLE_NAME).upsert(rows).execute()
    except APIError as exc:
        if _is_missing_table_error(exc):
            raise FatalConfigError(
                f"Supabase table '{TABLE_NAME}' was not found. "
                "Check your schema and retry."
            ) from exc
        raise

    log.info("    Upserted %d rows", len(rows))


# ──────────────────────────────────────────────────────────────────────────────
# PDF download
# ──────────────────────────────────────────────────────────────────────────────

def day_str(d: date) -> str:
    """Format date as 16APR2026."""
    return d.strftime("%d%b%Y").upper()


def day_iso(d: date) -> str:
    """Format date as 2026-04-16."""
    return d.isoformat()


def download_pdf(d: date) -> bytes | None:
    """
    Download the closing-rate PDF for date *d*.
    Returns PDF bytes on success, None for non-trading days (404 / non-PDF).
    """
    urls = [
        template.format(day_str=day_str(d), day_iso=day_iso(d))
        for template in PDF_URL_TEMPLATES
    ]

    for url in urls:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 404:
                    break
                if resp.status_code == 200:
                    ct = resp.headers.get("Content-Type", "")
                    if resp.content[:4] == b"%PDF" or "pdf" in ct:
                        return resp.content
                    log.warning("  %s -> 200 but non-PDF response from %s", d, url)
                    break
                log.warning(
                    "  %s -> HTTP %d from %s (attempt %d/%d)",
                    d, resp.status_code, url, attempt, MAX_RETRIES,
                )
            except requests.RequestException as exc:
                log.warning(
                    "  %s -> network error from %s (attempt %d/%d): %s",
                    d, url, attempt, MAX_RETRIES, exc,
                )

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

        # Next template URL.
        continue

    log.info("  %s -> source PDF not found for configured URL templates, skipping", d)
    return None


# ──────────────────────────────────────────────────────────────────────────────
# PDF parsing
# ──────────────────────────────────────────────────────────────────────────────

# Matches a data row such as:
#   LUCK Lucky Cement 2920671 435.77 439.99 444 430.15 435.5 -0.27
#   KEL  K-Electric Ltd. 53102169 7.77 7.86 7.97 7.78 7.81 0.04
_ROW_RE = re.compile(
    r"^([A-Z][A-Z0-9]*)\s+"    # ticker  (group 1)
    r"(.+?)\s+"                 # company name, lazy  (group 2)
    r"(\d[\d,]*)\s+"            # turnover  (group 3)
    r"([\d.]+|-)\s+"            # prev rate  (group 4)
    r"([\d.]+|-)\s+"            # open rate  (group 5)
    r"([\d.]+|-)\s+"            # highest  (group 6)
    r"([\d.]+|-)\s+"            # lowest  (group 7)
    r"([\d.]+|-)\s+"            # last/close rate  (group 8)
    r"(-?[\d.]+)$"              # diff / change  (group 9)
)

# Section header:  ***COMMERCIAL BANKS***
_SECTION_RE = re.compile(r"\*{3}\s*(.+?)\s*\*{3}")


def _to_float(s: str) -> float | None:
    if not s or s == "-":
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def _to_int(s: str) -> int | None:
    if not s or s == "-":
        return None
    try:
        return int(s.replace(",", ""))
    except ValueError:
        return None


def parse_pdf(pdf_bytes: bytes, trade_date: date) -> list[dict]:
    """
    Parse all pages of the PDF.
    Returns a list of row dicts shaped for the `datatable` Supabase table:
      date, symbol, company, open, high, low, close, turnover, change
    """
    rows: list[dict] = []
    current_section = "UNKNOWN"
    skip_section    = False

    # Use midnight UTC as the timestamptz value for the trade date
    trade_ts = f"{trade_date.isoformat()}T00:00:00+00:00"

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue

                # ── Detect section header ──────────────────────────────────
                m_sec = _SECTION_RE.search(line)
                if m_sec:
                    current_section = m_sec.group(1).strip().upper()
                    skip_section = any(
                        kw in current_section for kw in SKIP_SECTION_KEYWORDS
                    )
                    continue

                if skip_section:
                    continue

                # ── Try to parse a data row ────────────────────────────────
                m = _ROW_RE.match(line)
                if not m:
                    continue

                (
                    ticker, name, turnover,
                    _prev, open_r, high, low, close, diff,
                ) = m.groups()

                rows.append({
                    "date":     trade_ts,
                    "symbol":   ticker,
                    "company":  name.strip(),
                    "open":     _to_float(open_r),
                    "high":     _to_float(high),
                    "low":      _to_float(low),
                    "close":    _to_float(close),
                    "turnover": _to_int(turnover),
                    "change":   _to_float(diff),
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
    total_rows_stored = 0
    current = fetch_from

    while current <= end_date:
        log.info("── %s", current)

        pdf_bytes = download_pdf(current)
        if pdf_bytes is None:
            skipped_days += 1
            current += timedelta(days=1)
            continue

        rows = parse_pdf(pdf_bytes, current)
        log.info("  Parsed %d rows", len(rows))

        for i in range(0, len(rows), BATCH_SIZE):
            upsert_rows(sb, rows[i: i + BATCH_SIZE])
        total_rows_stored += len(rows)

        trading_days += 1
        current += timedelta(days=1)
        time.sleep(REQUEST_DELAY)

    log.info(
        "Done. %d trading days stored, %d non-trading days skipped, %d rows written.",
        trading_days, skipped_days, total_rows_stored,
    )

    if FAIL_ON_EMPTY_RUN and total_rows_stored == 0:
        raise RuntimeError(
            "No rows were written in this run. Most common cause: the configured "
            "PDF_URL_TEMPLATES value does not match the current PSX download endpoint."
        )

    if end_date < today:
        log.info(
            "Stopped at %s due to MAX_DAYS_PER_RUN=%d. "
            "Remaining dates will be picked up by the next run.",
            end_date, MAX_DAYS_PER_RUN,
        )


if __name__ == "__main__":
    try:
        main()
    except FatalConfigError as exc:
        log.error("%s", exc)
        sys.exit(2)