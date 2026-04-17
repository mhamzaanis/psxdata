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

TABLE_NAME         = "datatable"
SUMMARY_TABLE_NAME = "market_daily_summary"
START_DATE         = date.fromisoformat(os.getenv("START_DATE", "2018-01-01"))

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

# Fail the run if no rows are stored; prevents silent no-op runs.
FAIL_ON_EMPTY_RUN = os.getenv("FAIL_ON_EMPTY_RUN", "1").strip().lower() not in {
    "0", "false", "no",
}

# Sections to skip entirely (futures, bonds, defaulters)
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
    return "PGRST205" in msg and (TABLE_NAME in msg or SUMMARY_TABLE_NAME in msg)


# ──────────────────────────────────────────────────────────────────────────────
# Supabase helpers
# ──────────────────────────────────────────────────────────────────────────────

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def get_last_stored_date(sb: Client) -> date | None:
    """Return the most recent trade_date stored in market_daily_summary, or None."""
    try:
        res = (
            sb.table(SUMMARY_TABLE_NAME)
            .select("trade_date")
            .order("trade_date", desc=True)
            .limit(1)
            .execute()
        )
    except APIError as exc:
        if _is_missing_table_error(exc):
            raise FatalConfigError(
                f"Supabase table '{SUMMARY_TABLE_NAME}' was not found. "
                "Run the schema SQL first and retry."
            ) from exc
        raise

    if res.data:
        return date.fromisoformat(str(res.data[0]["trade_date"])[:10])
    return None


def upsert_summary(sb: Client, summary: dict) -> None:
    """Insert or update the market-level daily summary row."""
    try:
        sb.table(SUMMARY_TABLE_NAME).upsert(
            summary, on_conflict="trade_date"
        ).execute()
    except APIError as exc:
        if _is_missing_table_error(exc):
            raise FatalConfigError(
                f"Supabase table '{SUMMARY_TABLE_NAME}' was not found."
            ) from exc
        raise
    log.info("    Upserted market summary for %s", summary["trade_date"])


def upsert_rows(sb: Client, rows: list[dict]) -> None:
    """Upsert ticker rows, deduplicating on (symbol, trade_date)."""
    if not rows:
        return
    try:
        sb.table(TABLE_NAME).upsert(
            rows, on_conflict="symbol,trade_date"
        ).execute()
    except APIError as exc:
        if _is_missing_table_error(exc):
            raise FatalConfigError(
                f"Supabase table '{TABLE_NAME}' was not found."
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
                    log.warning("  %s -> 200 but non-PDF from %s", d, url)
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

    log.info("  %s -> PDF not found, skipping", d)
    return None


# ──────────────────────────────────────────────────────────────────────────────
# PDF parsing
# ──────────────────────────────────────────────────────────────────────────────

# Matches a data row such as:
#   LUCK Lucky Cement 2920671 435.77 439.99 444 430.15 435.5 -0.27
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

# PDF header line examples:
#   P. Vol.: 229930650  P.KSE100 Ind: 43740.49  P.KSE 30 Ind: 22059.24  Plus : 170
#   C. Vol.: 137042470  C.KSE100 Ind: 43829.08  C.KSE 30 Ind: 22090.54  Minus: 172
#   Total      359  Net Change: 88.59   Net Change: 31.30   Equal: 17
#   Flu No: 045/2018
_PVOL_RE   = re.compile(r"P\.\s*Vol\.:\s*([\d,]+)")
_CVOL_RE   = re.compile(r"C\.\s*Vol\.:\s*([\d,]+)")
_PKSE100_RE = re.compile(r"P\.KSE100\s+Ind:\s*([\d.]+)")
_CKSE100_RE = re.compile(r"C\.KSE100\s+Ind:\s*([\d.]+)")
_PKSE30_RE  = re.compile(r"P\.KSE\s*30\s+Ind:\s*([\d.]+)")
_CKSE30_RE  = re.compile(r"C\.KSE\s*30\s+Ind:\s*([\d.]+)")
_PLUS_RE    = re.compile(r"Plus\s*:\s*(\d+)")
_MINUS_RE   = re.compile(r"Minus\s*:\s*(\d+)")
_EQUAL_RE   = re.compile(r"Equal\s*:\s*(\d+)")
_FLUNO_RE   = re.compile(r"Flu\s+No[:\s]+([\w/]+)")


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


def _extract_header(full_text: str) -> dict:
    """Parse the market-level summary fields from the first page text."""

    def _find(pattern: re.Pattern) -> str | None:
        m = pattern.search(full_text)
        return m.group(1).replace(",", "") if m else None

    return {
        "prev_volume":    _to_int(_find(_PVOL_RE)),
        "curr_volume":    _to_int(_find(_CVOL_RE)),
        "kse100_prev":    _to_float(_find(_PKSE100_RE)),
        "kse100_close":   _to_float(_find(_CKSE100_RE)),
        "kse100_change":  round(
            (_to_float(_find(_CKSE100_RE)) or 0)
            - (_to_float(_find(_PKSE100_RE)) or 0),
            2,
        ),
        "kse30_prev":     _to_float(_find(_PKSE30_RE)),
        "kse30_close":    _to_float(_find(_CKSE30_RE)),
        "kse30_change":   round(
            (_to_float(_find(_CKSE30_RE)) or 0)
            - (_to_float(_find(_PKSE30_RE)) or 0),
            2,
        ),
        "advances":       _to_int(_find(_PLUS_RE)),
        "declines":       _to_int(_find(_MINUS_RE)),
        "unchanged":      _to_int(_find(_EQUAL_RE)),
        "flu_no":         _find(_FLUNO_RE),
    }


def parse_pdf(pdf_bytes: bytes, trade_date: date) -> tuple[dict, list[dict]]:
    """
    Parse all pages of the PDF.

    Returns:
        summary  – dict shaped for market_daily_summary
        rows     – list of dicts shaped for datatable
    """
    rows: list[dict] = []
    current_section = "UNKNOWN"
    skip_section    = False
    full_first_page = ""

    trade_date_str = trade_date.isoformat()   # plain date string: "2018-03-05"

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""

            if page_idx == 0:
                full_first_page = text

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
                    "trade_date": trade_date_str,
                    "symbol":     ticker,
                    "company":    name.strip(),
                    "open":       _to_float(open_r),
                    "high":       _to_float(high),
                    "low":        _to_float(low),
                    "close":      _to_float(close),
                    "turnover":   _to_int(turnover),
                    "change":     _to_float(diff),
                    "section":    current_section,
                })

    summary = _extract_header(full_first_page)
    summary["trade_date"] = trade_date_str

    return summary, rows


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

        summary, rows = parse_pdf(pdf_bytes, current)
        log.info("  Parsed %d ticker rows", len(rows))

        # Always insert the summary first (FK parent)
        upsert_summary(sb, summary)

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