"""
Hennepin County Sheriff Sale scraper.

Downloads the weekly sheriff sale notice PDF, extracts the parcel records
via pdfplumber (tables-first, text fallback), and writes sheriff_sale
signals.

PDFs are published at:
    https://www.hennepinsheriff.org/sheriff-sales/notices
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Any, ClassVar

import httpx
import pdfplumber
from bs4 import BeautifulSoup

from src.config import settings
from src.models.parcel import ParcelUpsert
from src.models.signal import SheriffSaleInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.audit_logger import log_error
from src.services.event_writer import (
    write_events_dedup,
    write_typed_signals_dedup,
)
from src.services.parcel_resolver import resolve_parcel
from src.utils.errors import ParseError, SourceUnavailableError
from src.utils.logger import logger
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id
from src.utils.retry import retry_on_transient

# Index page listing recent sheriff sale notice PDFs
_INDEX_URL = "https://www.hennepinsheriff.org/sheriff-sales/notices"

# Regex to find PIDs in extracted text (13 digits, optionally with separators)
_PID_PATTERN = re.compile(r"\b(\d{2}[-.\s]?\d{3}[-.\s]?\d{2}[-.\s]?\d{2}[-.\s]?\d{4})\b")

# Regex to extract dollar amounts
_AMOUNT_PATTERN = re.compile(r"\$\s*([\d,]+(?:\.\d{2})?)")

# Regex for dates like MM/DD/YYYY or MM-DD-YYYY
_DATE_PATTERN = re.compile(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b")


def _parse_amount(text: str) -> Decimal | None:
    """Extract a dollar amount from text, or None."""
    match = _AMOUNT_PATTERN.search(text)
    if not match:
        return None
    raw = match.group(1).replace(",", "")
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return None


def _parse_date(text: str) -> date | None:
    """Extract a MM/DD/YYYY date from text, or None."""
    match = _DATE_PATTERN.search(text)
    if not match:
        return None
    try:
        month, day, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
        return date(year, month, day)
    except (ValueError, TypeError):
        return None


def _split_into_blocks(text: str) -> list[str]:
    """
    Split PDF text into per-parcel blocks anchored on PIDs.

    Each block contains one PID and the text around it. The split point is
    where the next PID appears.
    """
    pids: list[tuple[int, str]] = [
        (m.start(), m.group(1)) for m in _PID_PATTERN.finditer(text)
    ]
    if not pids:
        return []

    blocks: list[str] = []
    for i, (start, _pid) in enumerate(pids):
        end = pids[i + 1][0] if i + 1 < len(pids) else len(text)
        blocks.append(text[start:end])
    return blocks


class HennepinSheriffScraper(BaseScraper[dict[str, Any], SheriffSaleInsert]):
    """Hennepin County sheriff sale PDFs."""

    source_name: ClassVar[str] = "hennepin_sheriff"
    signal_type: ClassVar[str] = "sheriff_sale"
    county_code: ClassVar[str] = "hennepin"

    # Index URL (overridden by subclasses for other counties)
    index_url: ClassVar[str] = _INDEX_URL

    @retry_on_transient(source="hennepin_sheriff")
    async def _fetch_index(self, client: httpx.AsyncClient) -> list[str]:
        """Fetch the notices index page and extract PDF URLs."""
        try:
            response = await client.get(self.index_url, follow_redirects=True)
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"Index fetch failed: {e}", source=self.source_name
            ) from e

        soup = BeautifulSoup(response.text, "lxml")
        pdf_urls: list[str] = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.lower().endswith(".pdf"):
                # Resolve relative URLs
                if href.startswith("/"):
                    href = f"https://www.hennepinsheriff.org{href}"
                pdf_urls.append(href)

        # Dedup, preserve order
        seen: set[str] = set()
        result: list[str] = []
        for url in pdf_urls:
            if url not in seen:
                seen.add(url)
                result.append(url)
        return result

    @retry_on_transient(source="hennepin_sheriff")
    async def _fetch_pdf(self, client: httpx.AsyncClient, url: str) -> bytes:
        """Download a PDF file's bytes."""
        try:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()
            return response.content
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"PDF download failed: {e}",
                source=self.source_name,
                context={"url": url},
            ) from e

    def _extract_text(self, pdf_bytes: bytes) -> str:
        """Extract all text from a PDF — tables-first, then prose fallback."""
        text_parts: list[str] = []
        try:
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    # Try tables first
                    tables = page.extract_tables() or []
                    for table in tables:
                        for row in table:
                            row_text = " ".join(
                                str(cell) for cell in row if cell is not None
                            )
                            text_parts.append(row_text)
                    # Always also extract prose text — tables may miss content
                    prose = page.extract_text()
                    if prose:
                        text_parts.append(prose)
        except Exception as e:
            raise ParseError(
                f"PDF parse failed: {e}", source=self.source_name
            ) from e
        return "\n".join(text_parts)

    def _parse_block(self, block: str, source_url: str) -> SheriffSaleInsert | None:
        """Parse a single per-parcel text block."""
        pid_match = _PID_PATTERN.search(block)
        if not pid_match:
            return None

        raw_pid = pid_match.group(1)
        normalized_pid, err = safe_normalize_parcel_id(self.county_code, raw_pid)
        if normalized_pid is None:
            log_error(
                run_id=None,
                error_type="validation_error",
                error_message=f"Bad PID in PDF: {err}",
                raw_record={"block": block[:500]},
            )
            return None

        sale_date = _parse_date(block)
        if sale_date is None:
            # Sale date is essential — skip if absent
            return None

        amount = _parse_amount(block)

        # Try to extract plaintiff/defendant from "X vs Y" pattern
        plaintiff: str | None = None
        defendant: str | None = None
        vs_match = re.search(r"(.{1,200})\s+v(?:s|\.)\s+(.{1,200})", block, re.IGNORECASE)
        if vs_match:
            plaintiff = vs_match.group(1).strip()[:500]
            defendant = vs_match.group(2).strip()[:500]

        return SheriffSaleInsert(
            parcel_id=normalized_pid,
            sale_date=sale_date,
            sale_amount=amount,
            plaintiff=plaintiff,
            defendant=defendant,
            source=self.source_name,
            raw_data={"block": block, "source_url": source_url},
            observed_at=datetime.now(timezone.utc),
        )

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """Download index page, then top N PDFs, return as raw blob list."""
        async with httpx.AsyncClient(
            timeout=settings.scraper_request_timeout_seconds * 2,
            headers={"User-Agent": "DistressProperties/1.0"},
        ) as client:
            pdf_urls = await self._fetch_index(client)
            logger.info(
                "Found sheriff sale PDFs",
                source=self.source_name,
                count=len(pdf_urls),
            )

            # Only process the 3 most recent (avoid re-processing all history)
            raw_blobs: list[dict[str, Any]] = []
            for url in pdf_urls[:3]:
                try:
                    pdf_bytes = await self._fetch_pdf(client, url)
                    raw_blobs.append({"url": url, "bytes": pdf_bytes})
                except SourceUnavailableError as e:
                    logger.warning(
                        "Skipping PDF that failed to download",
                        url=url,
                        error=str(e),
                    )
            return raw_blobs

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[SheriffSaleInsert]:
        """Parse the PDFs into SheriffSaleInsert rows."""
        signals: list[SheriffSaleInsert] = []
        for record in raw_records:
            url = record["url"]
            pdf_bytes = record["bytes"]
            try:
                text = self._extract_text(pdf_bytes)
                blocks = _split_into_blocks(text)
                for block in blocks:
                    parsed = self._parse_block(block, url)
                    if parsed is not None:
                        signals.append(parsed)
            except ParseError as e:
                log_error(
                    run_id=None,
                    error_type="parse_error",
                    error_message=str(e),
                    raw_record={"url": url},
                )
        return signals

    async def write(self, signals: list[SheriffSaleInsert]) -> tuple[int, int, int]:
        if not signals:
            return 0, 0, 0

        # Resolve unique parcels
        unique_pids: dict[str, ParcelUpsert] = {}
        for sig in signals:
            if sig.parcel_id not in unique_pids:
                unique_pids[sig.parcel_id] = ParcelUpsert(
                    parcel_id=sig.parcel_id,
                    county_code=self.county_code,
                    data_sources=[self.source_name],
                )

        for parcel_payload in unique_pids.values():
            resolve_parcel(parcel_payload)

        # Write typed sheriff_sales rows
        signal_rows = [sig.model_dump(mode="json", exclude_none=True) for sig in signals]
        new_typed, failed_typed = write_typed_signals_dedup(
            "sheriff_sales",
            signal_rows,
            on_conflict="parcel_id,sale_date,source",
        )

        # Write unified events
        events = [sig.to_event() for sig in signals]
        new_events, failed_events = write_events_dedup(events)

        return new_typed, 0, failed_typed + failed_events


__all__ = ["HennepinSheriffScraper"]
