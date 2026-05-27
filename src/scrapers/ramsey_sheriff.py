"""
Ramsey County Sheriff Sale scraper.

Same general approach as Hennepin (PDF download + pdfplumber extraction)
but with Ramsey-specific URLs and 12-digit PID format.

PDFs are published at:
    https://www.ramseycounty.us/your-government/leadership/sheriffs-office/sheriffs-office-services/civil-process/sheriff-sales
"""

from __future__ import annotations

from typing import ClassVar

from src.scrapers.hennepin_sheriff import HennepinSheriffScraper


_RAMSEY_INDEX_URL = (
    "https://www.ramseycounty.us/your-government/leadership/"
    "sheriffs-office/sheriffs-office-services/civil-process/sheriff-sales"
)


class RamseySheriffScraper(HennepinSheriffScraper):
    """Ramsey County sheriff sale PDFs."""

    source_name: ClassVar[str] = "ramsey_sheriff"
    signal_type: ClassVar[str] = "sheriff_sale"
    county_code: ClassVar[str] = "ramsey"
    index_url: ClassVar[str] = _RAMSEY_INDEX_URL


__all__ = ["RamseySheriffScraper"]
