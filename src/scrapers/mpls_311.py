"""
Minneapolis Regulatory Services code violations scraper.

Source: the CITY OF MINNEAPOLIS's own ArcGIS org (`afSMGVsC7QlRK1kZ`),
service `CaseViolations`, layer 0.

    https://services.arcgis.com/afSMGVsC7QlRK1kZ/ArcGIS/rest/services/CaseViolations/FeatureServer/0

=== REBUILT 2026-08-07 — THIS SCRAPER HAD NEVER WRITTEN A ROW ===
The previous version targeted a Socrata endpoint at
`opendata.minneapolismn.gov/resource/rmpv-bp76.json`. Minneapolis migrated
off Socrata to ArcGIS Hub and that host now returns an ArcGIS Hub
"This site is no longer supported" HTML page **at HTTP 200** — so the
scraper never errored, it just parsed nothing. Measured 2026-08-07:
`signals.code_violations` held 0 rows and `signals.distress_events` held 0
`mpls_311` events, since the scraper was written.

It also targeted four column names that do not exist on
`signals.code_violations`. Both the model and the table were fixed in
task 540; see `src/models/signal.py`.

=== THE SERVICE IS INSPECTIONS, NOT VIOLATIONS ===
Despite the name `CaseViolations`, its `serviceDescription` reads
"Case Inspections for the City of Minneapolis." It is ONE ROW PER
INSPECTION. A condemned property generates a new row at every reinspection.

Measured 2026-08-07:

    367,837   total inspection rows (all results, back to ~1989)
     36,353   rows whose Inspection_Result is a distress code
     11,501   distinct cases behind those
      4,732   distress rows with Completed_Date >= 2024-01-01
      1,391   distinct cases in that window  <-- what this scraper writes
          0   in-window rows missing an APN

So the whole 2024+ window fetches in a SINGLE page (under the layer's
`maxRecordCount` of 16,000) and collapses ~3.4:1 into case rows.

=== WHY A 2024 CUTOFF ===
The service carries NO resolution flag. A condemnation from 2011 with no
way to tell whether it was resolved is HISTORY, not distress — the building
has very likely been repaired, sold or demolished in the years since.
Publishing it as current distress is the same error that put fabricated
redemption deadlines in front of homeowners on 2026-08-07 (see
`outcome_capture/redemption_builder.py`). Better absent than wrong.

Cross-check available: `mpls_vbr` now holds the City's CURRENT vacant
building registry (311 properties). A condemnation here on a property NOT in
that registry has very likely been resolved.

=== RESULT CODES ===
Only codes whose meaning is self-evident are treated as distress. The City
publishes no glossary for this field — the dashboard's "Violation Codes" tab
documents a DIFFERENT vocabulary (BOT, DOT, DFB, F001...) and its "Glossary"
tab documents only dashboard field names. **Undocumented codes are excluded
deliberately**: misclassifying one puts a wrong signal in front of an owner.
Do not add TNC, LINT, LINTCIT, CGI, NVT, ENVGrant, Auth, Continue, FinalMit
or the Conduct*/Admin* families without written confirmation from Regulatory
Services.

=== FIELDS ===
    APN                            13-digit Hennepin PID (verified populated
                                   on 100% of in-window rows)
    Display                        property street address
    Violation_Case_Number          stable per case -> source_id
    Case_Type                      HIS / FIS / VBR / Nuisance / Hazmat
    Inspection_Result              the code (see _DISTRESS_RESULTS)
    Completed_Date                 EPOCH MILLISECONDS, not a string
    Scheduled_Date                 epoch ms
    Violation_Case_ID              internal id
    Violation_Case_Inspection_ID   unique per inspection row
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, ClassVar

from src.models.parcel import ParcelUpsert
from src.models.signal import CodeViolationInsert
from src.scrapers.base_arcgis_scraper import (
    BaseArcGISScraper,
    arcgis_date_to_date_only,
)
from src.services.event_writer import (
    write_events_dedup,
    write_typed_signals_dedup,
)
from src.services.parcel_resolver import resolve_parcel
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id
from src.utils.logger import logger


_FEATURE_SERVICE_URL = (
    "https://services.arcgis.com/afSMGVsC7QlRK1kZ"
    "/ArcGIS/rest/services/CaseViolations/FeatureServer/0"
)

# Earliest Completed_Date we accept. See "WHY A 2024 CUTOFF" above.
_WINDOW_START = "2024-01-01"

# Result codes treated as distress, mapped to severity.
#
# CRITICAL — the property is legally uninhabitable or the City is in court:
#   VACATE   occupants ordered out
#   CON      condemned
#   CONCIT   condemned with citation
#   Summons  criminal citation, city took the owner to court
#
# MEDIUM — vacancy and enforcement escalation:
#   UNOC/UNOCCIT  unoccupied (with citation)
#   VB/VO/VS      vacant building / open / secured
#   VBRRefer      referred into the VBR programme ($7,228/yr clock starts)
#   NOSH          notice of substandard housing
#   Abate         city did the work and billed the owner -> becomes a
#                 special assessment on the tax bill
_DISTRESS_RESULTS: dict[str, str] = {
    "VACATE": "critical",
    "CON": "critical",
    "CONCIT": "critical",
    "Summons": "critical",
    "UNOC": "medium",
    "UNOCCIT": "medium",
    "VB": "medium",
    "VO": "medium",
    "VS": "medium",
    "VBRRefer": "medium",
    "NOSH": "medium",
    "Abate": "medium",
}

# Tiebreak when one case has several inspections on the SAME date. Measured:
# case CE1010509 carries CON and CONCIT both on 2025-04-03, because a
# condemnation and its citation are recorded together. Without a
# deterministic rule the surviving row depends on arbitrary result ordering
# and the same case can flip between runs, churning `status` in the database.
# Higher rank wins.
_RESULT_RANK: dict[str, int] = {
    "VACATE": 100,
    "CONCIT": 90,
    "CON": 80,
    "Summons": 70,
    "NOSH": 60,
    "Abate": 50,
    "UNOCCIT": 40,
    "UNOC": 30,
    "VBRRefer": 25,
    "VO": 20,
    "VS": 15,
    "VB": 10,
}

_SQL_RESULT_LIST = ",".join(f"'{c}'" for c in _DISTRESS_RESULTS)


class MplsThreeOneOneScraper(BaseArcGISScraper[CodeViolationInsert]):
    """Minneapolis Regulatory Services case violations — City ArcGIS source."""

    source_name: ClassVar[str] = "mpls_311"
    signal_type: ClassVar[str] = "code_violation"
    county_code: ClassVar[str] = "hennepin"
    feature_service_url: ClassVar[str] = _FEATURE_SERVICE_URL

    # Filter server-side. The unfiltered layer is 367,837 rows; this brings
    # it to 4,732 — a single page. NEVER remove the Inspection_Result filter:
    # the excluded codes (Complete, Final, Cancel, NotNeeded, NA, ApptSet,
    # Advisory, Monitor, Virtual...) are routine inspection administration,
    # not distress, and would drown the signal 10:1.
    where_clause: ClassVar[str] = (
        f"Inspection_Result IN ({_SQL_RESULT_LIST}) "
        f"AND Completed_Date >= DATE '{_WINDOW_START}'"
    )

    # Point geometry is available but the parcel layer is the authority for
    # coordinates; APN is populated on 100% of in-window rows.
    return_geometry: ClassVar[bool] = False

    # ---- Feature parsing ----

    async def parse_feature(
        self,
        attributes: dict[str, Any],
        geometry: dict[str, Any] | None,
    ) -> CodeViolationInsert | None:
        """Convert one INSPECTION row into a provisional signal.

        Collapsing to one row per case happens in parse() — this method
        cannot do it, because it only ever sees a single feature.
        """
        case_number = attributes.get("Violation_Case_Number")
        if not case_number or not str(case_number).strip():
            return None

        result = str(attributes.get("Inspection_Result") or "").strip()
        severity = _DISTRESS_RESULTS.get(result)
        if severity is None:
            # The server-side filter should make this unreachable. If it
            # fires, the City has added a code and the where_clause and
            # _DISTRESS_RESULTS have drifted apart.
            logger.warning(
                "Unexpected Inspection_Result passed the server filter",
                source=self.source_name,
                inspection_result=result,
                case_number=str(case_number),
            )
            return None

        raw_apn = attributes.get("APN")
        if not raw_apn or not str(raw_apn).strip():
            # Measured 0 of 4,732 in-window rows. Skip rather than
            # synthesise: a code violation with no property is not a lead.
            return None
        parcel_id, err = safe_normalize_parcel_id("hennepin", str(raw_apn))
        if parcel_id is None:
            logger.warning(
                "Could not normalize Minneapolis APN",
                source=self.source_name,
                apn=str(raw_apn),
                case_number=str(case_number),
                error=err,
            )
            return None

        # Completed_Date is EPOCH MILLISECONDS on this service, not a string.
        completed = arcgis_date_to_date_only(attributes.get("Completed_Date"))
        violation_date: date | None = None
        if completed:
            try:
                violation_date = date.fromisoformat(completed)
            except ValueError:
                violation_date = None

        # A COMPLETION DATE IN THE FUTURE IS NOT A COMPLETION (2026-08-23).
        #
        # where_clause bounds Completed_Date below at _WINDOW_START and not
        # above, so a mistyped year in the City's own data passes straight
        # through. Measured: case CE1341491, a VBR Monitoring inspection with
        # Inspection_Result VO at 1916 JACKSON ST NE, carried
        # Completed_Date 1798502400000 = 2026-12-29 — four months ahead. Its
        # Scheduled_Date was 2025-11-09, so the inspection is real and only
        # the completion year is wrong.
        #
        # One row of 1,321, but the damage is out of proportion to the count,
        # and the WORST part is not the display:
        #
        #   * parse() below collapses inspections to one row per case and
        #     rule 1 is "latest violation_date wins". A future date is ALWAYS
        #     the latest, so the bad row wins the case and every correctly
        #     dated inspection on it is DISCARDED IN MEMORY, never written.
        #     Case CE1341491 holds exactly one row in the database for that
        #     reason.
        #   * event_date drives source_max_date, so this source reads as the
        #     freshest on the platform indefinitely.
        #   * date-descending is the default sort, so it sits above all 2,023
        #     other rows in the vacant category.
        #
        # Nulling rather than dropping is deliberate, and rule 3 of the
        # collapse is why: "rows with no date lose to any row that has one".
        # A nulled row stops being a candidate the moment a real inspection
        # exists on the same case, and survives only when it is the only
        # inspection there — which is the honest outcome. 666 live events
        # across four sources already carry a null event_date, and the view
        # handles them: redemption_days_left null, redemption_sort_bucket
        # populated on all 666.
        #
        # THE GUARD BELONGS HERE, BEFORE THE COLLAPSE. In parse() the bad row
        # would already have won.
        #
        # Log loudly. One row is a City typo. Fifty next month means
        # Completed_Date has changed meaning, and this warning naming the case
        # number is the only way that becomes visible.
        if violation_date is not None:
            today = datetime.now(timezone.utc).date()
            if violation_date > today:
                logger.warning(
                    "Minneapolis Completed_Date is in the future — treating "
                    "the case as undated rather than letting it win the "
                    "per-case collapse",
                    source=self.source_name,
                    case_number=str(case_number).strip(),
                    inspection_id=attributes.get(
                        "Violation_Case_Inspection_ID"
                    ),
                    completed_date=violation_date.isoformat(),
                    scheduled_date=arcgis_date_to_date_only(
                        attributes.get("Scheduled_Date")
                    ),
                    inspection_result=result,
                    today=today.isoformat(),
                )
                violation_date = None

        return CodeViolationInsert(
            parcel_id=parcel_id,
            county_code=self.county_code,
            city="Minneapolis",
            violation_type=result,
            violation_date=violation_date,
            status=str(attributes.get("Case_Type") or "").strip() or None,
            source_id=str(case_number).strip(),
            raw_data={
                "attributes": attributes,
                "address": str(attributes.get("Display") or "").strip() or None,
                "case_type": str(attributes.get("Case_Type") or "").strip() or None,
                "inspection_result": result,
                "inspection_id": attributes.get("Violation_Case_Inspection_ID"),
                "_source": self.source_name,
                "_window_start": _WINDOW_START,
            },
            observed_at=datetime.now(timezone.utc),
            source=self.source_name,
            violation_description=(
                f"{result} — {attributes.get('Case_Type') or 'case'}"
            ),
            severity=severity,  # type: ignore[arg-type]
        )

    # ---- Collapse inspections to cases ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[CodeViolationInsert]:
        """Parse every inspection, then keep ONE row per case.

        A property with a Tier 1 inspection, three reinspections and a
        warning is ONE distress signal, not five. Measured 2026-08-07:
        4,732 in-window inspections collapse to 1,391 cases.

        Selection rule, in order:
          1. latest violation_date wins (the current state of the case)
          2. on a DATE TIE, the higher-ranked result wins — see _RESULT_RANK
          3. rows with no date lose to any row that has one

        Rule 2 is not cosmetic. Without it the surviving row for a case with
        CON and CONCIT on the same day is arbitrary, and can differ between
        runs.
        """
        signals = await super().parse(raw_records)

        best: dict[str, CodeViolationInsert] = {}
        for sig in signals:
            existing = best.get(sig.source_id)
            if existing is None:
                best[sig.source_id] = sig
                continue

            new_date = sig.violation_date
            old_date = existing.violation_date

            if new_date is None and old_date is not None:
                continue
            if old_date is None and new_date is not None:
                best[sig.source_id] = sig
                continue

            if new_date is not None and old_date is not None:
                if new_date > old_date:
                    best[sig.source_id] = sig
                    continue
                if new_date < old_date:
                    continue

            # Same date (or both None) — rank decides.
            new_rank = _RESULT_RANK.get(sig.violation_type or "", 0)
            old_rank = _RESULT_RANK.get(existing.violation_type or "", 0)
            if new_rank > old_rank:
                best[sig.source_id] = sig

        collapsed = list(best.values())
        logger.info(
            "Minneapolis case violations collapsed",
            source=self.source_name,
            inspections=len(signals),
            cases=len(collapsed),
        )
        return collapsed

    # ---- Write ----

    async def write(
        self,
        signals: list[CodeViolationInsert],
    ) -> tuple[int, int, int]:
        """Persist signals: resolve parcels, write typed rows + unified events."""
        if not signals:
            return 0, 0, 0

        # --- Step 1: Resolve each unique parcel ---
        unique_parcels: dict[str, ParcelUpsert] = {}
        for sig in signals:
            if sig.parcel_id in unique_parcels:
                continue
            # ADDRESS DELIBERATELY NOT SENT (2026-08-14).
            #
            # This scraper is NOT the source of record for a parcel's address —
            # hennepin_parcels is, from the county assessor. And the city's
            # Display string is demonstrably unreliable: measured across 1,343
            # mpls_311 events, 70 carry a Display address that contradicts the
            # SAME RECORD's own APN and coordinates. Examples, each verified by
            # point-in-parcel against core.parcels.geom:
            #
            #   Display "2424 ALDRICH AVE N" -> coordinates land 0.5m inside
            #   the parcel the assessor calls 2424 ALDRICH AVE S
            #   Display "2200 EMERSON AVE N" -> 0.3m inside 2200 EMERSON AVE S
            #   Display "3520 43RD ST E"     -> 0.3m inside 3520 43RD ST W
            #   Display "1700 LAKE ST W"     -> 0.5m inside 1700 LAKE ST E
            #
            # In Minneapolis N and S are different halves of the city, so these
            # are different houses. The APN and the coordinates agree with each
            # other; only the Display string disagrees. The city publishes a
            # record that contradicts itself.
            #
            # WHY THIS MATTERED EVEN THOUGH NOTHING IS CURRENTLY CORRUPTED:
            # resolve_parcel applies FILL-IN semantics to address — it writes
            # only when the existing value is null or empty. So a parcel that
            # already has the assessor's address is safe TODAY, purely because
            # hennepin_parcels happened to get there first. That is ordering,
            # not design. Reach a parcel before the assessor loader does — a
            # new parcel, or a county onboarded signal-first — and the wrong
            # address lands, and fill-in then makes it PERMANENT, because
            # fill-in can never correct a populated value. Identical trap to
            # the one that left another county's property_type on 6,268 rows.
            #
            # The address stays in raw_data (it is what the city said, and
            # that is worth keeping); it just does not get written to the
            # spine. resolve_parcel is still called so the parcel row exists
            # and the event's FK holds.
            unique_parcels[sig.parcel_id] = ParcelUpsert(
                parcel_id=sig.parcel_id,
                county_code=self.county_code,
                state="MN",
                # city is safe where address is not: every one of these
                # complaints IS in Minneapolis. It is a constant about the
                # source, not a per-parcel claim that can be wrong.
                city="Minneapolis",
                data_sources=[self.source_name],
                last_observed_at=datetime.now(timezone.utc),
            )

        # resolve_parcel returns None on failure. Its result MUST be counted
        # and folded into the run's failure total — a discarded return value
        # let three scrapers report `failed=0` on 2026-08-07 while every one
        # of their parcel upserts was failing with 42P10.
        parcels_ok = 0
        parcels_failed = 0
        for parcel_payload in unique_parcels.values():
            if resolve_parcel(parcel_payload) is not None:
                parcels_ok += 1
            else:
                parcels_failed += 1

        # --- Step 2: Write typed signals.code_violations rows ---
        # These four have no column on signals.code_violations; they live in
        # raw_data and drive the distress_events projection. Sending them
        # would raise PGRST204 before on_conflict is evaluated.
        _IN_MEMORY_ONLY = {
            "source", "violation_description", "resolved_date", "severity",
        }
        signal_rows = []
        for sig in signals:
            row = sig.model_dump(mode="json", exclude_none=True)
            for k in _IN_MEMORY_ONLY:
                row.pop(k, None)
            signal_rows.append(row)

        # Conflict target must match signals.code_violations_dedup, created
        # 2026-08-07 as (county_code, source_id) NULLS NOT DISTINCT while the
        # table was empty. source_id carries Violation_Case_Number, which is
        # stable per case. Deliberately NOT keyed on parcel_id — one property
        # can hold several concurrent cases (measured: 1801 EMERSON AVE N has
        # CE1010509 and CE1010510) — nor on violation_date, because a
        # corrected date should UPDATE the row, not create a second one.
        new_typed, failed_typed = write_typed_signals_dedup(
            "code_violations",
            signal_rows,
            on_conflict="county_code,source_id",
        )

        # --- Step 3: Write unified distress_events ---
        events = [sig.to_event() for sig in signals]
        new_events, failed_events = write_events_dedup(events)

        critical = sum(1 for s in signals if s.severity == "critical")

        logger.info(
            "Minneapolis code violations write complete",
            parcels_ok=parcels_ok,
            parcels_failed=parcels_failed,
            typed_new=new_typed,
            events_new=new_events,
            critical=critical,
            failed=failed_typed + failed_events + parcels_failed,
        )

        return (new_typed, 0, failed_typed + failed_events + parcels_failed)


__all__ = ["MplsThreeOneOneScraper"]
