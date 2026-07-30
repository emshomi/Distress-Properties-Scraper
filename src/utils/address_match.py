@router.get(
    "/connect/lookup",
    status_code=http_status.HTTP_200_OK,
    summary="Find a property — masked, no distress detail",
)
async def connect_lookup(
    address: str = Query(..., min_length=3, max_length=200),
    city: Optional[str] = Query(default=None, max_length=60),
    county: Optional[str] = Query(default=None, max_length=40),
) -> dict[str, Any]:
    """Step one. Returns whether we hold a record and a MASKED address only.

    Deliberately returns NO redemption date, NO owner name and NO distress
    information — see the privacy rule in the module docstring. A stranger
    fuzzing addresses learns nothing they did not already know.

    MATCHING (rebuilt 2026-07-30):
      * The typed address is normalized the same way the stored one is —
        'Avenue North' and 'Ave. N.' both become 'AVE N'. Without this, an
        owner who writes their address the way they would on a letter is told
        we have no record of a property we hold perfectly well.
      * The house number is matched with a LEADING anchor, not a substring.
        Unanchored, a search for 5331 also returns 15331 Oak and anything on
        Highway 5331 — across 1.1M parcels that is noise from the whole metro,
        and the owner only sees masked addresses so cannot tell which is
        theirs.
      * A bare house number is refused outright rather than answered with ten
        unrelated properties.
    """
    if not is_searchable(address):
        return success_envelope({
            "query": address,
            "match_count": 0,
            "matches": [],
            "needs_more_input": True,
            "next_step": (
                "Please include the street name as well as the number — "
                "for example, 5331 Angeline."
            ),
        })

    normalized = normalize_address(address)
    number, street = split_house_number(normalized)

    try:
        q = core_table("parcels").select(
            "parcel_id, address, city, county_code, year_built, property_type"
        )
        if county:
            q = q.eq("county_code", county.strip().lower())
        if city:
            q = q.ilike("city", city.strip())
        # Anchor on the house number; the street name is filtered in Python
        # after normalization, because the stored value has not been
        # normalized and no SQL LIKE can do the suffix mapping.
        q = q.ilike("address", f"{number}%") if number else q.ilike(
            "address", f"%{normalized[:40]}%"
        )
        res = q.limit(60).execute()
        rows = res.data or []
    except Exception as e:
        logger.warning("connect lookup failed", error_type=type(e).__name__)
        rows = []

    # Keep rows whose normalized address starts with what they typed. This is
    # what lets '5331 Angeline' find '5331 ANGELINE AVE N' while still
    # rejecting '5331 Angelica Dr'.
    candidates = []
    for r in rows:
        stored = normalize_address(r.get("address"))
        if not stored:
            continue
        if stored.startswith(normalized) or normalized.startswith(stored):
            candidates.append(r)

    # Collapse duplicates. A foreclosed property often appears TWICE: once as
    # the real assessor parcel and once as a synthetic '<COUNTY>-FC-*'
    # placeholder minted by the foreclosure path when it could not resolve a
    # parcel. Verified live: '5331 Angeline' returned both '0911821120148' and
    # 'HENNEPIN-FC-2606002'.
    #
    # Two identical-looking rows is confusing for anyone, and actively harmful
    # here — if the owner picks the synthetic one, /connect/status finds no
    # assessed value, because synthetic parcels carry none. So when a real
    # parcel exists for an address, the placeholder is dropped.
    best: dict[str, dict[str, Any]] = {}
    for r in candidates:
        key = normalize_address(r.get("address"))
        is_synthetic = "-FC-" in (r["parcel_id"] or "")
        existing = best.get(key)
        if existing is None or (existing["_synthetic"] and not is_synthetic):
            best[key] = {
                "parcel_id": r["parcel_id"],
                "masked_address": _mask_address(r.get("address")),
                "city": r.get("city"),
                "county_code": r.get("county_code"),
                # Included so the owner can RECOGNISE their own home from a
                # masked address. '5XXX ANGELINE AVE N' alone is hard to
                # confirm on a street with similar numbers. These add nothing
                # a stranger could not read on the county's own public site.
                "year_built": r.get("year_built"),
                "property_type": r.get("property_type"),
                "_synthetic": is_synthetic,
            }
    matches = [
        {k: v for k, v in m.items() if not k.startswith("_")}
        for m in list(best.values())[:10]
    ]

    logger.info("connect lookup", county=county, has_city=bool(city),
                matches=len(matches))
    return success_envelope({
        "query": address,
        "match_count": len(matches),
        "matches": matches,
        "needs_more_input": False,
        "next_step": (
            "If one of these is your property, confirm you are the owner to "
            "see your redemption deadline and what is at stake."
        ),
    })
