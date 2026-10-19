from datetime import date

from fhir_common import (
    build_search_url,
    incremental_window,
    next_link,
    row_hash,
)


def test_build_search_url_contains_window_and_format():
    url = build_search_url("https://hapi.fhir.org/baseR4", "Patient", date(2026, 10, 13), date(2026, 10, 16), 50)
    assert url.startswith("https://hapi.fhir.org/baseR4/Patient?")
    assert "_lastUpdated=ge2026-10-13" in url
    assert "_lastUpdated=lt2026-10-16" in url
    assert "_count=50" in url
    assert "_format=json" in url


def test_next_link_found():
    bundle = {"link": [{"relation": "self", "url": "A"}, {"relation": "next", "url": "B"}]}
    assert next_link(bundle) == "B"


def test_next_link_missing_returns_none():
    bundle = {"link": [{"relation": "self", "url": "A"}]}
    assert next_link(bundle) is None


def test_incremental_window_first_run_uses_lookback():
    days = incremental_window(control_last_date=None, lookback_days=3, run_date=date(2026, 10, 16))
    assert days == [date(2026, 10, 13), date(2026, 10, 14), date(2026, 10, 15)]


def test_incremental_window_subsequent_run_continues_from_watermark():
    days = incremental_window(control_last_date=date(2026, 10, 14), lookback_days=3, run_date=date(2026, 10, 16))
    assert days == [date(2026, 10, 15)]


def test_incremental_window_no_new_days_when_caught_up():
    days = incremental_window(control_last_date=date(2026, 10, 15), lookback_days=3, run_date=date(2026, 10, 16))
    assert days == []


def test_row_hash_stable_and_sensitive_to_change():
    h1 = row_hash(["male", "1990-01-01", None])
    h2 = row_hash(["male", "1990-01-01", None])
    h3 = row_hash(["female", "1990-01-01", None])
    assert h1 == h2
    assert h1 != h3
