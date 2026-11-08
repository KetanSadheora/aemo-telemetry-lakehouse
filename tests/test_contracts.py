"""Unit tests for granular quarantine tagging."""

from __future__ import annotations

import pandas as pd

from src.quality.contracts import validate_and_tag_quarantine


def test_quarantine_granular_reasons():
    bad_batch = [
        {
            "region_id": "INVALID_STATE",
            "interval_utc": pd.Timestamp.now(tz="UTC"),
            "rrp_price_aud_per_mwh": 100.0,
        },
        {
            "region_id": "NSW1",
            "interval_utc": pd.Timestamp.now(tz="UTC"),
            "rrp_price_aud_per_mwh": 50000.0,
        },
        {
            "region_id": "VIC1",
            "interval_utc": pd.Timestamp.now(tz="UTC"),
            "rrp_price_aud_per_mwh": -1500.0,
        },
    ]
    clean, quar = validate_and_tag_quarantine(bad_batch)
    assert len(clean) == 0
    assert len(quar) == 3
    assert "INVALID_REGION" in quar[0]["_quarantine_reasons"]
    assert "EXCEEDS_CAP" in quar[1]["_quarantine_reasons"]
    assert "BELOW_FLOOR" in quar[2]["_quarantine_reasons"]


def test_quarantine_missing_keys_and_null_price():
    bad_batch = [
        {"region_id": "", "interval_utc": None, "rrp_price_aud_per_mwh": None},
    ]
    clean, quar = validate_and_tag_quarantine(bad_batch)
    assert clean == []
    reasons = quar[0]["_quarantine_reasons"]
    assert "MISSING_REGION_KEY" in reasons
    assert "MISSING_INTERVAL_KEY" in reasons
    assert "NULL_PRICE" in reasons


def test_clean_record_not_quarantined():
    good = [
        {
            "region_id": "NSW1",
            "interval_utc": pd.Timestamp.now(tz="UTC"),
            "rrp_price_aud_per_mwh": 120.5,
        }
    ]
    clean, quar = validate_and_tag_quarantine(good)
    assert len(clean) == 1
    assert quar == []
