"""Pydantic dispatch schema, granular failure tagging, and quarantine split."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from src.config import CONFIG, MarketConfig


class DispatchPriceRecord(BaseModel):
    """Silver-layer dispatch PRICE contract.

    Lineage columns ``_ingested_file`` and ``_ingested_sha256`` are required so
    every Silver row can be traced to an immutable Bronze object.
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    region_id: str
    interval_utc: datetime
    raw_settlement_date: str
    intervention_flag: int
    rrp_price_aud_per_mwh: float | None = None
    eep_price_aud_per_mwh: float | None = None
    ingested_file: str = Field(alias="_ingested_file")
    ingested_sha256: str = Field(alias="_ingested_sha256")
    silver_processed_at_utc: datetime = Field(alias="_silver_processed_at_utc")


def validate_and_tag_quarantine(
    records: list[dict[str, Any]],
    config: MarketConfig = CONFIG,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split records into clean vs quarantined with specific failure codes.

    Multiple violations on one row are joined with ``"; "``. Downstream Gold
    never sees quarantined rows; the pipeline continues for the clean subset.
    """
    clean: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []

    for r in records:
        reasons: list[str] = []
        if not r.get("region_id"):
            reasons.append("MISSING_REGION_KEY")
        elif r["region_id"] not in config.valid_regions:
            reasons.append(f"INVALID_REGION:{r['region_id']}")

        if not r.get("interval_utc"):
            reasons.append("MISSING_INTERVAL_KEY")

        rrp = r.get("rrp_price_aud_per_mwh")
        if rrp is None:
            reasons.append("NULL_PRICE")
        elif rrp < config.market_price_floor_aud:
            reasons.append(f"BELOW_FLOOR:{rrp}<{config.market_price_floor_aud}")
        elif rrp > config.market_price_cap_aud:
            reasons.append(f"EXCEEDS_CAP:{rrp}>{config.market_price_cap_aud}")

        if reasons:
            r["_quarantine_reasons"] = "; ".join(reasons)
            r["_quarantined_at_utc"] = pd.Timestamp.now(tz="UTC")
            quarantined.append(r)
        else:
            clean.append(r)

    return clean, quarantined


def apply_pydantic_schema(
    records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Reject rows that do not match ``DispatchPriceRecord`` (lineage fields included)."""
    clean: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    for r in records:
        try:
            DispatchPriceRecord.model_validate(r)
            clean.append(r)
        except ValidationError as exc:
            tagged = dict(r)
            tagged["_quarantine_reasons"] = f"SCHEMA_INVALID:{exc.error_count()}"
            tagged["_quarantined_at_utc"] = pd.Timestamp.now(tz="UTC")
            quarantined.append(tagged)
    return clean, quarantined
