"""Parse Bronze CSV, quarantine contract failures, MERGE clean rows into Silver."""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import pyarrow as pa
import structlog
from deltalake import DeltaTable, write_deltalake

from src.config import CONFIG, MarketConfig
from src.quality.contracts import apply_pydantic_schema, validate_and_tag_quarantine

logger = structlog.get_logger()

SILVER_COMPOSITE_KEY = (
    "target.region_id = source.region_id AND target.interval_utc = source.interval_utc"
)


def _parse_dispatch_price_rows(
    bronze_path: str,
    audit_meta: dict[str, Any],
) -> list[dict[str, Any]]:
    with open(bronze_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f]

    price_lines = [line.split(",") for line in lines if line.startswith("D,DISPATCH,PRICE,")]
    if not price_lines:
        raise ValueError("No price records extracted from Bronze CSV.")

    parsed_records: list[dict[str, Any]] = []
    for parts in price_lines:
        parts = [p.strip().strip('"') for p in parts]
        raw_settlement_date = parts[4]
        region_id = parts[6]
        intervention_flag = int(parts[8]) if parts[8] else 0
        rrp = float(parts[9]) if parts[9] else None
        eep = float(parts[10]) if len(parts) > 10 and parts[10] else None

        dt_aest = pd.to_datetime(raw_settlement_date, format="%Y/%m/%d %H:%M:%S").tz_localize(
            "Etc/GMT-10"
        )
        interval_utc = dt_aest.tz_convert("UTC")

        parsed_records.append(
            {
                "region_id": region_id,
                "interval_utc": interval_utc,
                "raw_settlement_date": raw_settlement_date,
                "intervention_flag": intervention_flag,
                "rrp_price_aud_per_mwh": rrp,
                "eep_price_aud_per_mwh": eep,
                "_ingested_file": os.path.basename(bronze_path),
                "_ingested_sha256": audit_meta.get("sha256_hash"),
                "_silver_processed_at_utc": pd.Timestamp.now(tz="UTC"),
            }
        )
    return parsed_records


def process_bronze_to_silver(
    bronze_path: str,
    audit_meta: dict[str, Any],
    silver_path: str | None = None,
    quarantine_path: str | None = None,
    config: MarketConfig = CONFIG,
) -> tuple[int, int]:
    """Validate PRICE rows and MERGE clean records on ``(region_id, interval_utc)``.

    Silver always carries ``_ingested_file`` and ``_ingested_sha256``. Quarantine
    writes are append-only and never block the clean MERGE.
    """
    silver_path = silver_path or config.silver_path
    quarantine_path = quarantine_path or config.quarantine_path

    parsed_records = _parse_dispatch_price_rows(bronze_path, audit_meta)
    clean_records, quarantined_records = validate_and_tag_quarantine(parsed_records, config)
    schema_ok, schema_bad = apply_pydantic_schema(clean_records)
    clean_records = schema_ok
    quarantined_records.extend(schema_bad)

    if quarantined_records:
        os.makedirs(os.path.dirname(quarantine_path) or ".", exist_ok=True)
        arrow_quar = pa.Table.from_pandas(pd.DataFrame(quarantined_records))
        write_deltalake(quarantine_path, arrow_quar, mode="append")
        logger.warning("records_quarantined", count=len(quarantined_records), path=quarantine_path)

    if clean_records:
        os.makedirs(os.path.dirname(silver_path) or ".", exist_ok=True)
        arrow_clean = pa.Table.from_pandas(pd.DataFrame(clean_records))
        delta_log = os.path.join(silver_path, "_delta_log")
        if not os.path.exists(delta_log):
            write_deltalake(silver_path, arrow_clean, mode="error")
        else:
            dt = DeltaTable(silver_path)
            (
                dt.merge(
                    source=arrow_clean,
                    predicate=SILVER_COMPOSITE_KEY,
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        logger.info("silver_merged", clean=len(clean_records), silver_path=silver_path)

    return len(clean_records), len(quarantined_records)
