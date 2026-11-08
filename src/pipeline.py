"""Prefect orchestration for the AEMO medallion lakehouse."""

from __future__ import annotations

import os
os.environ.setdefault("PREFECT_INSTRUMENTATION_OPENTELEMETRY_ENABLED", "False")

import structlog
from prefect import flow, task

from src.config import CONFIG
from src.ingestion.aemo_scraper import get_aemo_file_manifest, ingest_raw_bronze
from src.transformation.gold_marts import optimize_silver, process_silver_to_gold
from src.transformation.silver_merge import process_bronze_to_silver

logger = structlog.get_logger()


@task(retries=3, retry_delay_seconds=5, retry_jitter_factor=0.2)
def task_ingest(filename: str) -> dict:
    return ingest_raw_bronze(filename)


@task(retries=2, retry_delay_seconds=5)
def task_silver(audit_meta: dict) -> tuple[int, int]:
    return process_bronze_to_silver(audit_meta["bronze_path"], audit_meta)


@task(retries=2, retry_delay_seconds=5)
def task_gold() -> int:
    return process_silver_to_gold()


@task
def task_finops():
    return optimize_silver()


@flow(name="aemo_medallion_lakehouse_flow", retries=1, retry_delay_seconds=30)
def run_lakehouse_pipeline(watermark_ts: str | None = None) -> dict:
    """Discover DispatchIS files past the watermark and run Bronze → Silver → Gold."""
    CONFIG.ensure_directories()

    files_to_process = get_aemo_file_manifest(watermark_ts)
    logger.info("manifest_resolved", file_count=len(files_to_process), watermark_ts=watermark_ts)

    last_watermark = watermark_ts
    for fname in files_to_process:
        meta = task_ingest(fname)
        clean_cnt, quar_cnt = task_silver(meta)
        logger.info(
            "file_processed",
            filename=fname,
            clean=clean_cnt,
            quarantined=quar_cnt,
        )
        last_watermark = meta["file_timestamp"]

    gold_cnt = task_gold()
    task_finops()

    return {
        "processed_files": len(files_to_process),
        "new_watermark": last_watermark,
        "gold_rows": gold_cnt,
    }


if __name__ == "__main__":
    result = run_lakehouse_pipeline()
    print("Pipeline Run:", result)
