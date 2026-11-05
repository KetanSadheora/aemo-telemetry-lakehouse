"""Untouched raw CSV Bronze ingestion, watermarking, and manifest fetch."""

from __future__ import annotations

import hashlib
import io
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests
import structlog

from src.config import CONFIG, MarketConfig

logger = structlog.get_logger()


def get_aemo_file_manifest(
    watermark_ts: str | None = None,
    config: MarketConfig = CONFIG,
) -> list[str]:
    """Scan the AEMO portal and return DispatchIS ZIP names newer than the watermark.

    If ``watermark_ts`` is omitted, only the newest file is returned so a first
    run does not backfill the entire CURRENT directory.
    """
    headers = {"User-Agent": config.user_agent}
    resp = requests.get(config.aemo_dispatch_url, headers=headers, timeout=20)
    resp.raise_for_status()

    matches = re.findall(
        r"(PUBLIC_DISPATCHIS_(\d{12})_\d+\.zip)",
        resp.text,
        re.IGNORECASE,
    )
    unique_files = sorted(
        list({m[0]: m[1] for m in matches}.items()),
        key=lambda x: x[1],
    )

    if not watermark_ts:
        return [unique_files[-1][0]] if unique_files else []

    return [filename for filename, ts in unique_files if ts > watermark_ts]


def ingest_raw_bronze(
    filename: str,
    target_dir: str | None = None,
    config: MarketConfig = CONFIG,
) -> dict:
    """Download a DispatchIS ZIP and persist the inner CSV as untouched bytes.

    The Bronze object is a byte-for-byte copy of the extracted CSV. Audit
    metadata (SHA-256, source URL, timestamps) is returned separately and is
    never prepended to the raw file.
    """
    target_dir = target_dir or config.bronze_dir
    file_url = f"{config.aemo_dispatch_url}{filename}"
    resp = requests.get(
        file_url,
        headers={"User-Agent": config.user_agent},
        timeout=30,
    )
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = z.namelist()[0]
        raw_bytes = z.read(csv_name)

    sha256_hash = hashlib.sha256(raw_bytes).hexdigest()

    now_utc = datetime.now(timezone.utc)
    out_dir = Path(target_dir) / now_utc.strftime("year=%Y/month=%m/day=%d")
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_path = out_dir / filename.replace(".zip", ".raw.csv")

    with open(raw_path, "wb") as f:
        f.write(raw_bytes)

    logger.info(
        "bronze_ingested",
        bronze_path=str(raw_path),
        sha256_hash=sha256_hash,
        filename=filename,
    )

    return {
        "bronze_path": str(raw_path),
        "source_url": file_url,
        "sha256_hash": sha256_hash,
        "ingested_at_utc": now_utc.isoformat(),
        "file_timestamp": filename.split("_")[2],
    }
