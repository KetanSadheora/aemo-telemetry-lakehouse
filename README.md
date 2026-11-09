# AEMO Energy Market Medallion Lakehouse

Production-grade Python pipeline that ingests Australian Energy Market Operator (AEMO) 5-minute DispatchIS reports into a Bronze / Silver / Gold lakehouse. Logic is extracted from the working notebook `Autonomous_Industrial_Energy_Market_&_Telemetry_Lakehous.ipynb` without changing Bronze immutability, Silver composite-key MERGE, or quarantine tagging behaviour.

## Architecture

```
  [ AEMO Dispatch Portal (ZIP/CSV) ]
                 │
                 ▼
  ┌──────────────────────────────┐
  │        BRONZE LAYER          │  ◄── Byte-for-byte raw CSV archive
  │   (data/bronze/**/*.raw.csv) │      SHA-256 audit metadata (sidecar dict)
  └──────────────┬───────────────┘
                 │
                 ▼
  ┌──────────────────────────────┐
  │         SILVER LAYER         │  ◄── Contract validation + granular tags
  │     (Delta Lake MERGE)       │      Upsert on (region_id, interval_utc)
  └──────┬────────────────┬──────┘     _ingested_file / _ingested_sha256
         │                │
         ▼                ▼
  ┌──────────────┐┌──────────────┐
  │  VALID GOLD  ││  QUARANTINE  │  ◄── Non-blocking append of contract failures
  │  (DuckDB     ││ (Delta Lake) │
  │   hourly     │└──────────────┘
  │   mart)      │
  └──────────────┘
```

Prefect wraps discovery, ingest, Silver MERGE, Gold rebuild, and Silver `OPTIMIZE` compaction. Ingest retries three times; Silver and Gold retry twice.

### Package layout

| Path | Role |
| --- | --- |
| `src/config.py` | Pydantic `MarketConfig` (NEM floor/cap, regions, paths) |
| `src/ingestion/aemo_scraper.py` | Manifest watermarking; untouched Bronze bytes |
| `src/quality/contracts.py` | Dispatch schema + granular quarantine codes |
| `src/transformation/silver_merge.py` | Parse PRICE rows; idempotent Delta MERGE |
| `src/transformation/gold_marts.py` | DuckDB hourly regional rollups + compact |
| `src/pipeline.py` | Prefect flow and tasks |

## Local run

Python 3.11+ recommended.

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
set PYTHONPATH=.          # Windows cmd
$env:PYTHONPATH="."       # Windows PowerShell
export PYTHONPATH=.       # macOS / Linux

python -m src.pipeline
```

Optional incremental run from a watermark (`YYYYMMDDHHMM` from the DispatchIS filename):

```python
from src.pipeline import run_lakehouse_pipeline
run_lakehouse_pipeline(watermark_ts="202611031950")
```

Tests (quarantine and Silver writes use pytest `tmp_path` only — never `data/quarantine`):

```bash
pytest
```

CI on every push: Ruff lint + pytest (`.github/workflows/ci.yml`).

## Failure modes

| Failure | Handling |
| --- | --- |
| AEMO portal / ZIP download timeout or HTTP error | Prefect retries on `task_ingest` (3 attempts, 5s delay + jitter) |
| ZIP missing inner CSV or corrupt archive | Exception surfaces; flow fails that file; Bronze is not partially written |
| Bronze CSV has no `D,DISPATCH,PRICE` rows | `ValueError` — Silver task fails and retries |
| Invalid region, missing keys, null RRP, price below floor or above cap | Row tagged (`INVALID_REGION`, `MISSING_REGION_KEY`, `MISSING_INTERVAL_KEY`, `NULL_PRICE`, `BELOW_FLOOR`, `EXCEEDS_CAP`) and appended to quarantine Delta; clean rows still MERGE |
| Duplicate / revised dispatch interval | Silver `when_matched_update_all` / `when_not_matched_insert_all` on `(region_id, interval_utc)` — row count does not grow |
| Empty manifest | Flow completes with `processed_files: 0`; Gold still rebuilds from existing Silver |
| Gold DuckDB `delta_scan` against missing Silver | Gold task fails (nothing to aggregate) |

## Trade-offs

- **Bronze is raw bytes only.** Comments or headers are never prepended. Lineage lives in the audit dict and in Silver columns `_ingested_file` and `_ingested_sha256`. Replay is possible; Bronze files are not self-describing.
- **Watermark default is “newest file only”.** A first run does not backfill the entire CURRENT directory. Pass `watermark_ts` for incremental catch-up. The watermark is the filename timestamp, not a persisted control table — operators must store `new_watermark` themselves (Prefect result, scheduler param, or external store).
- **Quarantine is non-blocking.** Bad rows never fail the batch. The cost is that Gold can be complete for valid regions while a poisoned file is silent except for quarantine counts and logs. Alerting should watch `quarantined` counts.
- **Gold is full overwrite, not MERGE.** Hourly marts are cheap to recompute from Silver. This avoids incremental rollup bugs (late data in a prior hour) at the cost of rewriting the Gold table each run.
- **AEST → UTC uses `Etc/GMT-10`.** Dispatch settlement timestamps are treated as fixed UTC+10, not `Australia/Sydney` (no DST). That matches NEM market time and the original notebook.
- **DuckDB + Delta on local disk.** Suitable for a laptop / single-node lakehouse. Object-storage ACID still comes from Delta; this repo does not provision S3/ADLS or Unity Catalog.
- **pandas is required** even though the core stack is Arrow/Delta: AEMO datetime localisation and the original DataFrame → Arrow write path are preserved.

## Silver schema (clean rows)

`region_id`, `interval_utc`, `raw_settlement_date`, `intervention_flag`, `rrp_price_aud_per_mwh`, `eep_price_aud_per_mwh`, `_ingested_file`, `_ingested_sha256`, `_silver_processed_at_utc`.
