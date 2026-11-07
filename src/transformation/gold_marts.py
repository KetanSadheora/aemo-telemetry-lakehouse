"""DuckDB hourly regional rollups and Silver file compaction."""

from __future__ import annotations

import duckdb
import pyarrow as pa
import structlog
from deltalake import DeltaTable, write_deltalake

from src.config import CONFIG, MarketConfig

logger = structlog.get_logger()

HOURLY_REGIONAL_ROLLUP_SQL = """
    SELECT
        date_trunc('hour', interval_utc) AS hour_interval_utc,
        region_id,
        count(*) AS interval_count,
        round(avg(rrp_price_aud_per_mwh), 2) AS avg_rrp_aud_per_mwh,
        round(min(rrp_price_aud_per_mwh), 2) AS min_rrp_aud_per_mwh,
        round(max(rrp_price_aud_per_mwh), 2) AS max_rrp_aud_per_mwh,
        max(intervention_flag) AS had_intervention,
        current_timestamp AS _gold_processed_at_utc
    FROM delta_scan('{silver_path}')
    GROUP BY 1, 2
    ORDER BY 1 DESC, 2 ASC
"""


def process_silver_to_gold(
    silver_path: str | None = None,
    gold_path: str | None = None,
    config: MarketConfig = CONFIG,
) -> int:
    """Rebuild the hourly regional price mart from Silver via DuckDB."""
    silver_path = silver_path or config.silver_path
    gold_path = gold_path or config.gold_path

    con = duckdb.connect()
    query = HOURLY_REGIONAL_ROLLUP_SQL.format(silver_path=silver_path)
    gold_df = con.execute(query).df()
    arrow_gold = pa.Table.from_pandas(gold_df)
    write_deltalake(gold_path, arrow_gold, mode="overwrite")
    logger.info("gold_refreshed", rows=len(gold_df), gold_path=gold_path)
    return len(gold_df)


def optimize_silver(silver_path: str | None = None, config: MarketConfig = CONFIG):
    """Compact small Parquet files in the Silver Delta table (FinOps)."""
    silver_path = silver_path or config.silver_path
    dt = DeltaTable(silver_path)
    result = dt.optimize.compact()
    logger.info("silver_optimized", silver_path=silver_path)
    return result
