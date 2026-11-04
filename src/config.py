"""Declarative NEM market bounds and lakehouse path configuration."""

from __future__ import annotations

import os

from pydantic import BaseModel, Field, model_validator


class MarketConfig(BaseModel):
    """Parameterized NEM bounds and local lakehouse locations.

    Regulatory price limits are indexed annually; keep them here rather than
    inlined in ingestion or validation code.
    """

    market_price_floor_aud: float = Field(
        default=-1000.00,
        description="NEM market price floor (AUD/MWh)",
    )
    market_price_cap_aud: float = Field(
        default=16600.00,
        description="NEM market price cap (AUD/MWh)",
    )
    valid_regions: set[str] = Field(
        default_factory=lambda: {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"},
        description="Valid NEM regional reference nodes",
    )
    aemo_dispatch_url: str = Field(
        default="https://www.nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/",
        description="AEMO CURRENT DispatchIS report directory",
    )
    bronze_dir: str = "data/bronze"
    silver_path: str = "data/silver/dispatch_price"
    gold_path: str = "data/gold/fact_hourly_regional_prices"
    quarantine_path: str = "data/quarantine/dispatch_price"
    user_agent: str = "Mozilla/5.0"

    @model_validator(mode="after")
    def floor_must_be_below_cap(self) -> MarketConfig:
        if self.market_price_floor_aud >= self.market_price_cap_aud:
            raise ValueError("market_price_floor_aud must be below market_price_cap_aud")
        return self

    def ensure_directories(self) -> None:
        for folder in [
            self.bronze_dir,
            os.path.dirname(self.silver_path),
            os.path.dirname(self.gold_path),
            os.path.dirname(self.quarantine_path),
        ]:
            os.makedirs(folder, exist_ok=True)


CONFIG = MarketConfig()
