"""Pytest verifying that merging the same batch twice yields the same row count."""

from __future__ import annotations

from deltalake import DeltaTable

from src.transformation.silver_merge import process_bronze_to_silver


def test_silver_merge_idempotency(tmp_path):
    """Running the exact same batch twice MUST yield identical row counts in Delta."""
    test_silver = str(tmp_path / "test_silver")
    test_quarantine = str(tmp_path / "quarantine" / "dispatch_price")
    test_meta = {"sha256_hash": "mock_hash"}

    csv_content = (
        "C,NEM,DISPATCH,PRICE,1\n"
        'D,DISPATCH,PRICE,1,"2026/11/03 19:00:00",1,NSW1,1,0,120.50,0.0\n'
        'D,DISPATCH,PRICE,1,"2026/11/03 19:00:00",1,VIC1,1,0,140.20,0.0\n'
    )
    raw_file = tmp_path / "mock.raw.csv"
    raw_file.write_text(csv_content)

    clean1, quar1 = process_bronze_to_silver(
        str(raw_file),
        test_meta,
        silver_path=test_silver,
        quarantine_path=test_quarantine,
    )
    dt = DeltaTable(test_silver)
    count_after_first = dt.to_pyarrow_table().num_rows

    clean2, quar2 = process_bronze_to_silver(
        str(raw_file),
        test_meta,
        silver_path=test_silver,
        quarantine_path=test_quarantine,
    )
    dt_reloaded = DeltaTable(test_silver)
    count_after_second = dt_reloaded.to_pyarrow_table().num_rows
    silver_table = dt_reloaded.to_pyarrow_table()

    assert clean1 == 2
    assert quar1 == 0
    assert clean2 == 2
    assert quar2 == 0
    assert count_after_first == 2
    assert count_after_second == 2
    assert "_ingested_file" in silver_table.column_names
    assert "_ingested_sha256" in silver_table.column_names
    sha_values = silver_table.column("_ingested_sha256").to_pylist()
    assert set(sha_values) == {"mock_hash"}
