"""
Integration Tests for Watermark Manager and Incremental Windows.
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.storage.watermark_manager import WatermarkManager


def test_watermark_checkpoint_and_window(clean_metadata_tables):
    entity = "orders_isolated_test"
    # Cold start: no watermark
    initial_wm = WatermarkManager.get_watermark(entity)
    assert initial_wm is None

    # Calculate window before watermark
    w_start, w_end = WatermarkManager.get_extraction_window(entity)
    assert w_start < w_end

    # Advance watermark
    now_ts = datetime.now(UTC).isoformat()
    WatermarkManager.update_watermark(
        entity_name=entity,
        watermark_value=now_ts,
        batch_id="batch_checkpoint_1",
    )

    # Read back watermark
    updated_wm = WatermarkManager.get_watermark(entity)
    assert updated_wm == now_ts

    # Check that new extraction window incorporates late-arrival grace period
    new_start, new_end = WatermarkManager.get_extraction_window(entity, lookback_hours=12)
    assert new_start < datetime.fromisoformat(now_ts)
