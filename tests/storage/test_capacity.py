"""
Tests for capacity monitoring, partition fan-out metrics, and threshold alerting (CAPA-01).
"""
from collections import namedtuple
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.storage.capacity import (
    CapacityMonitor,
    DEFAULT_WARNING_FILES_PER_PARTITION,
    DEFAULT_CRITICAL_FILES_PER_PARTITION,
)
from src.storage.config import init_tick_lake
from src.storage.publication import LakePublisher
from src.storage.reader import TickLakeReader
from src.storage.schema import LAKE_SCHEMA_V1, ticks_to_table
from tests.fixtures.deterministic_quotes import QuoteTick


DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


def _create_sample_ticks(symbol: str, date_str: str, count: int = 10, start_idx: int = 0) -> list:
    base_dt = datetime.fromisoformat(f"{date_str}T10:00:00+00:00")
    ticks = []
    for i in range(count):
        idx = start_idx + i
        ticks.append(QuoteTick(
            timestamp=datetime(base_dt.year, base_dt.month, base_dt.day, 10, 0, idx % 60, (idx * 1000) % 1_000_000, tzinfo=timezone.utc),
            symbol=symbol,
            price=150.0 + (idx * 0.05),
            volume=100.0,
            bid=149.95,
            ask=150.05,
            source="CAPITAL",
            session="REG",
            ingest_id=f"{symbol}_{date_str}_{idx:06d}",
        ))
    return ticks


def test_capacity_monitor_empty_lake(tmp_path):
    """An empty lake reports HEALTHY with zero partitions and files."""
    lake_root = tmp_path / "empty_lake"
    init_tick_lake(lake_root)

    monitor = CapacityMonitor(lake_root)
    report = monitor.get_report(force_save=True, force_scan=True)

    assert report["status"] == "HEALTHY"
    assert report["total_partitions"] == 0
    assert report["total_parquet_files"] == 0
    assert report["total_parquet_bytes"] == 0
    assert report["small_file_ratio"] == 0.0
    assert report["receipt_count"] == 0
    assert report["intent_count"] == 0
    assert report["alerts"] == []
    assert monitor.status_file.is_file()


def test_capacity_small_file_metrics_and_buckets(tmp_path):
    """Verify classification into <64KB, 64KB-1MB, and >=1MB buckets."""
    lake_root = tmp_path / "lake_buckets"
    init_tick_lake(lake_root)
    publisher = LakePublisher(lake_root, writer_id="w1")

    # Publish small files (<64KB)
    for i in range(3):
        ticks = _create_sample_ticks("AAPL", "2026-10-02", count=5, start_idx=i*5)
        publisher.publish_batch(ticks, batch_id=f"batch_aapl_{i}", sequence=i+1)

    publisher.close()

    monitor = CapacityMonitor(lake_root, warning_files_per_partition=10)
    report = monitor.get_report(force_save=True, force_scan=True)

    assert report["total_partitions"] == 1
    assert report["total_parquet_files"] == 3
    assert report["small_files_lt_64kb"] == 3
    assert report["small_files_lt_1mb"] == 3
    assert report["large_files_gte_1mb"] == 0
    assert report["small_file_ratio"] == 1.0

    # Status file written
    assert monitor.status_file.is_file()
    with open(monitor.status_file, "r", encoding="utf-8") as f:
        saved = json.load(f)
    assert saved["total_parquet_files"] == 3


def test_capacity_threshold_alerts_partition_file_counts(tmp_path):
    """Test warning and critical alerts for file counts per partition."""
    lake_root = tmp_path / "lake_thresholds"
    init_tick_lake(lake_root)
    publisher = LakePublisher(lake_root, writer_id="w1")

    # Publish 6 batches for MSFT (exceeding warning threshold = 5)
    for i in range(6):
        ticks = _create_sample_ticks("MSFT", "2026-10-02", count=2, start_idx=i*2)
        publisher.publish_batch(ticks, batch_id=f"batch_msft_{i}", sequence=i+1)

    publisher.close()

    # Warning alert (>5 files)
    monitor_warn = CapacityMonitor(
        lake_root,
        warning_files_per_partition=5,
        critical_files_per_partition=20,
        critical_small_file_ratio=1.1,
        warning_small_file_ratio=1.1,
    )
    report_warn = monitor_warn.get_report(force_save=True, force_scan=True)

    assert report_warn["status"] == "WARNING"
    warn_alerts = [a for a in report_warn["alerts"] if a["metric"] == "partition_file_count"]
    assert len(warn_alerts) == 1
    assert warn_alerts[0]["level"] == "WARNING"
    assert "exceed warning threshold" in warn_alerts[0]["message"]
    assert any("Schedule offline compaction" in r for r in report_warn["recommendations"])

    # Critical alert (set critical=5)
    monitor_crit = CapacityMonitor(
        lake_root,
        warning_files_per_partition=3,
        critical_files_per_partition=5,
        critical_small_file_ratio=1.1,
        warning_small_file_ratio=1.1,
    )
    report_crit = monitor_crit.get_report(force_save=True, force_scan=True)

    assert report_crit["status"] == "CRITICAL"
    crit_alerts = [a for a in report_crit["alerts"] if a["metric"] == "partition_file_count"]
    assert len(crit_alerts) == 1
    assert crit_alerts[0]["level"] == "CRITICAL"
    assert "exceed critical threshold" in crit_alerts[0]["message"]
    assert any("Run offline compaction immediately" in r for r in report_crit["recommendations"])


def test_capacity_disk_headroom_injected_stats(tmp_path):
    """Test warning and critical alerts for disk space using injected disk stats."""
    lake_root = tmp_path / "lake_disk"
    init_tick_lake(lake_root)

    # 1. Injected critical disk space: 1 GB free (below 2 GB critical)
    mock_disk_critical = lambda path: DiskUsage(total=100 * 1024**3, used=99 * 1024**3, free=1 * 1024**3)
    monitor_crit = CapacityMonitor(lake_root, disk_usage_fn=mock_disk_critical)
    report_crit = monitor_crit.get_report(force_save=True, force_scan=True)

    assert report_crit["status"] == "CRITICAL"
    disk_alerts = [a for a in report_crit["alerts"] if a["metric"] == "disk_headroom"]
    assert len(disk_alerts) == 1
    assert disk_alerts[0]["level"] == "CRITICAL"
    assert any("critically low" in r for r in report_crit["recommendations"])

    # 2. Injected warning disk space: 5 GB free (below 10 GB warning, above 2 GB critical)
    mock_disk_warning = lambda path: DiskUsage(total=100 * 1024**3, used=95 * 1024**3, free=5 * 1024**3)
    monitor_warn = CapacityMonitor(lake_root, disk_usage_fn=mock_disk_warning)
    report_warn = monitor_warn.get_report(force_save=True, force_scan=True)

    assert report_warn["status"] == "WARNING"
    disk_warn_alerts = [a for a in report_warn["alerts"] if a["metric"] == "disk_headroom"]
    assert len(disk_warn_alerts) == 1
    assert disk_warn_alerts[0]["level"] == "WARNING"

    # 3. Healthy disk space: 50 GB free
    mock_disk_healthy = lambda path: DiskUsage(total=100 * 1024**3, used=50 * 1024**3, free=50 * 1024**3)
    monitor_ok = CapacityMonitor(lake_root, disk_usage_fn=mock_disk_healthy)
    report_ok = monitor_ok.get_report(force_save=True, force_scan=True)

    assert report_ok["status"] == "HEALTHY"
    assert not any(a["metric"] == "disk_headroom" for a in report_ok["alerts"])


def test_capacity_rate_limiting(tmp_path):
    """Subsequent get_report calls within rate limit interval do not rescan or overwrite unless forced."""
    lake_root = tmp_path / "lake_rate_limit"
    init_tick_lake(lake_root)

    monitor = CapacityMonitor(lake_root, rate_limit_seconds=60.0)
    report1 = monitor.get_report(force_save=True, force_scan=True)
    mtime1 = monitor.status_file.stat().st_mtime

    # Call again immediately without force
    report2 = monitor.get_report(force_save=True, force_scan=False)
    mtime2 = monitor.status_file.stat().st_mtime

    assert report1["timestamp"] == report2["timestamp"]
    assert mtime1 == mtime2

    # Call with force_scan=True
    time.sleep(0.01)
    report3 = monitor.get_report(force_save=True, force_scan=True)
    mtime3 = monitor.status_file.stat().st_mtime

    assert mtime3 >= mtime1


def test_capacity_reader_integration(tmp_path):
    """TickLakeReader.get_capacity_report() successfully surfaces capacity metrics."""
    lake_root = tmp_path / "lake_reader_capa"
    init_tick_lake(lake_root)
    publisher = LakePublisher(lake_root, writer_id="w1")

    ticks = _create_sample_ticks("NVDA", "2026-10-02", count=20)
    publisher.publish_batch(ticks, batch_id="batch_nvda_1", sequence=1)
    publisher.close()

    reader = TickLakeReader(root=lake_root)
    report = reader.get_capacity_report(force=True)

    assert report["status"] in ("HEALTHY", "WARNING", "CRITICAL")
    assert report["total_partitions"] == 1
    assert "NVDA/2026-10-02" in report["partitions"]
    assert report["partitions"]["NVDA/2026-10-02"]["file_count"] == 1


def test_capacity_cli_execution(tmp_path):
    """CLI python -m src.storage.capacity executes and outputs human and JSON reports."""
    lake_root = tmp_path / "lake_cli"
    init_tick_lake(lake_root)

    # Human-readable output
    res_human = subprocess.run(
        [sys.executable, "-m", "src.storage.capacity", "--lake-root", str(lake_root), "--force"],
        capture_output=True,
        text=True,
    )
    assert res_human.returncode == 0
    assert "TICK LAKE CAPACITY REPORT" in res_human.stdout
    assert str(lake_root) in res_human.stdout

    # JSON output
    res_json = subprocess.run(
        [sys.executable, "-m", "src.storage.capacity", "--lake-root", str(lake_root), "--json", "--force"],
        capture_output=True,
        text=True,
    )
    assert res_json.returncode == 0
    parsed = json.loads(res_json.stdout)
    assert parsed["status"] == "HEALTHY"
    assert parsed["total_partitions"] == 0
