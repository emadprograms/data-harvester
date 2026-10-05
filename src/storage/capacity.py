"""
Capacity monitoring, partition fan-out metrics, and threshold alerting for Tick Lake.
Milestone 4.3 - Package E (CAPA-01).
"""
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from src.storage.config import decode_symbol, resolve_tick_lake_root


# Default threshold constants
DEFAULT_WARNING_FILES_PER_PARTITION = 5
DEFAULT_CRITICAL_FILES_PER_PARTITION = 20
DEFAULT_WARNING_SMALL_FILE_RATIO = 0.30
DEFAULT_CRITICAL_SMALL_FILE_RATIO = 0.70
DEFAULT_WARNING_DISK_HEADROOM_BYTES = 10 * 1024 * 1024 * 1024   # 10 GiB
DEFAULT_CRITICAL_DISK_HEADROOM_BYTES = 2 * 1024 * 1024 * 1024    # 2 GiB
DEFAULT_RATE_LIMIT_SECONDS = 60.0
CAPACITY_STATUS_FILENAME = "capacity_status.json"

# Size bucket thresholds
SIZE_64KB = 64 * 1024
SIZE_1MB = 1024 * 1024


@dataclass
class CapacityAlert:
    level: str  # "WARNING" | "CRITICAL"
    metric: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PartitionMetrics:
    symbol: str
    date: str
    file_count: int
    total_bytes: int
    small_files_lt_64kb: int
    small_files_64kb_to_1mb: int
    large_files_gte_1mb: int
    status: str = "HEALTHY"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class CapacityMonitor:
    """
    Monitors Tick Lake partition capacity, small-file distributions, control artifact
    growth, free disk space, and partition discovery latency.
    """

    def __init__(
        self,
        lake_root: Optional[Union[str, Path]] = None,
        warning_files_per_partition: int = DEFAULT_WARNING_FILES_PER_PARTITION,
        critical_files_per_partition: int = DEFAULT_CRITICAL_FILES_PER_PARTITION,
        warning_small_file_ratio: float = DEFAULT_WARNING_SMALL_FILE_RATIO,
        critical_small_file_ratio: float = DEFAULT_CRITICAL_SMALL_FILE_RATIO,
        warning_disk_headroom_bytes: int = DEFAULT_WARNING_DISK_HEADROOM_BYTES,
        critical_disk_headroom_bytes: int = DEFAULT_CRITICAL_DISK_HEADROOM_BYTES,
        disk_usage_fn: Optional[Callable[[Path], Any]] = None,
        rate_limit_seconds: float = DEFAULT_RATE_LIMIT_SECONDS,
    ):
        self.root = resolve_tick_lake_root(lake_root)
        self.warning_files_per_partition = int(warning_files_per_partition)
        self.critical_files_per_partition = int(critical_files_per_partition)
        self.warning_small_file_ratio = float(warning_small_file_ratio)
        self.critical_small_file_ratio = float(critical_small_file_ratio)
        self.warning_disk_headroom_bytes = int(warning_disk_headroom_bytes)
        self.critical_disk_headroom_bytes = int(critical_disk_headroom_bytes)
        self.disk_usage_fn = disk_usage_fn or shutil.disk_usage
        self.rate_limit_seconds = float(rate_limit_seconds)
        self.control_dir = self.root / "_control"
        self.status_file = self.control_dir / CAPACITY_STATUS_FILENAME

    def scan(self) -> Dict[str, Any]:
        """
        Executes a fresh capacity measurement across lake hierarchy and control directories.
        """
        t0 = time.perf_counter()

        ticks_dir = self.root / "ticks"
        partitions: Dict[str, Dict[str, Any]] = {}
        total_parquet_files = 0
        total_parquet_bytes = 0
        small_files_lt_64kb = 0
        small_files_64kb_to_1mb = 0
        large_files_gte_1mb = 0

        # Discover partitions and measure files/day by symbol and UTC date
        if ticks_dir.is_dir():
            try:
                sym_entries = [p for p in ticks_dir.iterdir() if p.is_dir() and p.name.startswith("symbol=")]
            except OSError:
                sym_entries = []

            for sym_dir in sorted(sym_entries, key=lambda p: p.name):
                encoded_sym = sym_dir.name[len("symbol="):]
                symbol = decode_symbol(encoded_sym)
                try:
                    date_entries = [p for p in sym_dir.iterdir() if p.is_dir() and p.name.startswith("date=")]
                except OSError:
                    date_entries = []

                for date_dir in sorted(date_entries, key=lambda p: p.name):
                    dt_str = date_dir.name[len("date="):]
                    try:
                        files = [
                            f for f in date_dir.iterdir()
                            if f.is_file() and f.name.endswith(".parquet") and not f.name.startswith((".", "tmp_"))
                        ]
                    except OSError:
                        files = []

                    part_file_count = len(files)
                    part_bytes = 0
                    part_lt_64kb = 0
                    part_64kb_1mb = 0
                    part_gte_1mb = 0

                    for f in files:
                        try:
                            sz = f.stat().st_size
                        except OSError:
                            continue
                        part_bytes += sz
                        if sz < SIZE_64KB:
                            part_lt_64kb += 1
                        elif sz < SIZE_1MB:
                            part_64kb_1mb += 1
                        else:
                            part_gte_1mb += 1

                    part_status = "HEALTHY"
                    if part_file_count >= self.critical_files_per_partition:
                        part_status = "CRITICAL"
                    elif part_file_count >= self.warning_files_per_partition:
                        part_status = "WARNING"

                    key = f"{symbol}/{dt_str}"
                    part_metrics = PartitionMetrics(
                        symbol=symbol,
                        date=dt_str,
                        file_count=part_file_count,
                        total_bytes=part_bytes,
                        small_files_lt_64kb=part_lt_64kb,
                        small_files_64kb_to_1mb=part_64kb_1mb,
                        large_files_gte_1mb=part_gte_1mb,
                        status=part_status,
                    )
                    partitions[key] = part_metrics.to_dict()

                    total_parquet_files += part_file_count
                    total_parquet_bytes += part_bytes
                    small_files_lt_64kb += part_lt_64kb
                    small_files_64kb_to_1mb += part_64kb_1mb
                    large_files_gte_1mb += part_gte_1mb

        discovery_latency_ms = round((time.perf_counter() - t0) * 1000.0, 3)
        small_files_lt_1mb = small_files_lt_64kb + small_files_64kb_to_1mb
        small_file_ratio = (
            round(small_files_lt_1mb / total_parquet_files, 4) if total_parquet_files > 0 else 0.0
        )

        # Measure receipts count & growth
        receipts_dir = self.root / "_control" / "receipts"
        receipt_count = 0
        receipt_bytes = 0
        if receipts_dir.is_dir():
            try:
                for r in receipts_dir.iterdir():
                    if r.is_file() and r.name.endswith(".json"):
                        receipt_count += 1
                        try:
                            receipt_bytes += r.stat().st_size
                        except OSError:
                            pass
            except OSError:
                pass

        # Measure intent count & growth
        intent_dir = self.root / "_control" / "intent"
        intent_count = 0
        intent_bytes = 0
        pending_intent_count = 0
        if intent_dir.is_dir():
            try:
                for it in intent_dir.iterdir():
                    if it.is_file() and it.name.endswith(".json"):
                        intent_count += 1
                        try:
                            intent_bytes += it.stat().st_size
                        except OSError:
                            pass
                        # Check if intent is uncommitted (no matching receipt)
                        matching_receipt = receipts_dir / it.name
                        if not matching_receipt.is_file():
                            pending_intent_count += 1
            except OSError:
                pass

        # Measure free disk space
        try:
            usage = self.disk_usage_fn(self.root)
            disk_total = int(usage.total)
            disk_used = int(usage.used)
            disk_free = int(usage.free)
        except Exception:
            disk_total = 0
            disk_used = 0
            disk_free = 0

        disk_free_percent = round((disk_free / disk_total * 100.0), 2) if disk_total > 0 else 0.0

        # Evaluate threshold alerts
        alerts: List[Dict[str, Any]] = []
        recommendations: List[str] = []

        # 1. Partition file count thresholds
        critical_partitions = [k for k, p in partitions.items() if p["status"] == "CRITICAL"]
        warning_partitions = [k for k, p in partitions.items() if p["status"] == "WARNING"]

        if critical_partitions:
            alerts.append(CapacityAlert(
                level="CRITICAL",
                metric="partition_file_count",
                message=f"{len(critical_partitions)} partitions exceed critical threshold (>{self.critical_files_per_partition} files/partition)",
                details={"partitions": critical_partitions[:10], "total_critical": len(critical_partitions)},
            ).to_dict())
            recommendations.append(
                f"Run offline compaction immediately on {len(critical_partitions)} fragmented partitions to eliminate glob overhead."
            )
        elif warning_partitions:
            alerts.append(CapacityAlert(
                level="WARNING",
                metric="partition_file_count",
                message=f"{len(warning_partitions)} partitions exceed warning threshold (>{self.warning_files_per_partition} files/partition)",
                details={"partitions": warning_partitions[:10], "total_warning": len(warning_partitions)},
            ).to_dict())
            recommendations.append(
                f"Schedule offline compaction on {len(warning_partitions)} partitions approaching fan-out limits."
            )

        # 2. Small file ratio threshold
        if total_parquet_files > 0:
            if small_file_ratio >= self.critical_small_file_ratio:
                alerts.append(CapacityAlert(
                    level="CRITICAL",
                    metric="small_file_ratio",
                    message=f"Small-file ratio ({small_file_ratio * 100:.1f}%) exceeds critical threshold ({self.critical_small_file_ratio * 100:.1f}%)",
                    details={"small_files_lt_1mb": small_files_lt_1mb, "total_files": total_parquet_files, "ratio": small_file_ratio},
                ).to_dict())
                recommendations.append(
                    "High small-file ratio (<1MB). Consolidate partitions to reduce metadata sync latency and writer CPU fan-out."
                )
            elif small_file_ratio >= self.warning_small_file_ratio:
                alerts.append(CapacityAlert(
                    level="WARNING",
                    metric="small_file_ratio",
                    message=f"Small-file ratio ({small_file_ratio * 100:.1f}%) exceeds warning threshold ({self.warning_small_file_ratio * 100:.1f}%)",
                    details={"small_files_lt_1mb": small_files_lt_1mb, "total_files": total_parquet_files, "ratio": small_file_ratio},
                ).to_dict())
                recommendations.append(
                    "Moderate small-file ratio. Plan partition compaction during next scheduled maintenance window."
                )

        # 3. Disk headroom thresholds
        if disk_free < self.critical_disk_headroom_bytes:
            alerts.append(CapacityAlert(
                level="CRITICAL",
                metric="disk_headroom",
                message=f"Free disk space ({disk_free / (1024**3):.2f} GiB) is below critical headroom ({self.critical_disk_headroom_bytes / (1024**3):.2f} GiB)",
                details={"free_bytes": disk_free, "required_bytes": self.critical_disk_headroom_bytes},
            ).to_dict())
            recommendations.append(
                "Disk headroom critically low. Physically purge pending symbols, archive completed milestones, or expand volume."
            )
        elif disk_free < self.warning_disk_headroom_bytes:
            alerts.append(CapacityAlert(
                level="WARNING",
                metric="disk_headroom",
                message=f"Free disk space ({disk_free / (1024**3):.2f} GiB) is below warning headroom ({self.warning_disk_headroom_bytes / (1024**3):.2f} GiB)",
                details={"free_bytes": disk_free, "required_bytes": self.warning_disk_headroom_bytes},
            ).to_dict())
            recommendations.append(
                "Disk headroom warning. Monitor growth and consider running compaction/purge."
            )

        # 4. Pending intents warning
        if pending_intent_count > 0:
            alerts.append(CapacityAlert(
                level="WARNING",
                metric="pending_intents",
                message=f"{pending_intent_count} uncommitted intents found in _control/intent/",
                details={"pending_count": pending_intent_count},
            ).to_dict())
            recommendations.append(
                "Uncommitted intents detected. Run publication recovery to reconcile staged publications."
            )

        # Determine overall lake status
        overall_status = "HEALTHY"
        if any(a["level"] == "CRITICAL" for a in alerts):
            overall_status = "CRITICAL"
        elif any(a["level"] == "WARNING" for a in alerts):
            overall_status = "WARNING"

        now_iso = datetime.now(timezone.utc).isoformat()
        report: Dict[str, Any] = {
            "timestamp": now_iso,
            "lake_root": str(self.root),
            "status": overall_status,
            "total_partitions": len(partitions),
            "total_parquet_files": total_parquet_files,
            "total_parquet_bytes": total_parquet_bytes,
            "small_files_lt_64kb": small_files_lt_64kb,
            "small_files_64kb_to_1mb": small_files_64kb_to_1mb,
            "small_files_lt_1mb": small_files_lt_1mb,
            "large_files_gte_1mb": large_files_gte_1mb,
            "small_file_ratio": small_file_ratio,
            "receipt_count": receipt_count,
            "receipt_bytes": receipt_bytes,
            "intent_count": intent_count,
            "intent_bytes": intent_bytes,
            "pending_intent_count": pending_intent_count,
            "disk_total_bytes": disk_total,
            "disk_used_bytes": disk_used,
            "disk_free_bytes": disk_free,
            "disk_free_percent": disk_free_percent,
            "discovery_latency_ms": discovery_latency_ms,
            "partitions": partitions,
            "alerts": alerts,
            "recommendations": recommendations,
        }
        return report

    def save_status(self, report: Dict[str, Any], force: bool = False) -> Path:
        """
        Atomically saves capacity report to <lake_root>/_control/capacity_status.json
        respecting rate limits unless force=True.
        """
        if not force and self.status_file.is_file():
            try:
                mtime = self.status_file.stat().st_mtime
                if time.time() - mtime < self.rate_limit_seconds:
                    return self.status_file
            except OSError:
                pass

        self.control_dir.mkdir(parents=True, exist_ok=True)
        tmp_name = f"tmp_capacity_{os.getpid()}_{int(time.time()*1000)}.json"
        tmp_path = self.control_dir / tmp_name

        payload = json.dumps(report, indent=2)
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, self.status_file)
        try:
            dir_fd = os.open(str(self.control_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

        return self.status_file

    def get_report(self, force_save: bool = True, force_scan: bool = False) -> Dict[str, Any]:
        """
        Returns capacity report, reading cached status if within rate limit and not forced,
        or executing a scan and saving status.
        """
        if not force_scan and self.status_file.is_file():
            try:
                mtime = self.status_file.stat().st_mtime
                if time.time() - mtime < self.rate_limit_seconds:
                    with open(self.status_file, "r", encoding="utf-8") as f:
                        cached = json.load(f)
                    if isinstance(cached, dict) and "status" in cached:
                        return cached
            except Exception:
                pass

        report = self.scan()
        if force_save:
            self.save_status(report, force=force_scan)
        return report


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Tick Lake Capacity Monitor (CAPA-01)")
    parser.add_argument("--lake-root", type=str, default=None, help="Root path of the tick lake")
    parser.add_argument("--json", action="store_true", help="Output report as formatted JSON")
    parser.add_argument("--force", action="store_true", help="Force fresh scan ignoring rate limits")
    parser.add_argument("--strict", action="store_true", help="Exit with non-zero code if status is WARNING or CRITICAL")
    args = parser.parse_args()

    try:
        monitor = CapacityMonitor(lake_root=args.lake_root)
        report = monitor.get_report(force_save=True, force_scan=args.force)

        if args.json:
            print(json.dumps(report, indent=2))
        else:
            status = report["status"]
            color = "\033[92m" if status == "HEALTHY" else ("\033[93m" if status == "WARNING" else "\033[91m")
            reset = "\033[0m"
            print("=" * 70)
            print(f" TICK LAKE CAPACITY REPORT: {color}{status}{reset}")
            print("=" * 70)
            print(f"Lake Root:             {report['lake_root']}")
            print(f"Total Partitions:      {report['total_partitions']}")
            print(f"Total Parquet Files:   {report['total_parquet_files']} ({report['total_parquet_bytes'] / (1024*1024):.2f} MB)")
            print(f"Small Files (<64KB):   {report['small_files_lt_64kb']}")
            print(f"Small Files (<1MB):    {report['small_files_lt_1mb']}")
            print(f"Small-File Ratio:      {report['small_file_ratio'] * 100:.1f}%")
            print(f"Receipts / Intents:    {report['receipt_count']} receipts ({report['receipt_bytes'] / 1024:.1f} KB), {report['intent_count']} intents")
            print(f"Pending Intents:       {report['pending_intent_count']}")
            print(f"Disk Free Space:       {report['disk_free_bytes'] / (1024**3):.2f} GiB ({report['disk_free_percent']}%)")
            print(f"Discovery Latency:     {report['discovery_latency_ms']:.2f} ms")

            if report["alerts"]:
                print("-" * 70)
                print(" ALERTS:")
                for alert in report["alerts"]:
                    lvl = alert["level"]
                    lvl_col = "\033[91m" if lvl == "CRITICAL" else "\033[93m"
                    print(f"  [{lvl_col}{lvl}{reset}] {alert['message']}")

            if report["recommendations"]:
                print("-" * 70)
                print(" RECOMMENDATIONS:")
                for rec in report["recommendations"]:
                    print(f"  * {rec}")
            print("=" * 70)

        if args.strict:
            if report["status"] == "CRITICAL":
                sys.exit(2)
            elif report["status"] == "WARNING":
                sys.exit(1)
        sys.exit(0)
    except Exception as exc:
        print(f"Capacity monitoring failed: {exc}", file=sys.stderr)
        sys.exit(3)


if __name__ == "__main__":
    main()
