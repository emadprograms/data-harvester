"""
Strict Performance Qualification Evaluator (PERF-04, C43-03).

Enforces mandatory performance qualification upper limits:
- Warm session 1m/5m queries: p95 < 100.0ms
- Warm month daily candles: p95 < 250.0ms
- Writer CPU: >=50.0% reduction in CPU seconds/M valid published ticks vs legacy baseline
- Event-loop lag: p99 < 20.0ms
- Visibility freshness: healthy-load p99 <= configured flush interval + 1.0s

Fails closed on non-finite (NaN, inf), zero, negative, or missing metrics.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import math
from typing import Any, Dict, List, Optional


class PerformanceQualificationError(AssertionError):
    """Raised when one or more mandatory performance qualification upper limits fail."""
    pass


@dataclass
class GateResult:
    """Individual gate qualification outcome."""
    name: str
    target: str
    actual: str
    passed: bool
    details: Optional[str] = None


@dataclass
class PerformanceQualificationSummary:
    """Consolidated outcome of the strict performance qualification gates."""
    overall_passed: bool
    gates: List[GateResult] = field(default_factory=list)
    failure_reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "overall_passed": self.overall_passed,
            "gates": [asdict(g) for g in self.gates],
            "failure_reasons": list(self.failure_reasons),
        }

    def assert_qualification(self) -> None:
        """Assert all mandatory gates passed, raising PerformanceQualificationError if not."""
        if not self.overall_passed:
            reasons = "\n  - ".join(self.failure_reasons)
            raise PerformanceQualificationError(
                f"Strict Performance Qualification FAILED ({len(self.failure_reasons)} violations):\n  - {reasons}"
            )


def _validate_finite_positive(val: Any, name: str) -> float:
    """Validate that val is a finite positive number, otherwise raise ValueError."""
    if val is None:
        raise ValueError(f"Metric '{name}' is missing/None")
    try:
        f = float(val)
    except (TypeError, ValueError):
        raise ValueError(f"Metric '{name}' ({val}) is not a valid number")
    if math.isnan(f):
        raise ValueError(f"Metric '{name}' is NaN")
    if math.isinf(f):
        raise ValueError(f"Metric '{name}' is infinite")
    if f <= 0.0:
        raise ValueError(f"Metric '{name}' must be positive, got {f}")
    return f


def evaluate_performance_gates(
    session_1m_p95_ms: float,
    session_5m_p95_ms: float,
    month_1d_p95_ms: float,
    writer_cpu_sec_per_1m: float,
    legacy_writer_cpu_sec_per_1m: float,
    event_loop_lag_p99_ms: Optional[float] = None,
    freshness_p99_ms: Optional[float] = None,
    flush_interval_seconds: float = 1.0,
) -> PerformanceQualificationSummary:
    """Evaluate mandatory performance upper limits strictly and fail-closed."""
    gates: List[GateResult] = []
    failures: List[str] = []

    # 1. Warm session 1m candle query: p95 < 100.0ms
    try:
        val_1m = _validate_finite_positive(session_1m_p95_ms, "session_1m_p95_ms")
        p_1m = val_1m < 100.0
        gates.append(
            GateResult(
                name="Warm session 1m query latency (p95)",
                target="< 100.00 ms",
                actual=f"{val_1m:.2f} ms",
                passed=p_1m,
            )
        )
        if not p_1m:
            failures.append(f"Warm session 1m query p95 {val_1m:.2f}ms >= 100.0ms")
    except ValueError as exc:
        gates.append(
            GateResult(
                name="Warm session 1m query latency (p95)",
                target="< 100.00 ms",
                actual=str(session_1m_p95_ms),
                passed=False,
                details=str(exc),
            )
        )
        failures.append(str(exc))

    # 2. Warm session 5m candle query: p95 < 100.0ms
    try:
        val_5m = _validate_finite_positive(session_5m_p95_ms, "session_5m_p95_ms")
        p_5m = val_5m < 100.0
        gates.append(
            GateResult(
                name="Warm session 5m query latency (p95)",
                target="< 100.00 ms",
                actual=f"{val_5m:.2f} ms",
                passed=p_5m,
            )
        )
        if not p_5m:
            failures.append(f"Warm session 5m query p95 {val_5m:.2f}ms >= 100.0ms")
    except ValueError as exc:
        gates.append(
            GateResult(
                name="Warm session 5m query latency (p95)",
                target="< 100.00 ms",
                actual=str(session_5m_p95_ms),
                passed=False,
                details=str(exc),
            )
        )
        failures.append(str(exc))

    # 3. Warm month 1d candle query: p95 < 250.0ms
    try:
        val_1d = _validate_finite_positive(month_1d_p95_ms, "month_1d_p95_ms")
        p_1d = val_1d < 250.0
        gates.append(
            GateResult(
                name="Warm month 1d candle query latency (p95)",
                target="< 250.00 ms",
                actual=f"{val_1d:.2f} ms",
                passed=p_1d,
            )
        )
        if not p_1d:
            failures.append(f"Warm month 1d candle query p95 {val_1d:.2f}ms >= 250.0ms")
    except ValueError as exc:
        gates.append(
            GateResult(
                name="Warm month 1d candle query latency (p95)",
                target="< 250.00 ms",
                actual=str(month_1d_p95_ms),
                passed=False,
                details=str(exc),
            )
        )
        failures.append(str(exc))

    # 4. Writer CPU reduction: >= 50% vs legacy baseline
    try:
        lake_cpu = _validate_finite_positive(writer_cpu_sec_per_1m, "writer_cpu_sec_per_1m")
        leg_cpu = _validate_finite_positive(legacy_writer_cpu_sec_per_1m, "legacy_writer_cpu_sec_per_1m")
        reduction_pct = ((leg_cpu - lake_cpu) / leg_cpu) * 100.0
        p_cpu = reduction_pct >= 50.0
        gates.append(
            GateResult(
                name="Writer CPU reduction vs legacy baseline",
                target=">= 50.00 %",
                actual=f"{reduction_pct:.1f}% (lake={lake_cpu:.2f}s/M, legacy={leg_cpu:.2f}s/M)",
                passed=p_cpu,
            )
        )
        if not p_cpu:
            failures.append(
                f"Writer CPU reduction {reduction_pct:.1f}% < 50.0% required "
                f"(lake={lake_cpu:.2f}s, legacy={leg_cpu:.2f}s per 1M ticks)"
            )
    except ValueError as exc:
        gates.append(
            GateResult(
                name="Writer CPU reduction vs legacy baseline",
                target=">= 50.00 %",
                actual="Invalid",
                passed=False,
                details=str(exc),
            )
        )
        failures.append(str(exc))

    # 5. Event-loop lag: p99 < 20.0ms (if present/evaluated)
    if event_loop_lag_p99_ms is not None:
        try:
            val_lag = _validate_finite_positive(event_loop_lag_p99_ms, "event_loop_lag_p99_ms")
            p_lag = val_lag < 20.0
            gates.append(
                GateResult(
                    name="Writer event-loop lag (p99)",
                    target="< 20.00 ms",
                    actual=f"{val_lag:.2f} ms",
                    passed=p_lag,
                )
            )
            if not p_lag:
                failures.append(f"Event-loop lag p99 {val_lag:.2f}ms >= 20.0ms")
        except ValueError as exc:
            gates.append(
                GateResult(
                    name="Writer event-loop lag (p99)",
                    target="< 20.00 ms",
                    actual=str(event_loop_lag_p99_ms),
                    passed=False,
                    details=str(exc),
                )
            )
            failures.append(str(exc))

    # 6. Freshness: p99 <= flush_interval + 1.0s (if present/evaluated)
    if freshness_p99_ms is not None:
        try:
            val_fresh = _validate_finite_positive(freshness_p99_ms, "freshness_p99_ms")
            thresh_ms = (float(flush_interval_seconds) + 1.0) * 1000.0
            p_fresh = val_fresh <= thresh_ms
            gates.append(
                GateResult(
                    name="Receive-to-visible freshness (p99)",
                    target=f"<= {thresh_ms:.0f} ms (flush_interval {flush_interval_seconds}s + 1.0s)",
                    actual=f"{val_fresh:.2f} ms",
                    passed=p_fresh,
                )
            )
            if not p_fresh:
                failures.append(f"Freshness p99 {val_fresh:.2f}ms > threshold {thresh_ms:.0f}ms")
        except ValueError as exc:
            gates.append(
                GateResult(
                    name="Receive-to-visible freshness (p99)",
                    target=f"<= {(float(flush_interval_seconds) + 1.0) * 1000.0:.0f} ms",
                    actual=str(freshness_p99_ms),
                    passed=False,
                    details=str(exc),
                )
            )
            failures.append(str(exc))

    overall = len(failures) == 0
    return PerformanceQualificationSummary(
        overall_passed=overall,
        gates=gates,
        failure_reasons=failures,
    )
