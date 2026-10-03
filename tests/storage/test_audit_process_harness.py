"""Tests for the spawned-process ready/release/committed barrier helper."""
from tests.support.process_harness import BarrierProcess, barrier_file_worker


def test_spawned_process_barriers_and_unconditional_cleanup(tmp_path):
    output = tmp_path / "committed.txt"
    with BarrierProcess(barrier_file_worker, args=(str(output),)) as child:
        assert child.wait_ready(timeout=10.0)
        assert not output.exists()
        child.allow()
        assert child.wait_committed(timeout=10.0)
        assert child.join(timeout=10.0) == 0
    assert output.read_text(encoding="utf-8") == "committed"
