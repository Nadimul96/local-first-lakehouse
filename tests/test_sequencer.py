from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from lakehouse.ingest.sequencer import Sequencer


def test_releases_only_events_older_than_the_window() -> None:
    seq: Sequencer[str] = Sequencer(window_ms=100)
    assert seq.add(1_000, "a") == []
    assert seq.add(1_050, "b") == []
    assert seq.add(1_120, "c") == ["a"]
    assert seq.add(1_300, "d") == ["b", "c"]
    assert seq.flush() == ["d"]


def test_late_event_is_released_in_timestamp_order_with_arrival_tiebreak() -> None:
    seq: Sequencer[str] = Sequencer(window_ms=50)
    seq.add(1_000, "first")
    seq.add(990, "earlier")
    seq.add(1_000, "same-time-later-arrival")
    assert seq.add(2_000, "late") == ["earlier", "first", "same-time-later-arrival"]


def test_bounded_memory_forces_oldest_out_instead_of_dropping() -> None:
    seq: Sequencer[int] = Sequencer(window_ms=10_000, max_pending=3)
    released = [item for n in range(5) for item in seq.add(n, n)]
    assert released == [0, 1]
    assert seq.forced_releases == 2
    assert seq.pending == 3


def test_zero_window_is_pass_through() -> None:
    seq: Sequencer[str] = Sequencer(window_ms=0)
    assert seq.add(5, "a") == ["a"]
    assert seq.add(3, "b") == ["b"]


@given(
    st.lists(st.tuples(st.integers(0, 5_000), st.integers()), max_size=200),
    st.integers(0, 1_000),
    st.integers(1, 50),
)
def test_every_event_is_released_exactly_once(
    events: list[tuple[int, int]], window: int, cap: int
) -> None:
    seq: Sequencer[tuple[int, int]] = Sequencer(window_ms=window, max_pending=cap)
    released: list[tuple[int, int]] = []
    for index, (ts, value) in enumerate(events):
        released.extend(seq.add(ts, (index, value)))
        assert seq.pending <= cap
    released.extend(seq.flush())
    assert sorted(released) == [(i, v) for i, (_, v) in enumerate(events)]
