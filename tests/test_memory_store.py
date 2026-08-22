"""Characterization tests for MemoryStore and the pattern-matching helpers in
pt_trainer.py, written before rewriting get_patterns_matrix()'s per-call rebuild.

get_patterns_matrix() is called once per hot-loop iteration (tens of thousands
of times per timeframe) and currently rebuilds an np.array from a Python list
of ndarrays on every call. These tests pin down its exact behavior — including
the "too-short pattern" filter — so the rewrite can be verified to preserve it.
"""
import numpy as np
import pytest

import pt_trainer as t


# ---------------------------------------------------------------------------
# MemoryStore.load() / _parse_memories / _parse_weights
# ---------------------------------------------------------------------------


def test_load_missing_file_starts_empty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    store = t.MemoryStore("1hour")
    store.load()
    assert store.patterns == []
    assert store.weights == []
    assert store.dirty is False


def test_load_parses_well_formed_blob(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "training_data.json").write_text(
        '{"1hour": {"memories": "1.0 2.0{}10.0{}-5.0~3.0 4.0{}20.0{}-15.0", '
        '"weights": "1.0 1.5", "weights_high": "1.0 1.0", "weights_low": "1.0 1.0"}}'
    )
    store = t.MemoryStore("1hour")
    store.load()
    assert len(store.patterns) == 2
    np.testing.assert_array_equal(store.patterns[0], [1.0, 2.0])
    np.testing.assert_array_equal(store.patterns[1], [3.0, 4.0])
    # high/low stored /100 on disk, /100'd again on load per the docstring
    assert store.high_pcts == [0.1, 0.2]
    assert store.low_pcts == [-0.05, -0.15]
    assert store.weights == [1.0, 1.5]


def test_load_skips_malformed_entries(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    blob = "1.0 2.0{}10.0{}-5.0~garbage~1.0{}only_two_parts~3.0 4.0{}20.0{}-15.0"
    (tmp_path / "training_data.json").write_text(
        '{"1hour": {"memories": "%s"}}' % blob
    )
    store = t.MemoryStore("1hour")
    store.load()
    # only the two well-formed entries survive
    assert len(store.patterns) == 2
    np.testing.assert_array_equal(store.patterns[0], [1.0, 2.0])
    np.testing.assert_array_equal(store.patterns[1], [3.0, 4.0])


def test_load_pads_short_weight_lists_with_default(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "training_data.json").write_text(
        '{"1hour": {"memories": "1.0 2.0{}10.0{}-5.0~3.0 4.0{}20.0{}-15.0", '
        '"weights": "0.5"}}'
    )
    store = t.MemoryStore("1hour")
    store.load()
    assert store.weights == [0.5, 1.0]  # padded to match pattern count


# ---------------------------------------------------------------------------
# add_entry / flush round-trip
# ---------------------------------------------------------------------------


def test_add_entry_appends_to_all_parallel_lists():
    store = t.MemoryStore("1hour")
    store.add_entry(np.array([1.0, 2.0]), high_pct=0.1, low_pct=-0.05)
    assert store.count == 1
    assert store.high_pcts == [0.1]
    assert store.low_pcts == [-0.05]
    assert store.weights == store.high_weights == store.low_weights == [1.0]
    assert store.dirty is True


def test_flush_noop_when_not_dirty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    store = t.MemoryStore("1hour")
    store.flush()
    assert not (tmp_path / "training_data.json").exists()


def test_flush_then_load_round_trips(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    store = t.MemoryStore("1hour")
    store.add_entry(np.array([1.0, 2.0]), high_pct=0.1, low_pct=-0.05)
    store.add_entry(np.array([3.0, 4.0]), high_pct=0.2, low_pct=-0.15)
    store.flush()

    reloaded = t.MemoryStore("1hour")
    reloaded.load()
    assert len(reloaded.patterns) == 2
    np.testing.assert_array_equal(reloaded.patterns[0], [1.0, 2.0])
    np.testing.assert_array_equal(reloaded.patterns[1], [3.0, 4.0])
    assert reloaded.high_pcts == pytest.approx([0.1, 0.2])
    assert reloaded.low_pcts == pytest.approx([-0.05, -0.15])


# ---------------------------------------------------------------------------
# get_patterns_matrix — the function being rewritten
# ---------------------------------------------------------------------------


def test_get_patterns_matrix_empty_store_returns_none():
    store = t.MemoryStore("1hour")
    assert store.get_patterns_matrix() is None


def test_get_patterns_matrix_shape_and_values():
    store = t.MemoryStore("1hour")
    store.add_entry(np.array([1.0, 99.0]), 0.0, 0.0)  # pattern=[1.0], outcome=99.0
    store.add_entry(np.array([2.0, 98.0]), 0.0, 0.0)
    matrix = store.get_patterns_matrix()
    # PATTERN_LENGTH=2 -> pat_len=1 -> only the first column, outcome excluded
    np.testing.assert_array_equal(matrix, [[1.0], [2.0]])


def test_get_patterns_matrix_filters_patterns_too_short(tmp_path, monkeypatch):
    """A too-short pattern (fewer values than pat_len) can arrive via a
    corrupted/legacy training_data.json; get_patterns_matrix must silently
    drop it via load(), not raise."""
    monkeypatch.chdir(tmp_path)
    blob = "1.0 99.0{}0.0{}0.0~5.0{}0.0{}0.0~2.0 98.0{}0.0{}0.0"
    (tmp_path / "training_data.json").write_text('{"1hour": {"memories": "%s"}}' % blob)
    store = t.MemoryStore("1hour")
    store.load()
    matrix = store.get_patterns_matrix()
    np.testing.assert_array_equal(matrix, [[1.0], [2.0]])


def test_get_patterns_matrix_all_too_short_returns_none(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    blob = "5.0{}0.0{}0.0"
    (tmp_path / "training_data.json").write_text('{"1hour": {"memories": "%s"}}' % blob)
    store = t.MemoryStore("1hour")
    store.load()
    assert store.get_patterns_matrix() is None


def test_get_patterns_matrix_matches_naive_rebuild_after_many_entries(tmp_path, monkeypatch):
    """Equivalence check against the original 'rebuild from the full list every
    call' semantics, across a mix of valid and malformed patterns — this is
    the invariant the incremental rewrite must preserve. Starts from a loaded
    store (with some malformed legacy entries) and then adds many more via
    add_entry, exercising both the rebuild and incremental-append paths."""
    monkeypatch.chdir(tmp_path)
    blob = "~".join(
        f"{i}.0{{}}0.0{{}}0.0" if i % 37 == 0 else f"{i}.0 {i + 1}.0{{}}0.0{{}}0.0"
        for i in range(100)
    )
    (tmp_path / "training_data.json").write_text('{"1hour": {"memories": "%s"}}' % blob)
    store = t.MemoryStore("1hour")
    store.load()

    rng = np.random.default_rng(0)
    for i in range(500):
        store.add_entry(np.array([rng.random(), float(i)]), 0.0, 0.0)

    pat_len = t.PATTERN_LENGTH - 1
    valid = [p for p in store.patterns if len(p) > pat_len]
    expected = np.array([p[:pat_len] for p in valid], dtype=np.float64)

    np.testing.assert_array_equal(store.get_patterns_matrix(), expected)


# ---------------------------------------------------------------------------
# get_outcomes
# ---------------------------------------------------------------------------


def test_get_outcomes_returns_last_element_per_pattern():
    store = t.MemoryStore("1hour")
    store.add_entry(np.array([1.0, 99.0]), 0.0, 0.0)
    store.add_entry(np.array([2.0, 98.0]), 0.0, 0.0)
    np.testing.assert_array_equal(store.get_outcomes(), [99.0, 98.0])


def test_get_outcomes_empty_pattern_defaults_to_zero():
    store = t.MemoryStore("1hour")
    store.patterns = [np.array([]), np.array([1.0, 5.0])]
    np.testing.assert_array_equal(store.get_outcomes(), [0.0, 5.0])


def test_get_outcomes_empty_store():
    store = t.MemoryStore("1hour")
    assert store.get_outcomes().shape == (0,)


# ---------------------------------------------------------------------------
# compute_pct_changes
# ---------------------------------------------------------------------------


def test_compute_pct_changes_basic():
    opens = np.array([100.0, 50.0])
    closes = np.array([110.0, 45.0])
    highs = np.array([120.0, 55.0])
    lows = np.array([90.0, 40.0])
    close_pct, high_pct, low_pct = t.compute_pct_changes(opens, closes, highs, lows)
    np.testing.assert_allclose(close_pct, [10.0, -10.0])
    np.testing.assert_allclose(high_pct, [20.0, 10.0])
    np.testing.assert_allclose(low_pct, [-10.0, -20.0])


def test_compute_pct_changes_zero_open_guarded():
    opens = np.array([0.0])
    closes = np.array([10.0])
    highs = np.array([10.0])
    lows = np.array([10.0])
    close_pct, high_pct, low_pct = t.compute_pct_changes(opens, closes, highs, lows)
    assert close_pct[0] == 0.0
    assert high_pct[0] == 0.0
    assert low_pct[0] == 0.0


# ---------------------------------------------------------------------------
# find_matches
# ---------------------------------------------------------------------------


def test_find_matches_none_memory_matrix():
    indices, diffs = t.find_matches(np.array([1.0]), None, threshold=5.0)
    assert len(indices) == 0
    assert len(diffs) == 0


def test_find_matches_empty_memory_matrix():
    indices, diffs = t.find_matches(
        np.array([1.0]), np.empty((0, 1)), threshold=5.0
    )
    assert len(indices) == 0


def test_find_matches_basic_match_and_nonmatch():
    current = np.array([10.0])
    memory = np.array([[10.5], [50.0]])  # first is close, second is far
    indices, diffs = t.find_matches(current, memory, threshold=10.0)
    assert list(indices) == [0]


def test_find_matches_sums_zero_guarded_not_nan():
    """current=+5, memory row=-5 -> sums==0, must yield 0.0 diff, not inf/nan."""
    current = np.array([5.0])
    memory = np.array([[-5.0]])
    indices, diffs = t.find_matches(current, memory, threshold=0.0)
    assert list(indices) == [0]
    assert diffs[0] == 0.0


def test_find_matches_threshold_boundary_is_inclusive():
    current = np.array([10.0])
    memory = np.array([[11.0]])
    # sum=21, diff=1, pct_diff = 1/(21/2)*100 = 9.523809...
    diffs_at = 100.0 * 1 / (21 / 2)
    indices, diffs = t.find_matches(current, memory, threshold=diffs_at)
    assert list(indices) == [0]
    below = t.find_matches(current, memory, threshold=diffs_at - 1e-9)[0]
    assert len(below) == 0


# ---------------------------------------------------------------------------
# compute_weighted_prediction
# ---------------------------------------------------------------------------


def test_compute_weighted_prediction_empty_indices():
    result = t.compute_weighted_prediction(
        np.array([], dtype=np.int64),
        np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([]),
    )
    assert result == (0.0, 0.0, 0.0)


def test_compute_weighted_prediction_basic():
    indices = np.array([0, 1])
    outcomes = np.array([10.0, 20.0])
    weights = np.array([1.0, 1.0])
    high_pcts = np.array([0.5, 0.7])
    high_weights = np.array([1.0, 1.0])
    low_pcts = np.array([-0.2, -0.4])
    low_weights = np.array([1.0, 1.0])
    close_pred, high_pred, low_pred = t.compute_weighted_prediction(
        indices, outcomes, weights, high_pcts, high_weights, low_pcts, low_weights
    )
    assert close_pred == pytest.approx(0.15)  # mean(10,20)/100
    assert high_pred == pytest.approx(0.6)
    assert low_pred == pytest.approx(-0.3)


# ---------------------------------------------------------------------------
# update_weight
# ---------------------------------------------------------------------------


def test_update_weight_within_tolerance_unchanged():
    # predicted=1.0, tolerance band = 0.1 -> actual within [0.9, 1.1] unchanged
    result = t.update_weight(1.05, 1.0, current_weight=0.5, clamp=(-2.0, 2.0))
    assert result == 0.5


def test_update_weight_above_tolerance_increases():
    result = t.update_weight(2.0, 1.0, current_weight=0.5, clamp=(-2.0, 2.0))
    assert result == pytest.approx(0.75)


def test_update_weight_below_tolerance_decreases():
    result = t.update_weight(0.0, 1.0, current_weight=0.5, clamp=(-2.0, 2.0))
    assert result == pytest.approx(0.25)


def test_update_weight_clamped_at_upper_bound():
    result = t.update_weight(2.0, 1.0, current_weight=1.9, clamp=(-2.0, 2.0))
    assert result == 2.0


def test_update_weight_clamped_at_lower_bound():
    result = t.update_weight(0.0, 1.0, current_weight=-1.9, clamp=(-2.0, 2.0))
    assert result == -2.0


# ---------------------------------------------------------------------------
# AccuracyTracker
# ---------------------------------------------------------------------------


def test_accuracy_tracker_empty_is_zero():
    tracker = t.AccuracyTracker(window=3)
    assert tracker.accuracy == 0.0


def test_accuracy_tracker_computes_hit_rate():
    tracker = t.AccuracyTracker(window=10)
    for hit in [True, True, False, True]:
        tracker.record(hit)
    assert tracker.accuracy == pytest.approx(75.0)


def test_accuracy_tracker_rolling_window_evicts_oldest():
    tracker = t.AccuracyTracker(window=3)
    tracker.record(False)  # will be evicted
    tracker.record(True)
    tracker.record(True)
    tracker.record(True)
    assert len(tracker.hits) == 3
    assert tracker.accuracy == pytest.approx(100.0)
