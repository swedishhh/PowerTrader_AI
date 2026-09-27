"""Regression tests for ArcticService's concurrency safety.

The live container segfaulted twice on 2026-09-27 with the same signature
(PID 1, signal 11, no Python traceback) from unsynchronized concurrent
access to a shared ArcticDB/LMDB store. The first fix (a singleton server —
pt_arctic_server.py) removed cross-*process* access, but missed that
multiprocessing.managers.BaseManager handles each client connection in its
own thread and doesn't serialize calls — so multiple threads inside one
process could still land in ArcticService's methods on the same
Arctic/Library object at once. The real fix is the threading.Lock in
ArcticService; these tests exist so that lock can never be silently removed,
narrowed, or broken by a refactor without a test failing first.
"""
import threading
import time

import pandas as pd
import pytest

import pt_arctic_server as srv

pytest.importorskip("arcticdb")


def _make_store(tmp_path, coins=("BTC", "ETH", "SOL", "XRP", "DOGE"), rows=200):
    import arcticdb as adb

    arctic_url = f"lmdb:///{tmp_path}"
    a = adb.Arctic(arctic_url)
    a.create_library("kucoin60")
    lib = a.get_library("kucoin60")
    idx = pd.date_range("2026-01-01", periods=rows, freq="h", tz="UTC")
    df = pd.DataFrame(
        {"open": range(rows), "high": range(rows), "low": range(rows),
         "close": range(rows), "volume": [1] * rows},
        index=idx,
    )
    for coin in coins:
        lib.write(f"{coin}_USDT", df)
    return arctic_url


def test_calls_are_serialized_not_concurrent(tmp_path):
    """Directly proves the lock gates access — not just "present", but
    actually blocking a concurrent caller — rather than relying on a stress
    test's timing luck to expose a broken/missing lock."""
    arctic_url = _make_store(tmp_path, coins=("BTC",), rows=5)
    service = srv.ArcticService(arctic_url)

    service._lock.acquire()
    result = {}
    started = threading.Event()

    def call():
        started.set()
        result["libs"] = service.list_libraries()

    th = threading.Thread(target=call, daemon=True)
    th.start()
    started.wait(timeout=2)
    time.sleep(0.2)  # give it a chance to (wrongly) finish if unlocked

    assert th.is_alive(), "call should still be blocked while the lock is held"
    assert "libs" not in result

    service._lock.release()
    th.join(timeout=2)
    assert not th.is_alive()
    assert result["libs"] == ["kucoin60"]


def test_concurrent_reads_across_threads_dont_corrupt_or_crash(tmp_path):
    """The actual live access pattern: many threads (pt_web's file-watcher
    loop, request handlers, other subprocesses' connections) hitting the
    same ArcticService concurrently. Not a proof of absence of races, but a
    real regression guard — this reliably deadlocks/corrupts within a few
    hundred ops if the lock is removed or doesn't cover the full call."""
    coins = ("BTC", "ETH", "SOL", "XRP", "DOGE")
    arctic_url = _make_store(tmp_path, coins=coins, rows=200)
    service = srv.ArcticService(arctic_url)

    errors = []

    def worker(worker_id):
        try:
            for i in range(60):
                coin = coins[(worker_id + i) % len(coins)]
                sym = f"{coin}_USDT"
                out = service.read("kucoin60", sym)
                if len(out) != 200:
                    raise AssertionError(f"worker {worker_id}: got {len(out)} rows for {sym}")
                if "kucoin60" not in service.list_libraries():
                    raise AssertionError(f"worker {worker_id}: kucoin60 missing")
                if sym not in service.list_symbols("kucoin60"):
                    raise AssertionError(f"worker {worker_id}: {sym} missing")
        except Exception as e:
            errors.append((worker_id, repr(e)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=30)

    still_alive = [th for th in threads if th.is_alive()]
    assert not still_alive, f"{len(still_alive)} worker thread(s) never finished (deadlock?)"
    assert not errors, f"{len(errors)} worker(s) hit errors: {errors[:5]}"


def test_concurrent_writes_and_reads_on_distinct_symbols(tmp_path):
    """Interleaved writers and readers — covers the write path too, not just
    reads. Each writer repeatedly write()s its own distinct symbol (rather
    than append(), which requires strictly-increasing timestamps and would
    make this fragile to thread scheduling order for reasons unrelated to
    the lock under test); readers hit a stable pre-populated symbol no
    writer touches."""
    coins = ("BTC", "ETH")
    arctic_url = _make_store(tmp_path, coins=coins, rows=50)
    service = srv.ArcticService(arctic_url)
    errors = []

    def reader():
        try:
            for _ in range(40):
                out = service.read("kucoin60", "BTC_USDT")
                assert len(out) == 50
        except Exception as e:
            errors.append(("reader", repr(e)))

    def writer(writer_id):
        try:
            idx = pd.date_range("2026-02-01", periods=10, freq="h", tz="UTC")
            for i in range(15):
                df = pd.DataFrame(
                    {"open": [float(i)] * 10, "high": [float(i)] * 10,
                     "low": [float(i)] * 10, "close": [float(i)] * 10,
                     "volume": [1.0] * 10},
                    index=idx,
                )
                service.write("kucoin60", f"WRITER{writer_id}_USDT", df)
        except Exception as e:
            errors.append((f"writer-{writer_id}", repr(e)))

    threads = [threading.Thread(target=reader) for _ in range(6)] + \
              [threading.Thread(target=writer, args=(w,)) for w in range(3)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=30)

    assert not any(th.is_alive() for th in threads), "deadlock: a thread never finished"
    assert not errors, f"errors: {errors[:5]}"

    for w in range(3):
        out = service.read("kucoin60", f"WRITER{w}_USDT")
        assert len(out) == 10
