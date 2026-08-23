"""Integration test for _fetch_from_arctic's asof_ts + MAX_CANDLES interaction.

ArcticDB only allows one of date_range/row_range per read() call, so when
asof_ts is set the MAX_CANDLES cap has to be re-applied in pandas after the
date_range read. This needs a real ArcticDB store to exercise meaningfully
(the cutoff/tail logic isn't worth mocking), so it skips gracefully if BTC's
1h data isn't present in the mounted store rather than failing CI/dev setups
without that data.
"""
import time

import pytest

import pt_trainer as t

pytestmark = pytest.mark.skipif(
    t._fetch_from_arctic("BTC", 60, "kucoin_local") is None,
    reason="BTC 1h data not available in this environment's ArcticDB store",
)


def test_asof_ts_still_respects_max_candles():
    full = t._fetch_from_arctic("BTC", 60, "kucoin_local")
    assert len(full) == t.MAX_CANDLES, (
        "expected BTC's plain (no asof_ts) read to already be at the cap; "
        "if this fails the live store has fewer than MAX_CANDLES rows and "
        "this test isn't exercising the tail-trim path"
    )

    # asof_ts far in the future -> date_range cutoff excludes nothing, so
    # without the post-hoc tail() trim this would return the *entire*
    # history (77k+ rows for BTC), not just MAX_CANDLES.
    capped = t._fetch_from_arctic("BTC", 60, "kucoin_local", asof_ts=time.time() + 3600)
    assert len(capped) == t.MAX_CANDLES
    # and it should be the most recent MAX_CANDLES rows, matching the
    # no-asof_ts path exactly
    assert capped.index.equals(full.index)


def test_asof_ts_cutoff_excludes_rows_beyond_it():
    # a cutoff of "now" should never include future-dated rows regardless
    # of the cap
    cutoff_ts = time.time()
    df = t._fetch_from_arctic("BTC", 60, "kucoin_local", asof_ts=cutoff_ts)
    assert df.index.max() < __import__("pandas").Timestamp(cutoff_ts, unit="s", tz="UTC")
