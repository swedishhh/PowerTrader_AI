"""Tests for pt_account_analytics.py — written alongside the implementation,
exercising the pure reconstruction/pricing atoms plus the streaming reader's
robustness against malformed/partial data (no real ArcticDB/file I/O beyond
tmp_path-written fixtures)."""
import json

import pandas as pd
import pytest

import pt_account_analytics as a


# ---------------------------------------------------------------------------
# apply_fee_fallback_adjustment
# ---------------------------------------------------------------------------


def test_fee_fallback_noop_when_fees_not_missing():
    row = {"side": "sell", "net_usd": 100.0, "fees_missing": False, "fees_fallback_applied_usd": 0.0}
    assert a.apply_fee_fallback_adjustment(row) == 100.0


def test_fee_fallback_applied_on_sell_when_fees_missing():
    row = {"side": "sell", "net_usd": 100.0, "fees_missing": True, "fees_fallback_applied_usd": 0.02}
    assert a.apply_fee_fallback_adjustment(row) == pytest.approx(99.98)


def test_fee_fallback_noop_on_buy_even_if_fees_missing():
    # fallback fee is sell-only per pt_trader.py; buys never get adjusted
    row = {"side": "buy", "net_usd": -50.0, "fees_missing": True, "fees_fallback_applied_usd": 0.02}
    assert a.apply_fee_fallback_adjustment(row) == -50.0


def test_fee_fallback_noop_for_paper_trading_zero_fees():
    # demo/shadow: fees_usd=0.0, fees_missing=False always
    row = {"side": "sell", "net_usd": 5.36, "fees_missing": False, "fees_fallback_applied_usd": 0.0}
    assert a.apply_fee_fallback_adjustment(row) == 5.36


# ---------------------------------------------------------------------------
# stream_trade_deltas / _iter_raw_lines
# ---------------------------------------------------------------------------


def _trade_line(ts, side, symbol, qty, price, tag=None, fees_missing=False, fees_fallback=0.0):
    notional = qty * price
    net = -(notional) if side == "buy" else notional
    if side == "sell" and fees_missing:
        pass  # net_usd itself doesn't include the fallback per pt_trader.py
    return json.dumps({
        "ts": ts, "side": side, "tag": tag, "symbol": symbol, "qty": qty, "price": price,
        "notional_usd": notional, "net_usd": net, "avg_cost_basis": None, "pnl_pct": None,
        "fees_usd": 0.0, "fees_missing": fees_missing, "fees_fallback_applied_usd": fees_fallback,
        "realized_profit_usd": None, "order_id": "x", "position_cost_used_usd": None,
        "position_cost_after_usd": None,
    })


def test_stream_trade_deltas_basic(tmp_path):
    path = tmp_path / "trade_history.jsonl"
    path.write_text("\n".join([
        _trade_line("2026-01-01T00:00:00Z", "buy", "BTC_USD", 0.1, 100.0),
        json.dumps({"ts": "2026-01-01T00:01:00Z", "side": "skip", "tag": "SKIP", "symbol": "ETH_USD", "qty": 0, "price": None, "notional_usd": None, "reason": "no funds"}),
        _trade_line("2026-01-01T00:02:00Z", "sell", "BTC_USD", 0.05, 110.0),
    ]) + "\n")

    deltas = [d for d, _ in a.stream_trade_deltas(path)]
    assert len(deltas) == 2  # skip excluded
    assert deltas[0].coin == "BTC"
    assert deltas[0].qty_delta == pytest.approx(0.1)
    assert deltas[0].cash_delta == pytest.approx(-10.0)
    assert deltas[1].qty_delta == pytest.approx(-0.05)
    assert deltas[1].cash_delta == pytest.approx(5.5)


def test_stream_trade_deltas_skips_malformed_lines(tmp_path):
    path = tmp_path / "trade_history.jsonl"
    path.write_text("\n".join([
        _trade_line("2026-01-01T00:00:00Z", "buy", "BTC_USD", 0.1, 100.0),
        "not json at all {{{",
        '{"side": "buy", "missing_ts_field": true}',
        _trade_line("2026-01-01T00:02:00Z", "sell", "BTC_USD", 0.05, 110.0),
    ]) + "\n")

    deltas = [d for d, _ in a.stream_trade_deltas(path)]
    assert len(deltas) == 2


def test_stream_trade_deltas_handles_oversized_line_without_crashing(tmp_path):
    path = tmp_path / "trade_history.jsonl"
    huge = "x" * (a.MAX_LINE_BYTES * 2)
    path.write_text(
        _trade_line("2026-01-01T00:00:00Z", "buy", "BTC_USD", 0.1, 100.0) + "\n"
        + huge + "\n"
        + _trade_line("2026-01-01T00:02:00Z", "sell", "BTC_USD", 0.05, 110.0) + "\n"
    )
    deltas = [d for d, _ in a.stream_trade_deltas(path)]
    assert len(deltas) == 2  # oversized line dropped, parsing resyncs after it


def test_stream_trade_deltas_incremental_offset(tmp_path):
    path = tmp_path / "trade_history.jsonl"
    path.write_text(_trade_line("2026-01-01T00:00:00Z", "buy", "BTC_USD", 0.1, 100.0) + "\n")

    first_pass = list(a.stream_trade_deltas(path))
    assert len(first_pass) == 1
    _, offset_after_first = first_pass[0]

    with open(path, "a") as f:
        f.write(_trade_line("2026-01-01T00:05:00Z", "sell", "BTC_USD", 0.05, 110.0) + "\n")

    second_pass = list(a.stream_trade_deltas(path, start_offset=offset_after_first))
    assert len(second_pass) == 1
    assert second_pass[0][0].coin == "BTC"
    assert second_pass[0][0].qty_delta == pytest.approx(-0.05)


def test_get_all_deltas_cache_only_reparses_new_bytes(tmp_path, monkeypatch):
    trade_path = tmp_path / "trade_history.jsonl"
    cache_path = tmp_path / "account_ledger_cache.json"
    trade_path.write_text(_trade_line("2026-01-01T00:00:00Z", "buy", "BTC_USD", 0.1, 100.0) + "\n")

    calls = []
    real_stream = a.stream_trade_deltas

    def spy(path, start_offset=0):
        calls.append(start_offset)
        yield from real_stream(path, start_offset)

    monkeypatch.setattr(a, "stream_trade_deltas", spy)

    deltas1 = a._get_all_deltas(trade_path, cache_path)
    assert len(deltas1) == 1
    assert calls[-1] == 0  # first call parses from the start

    with open(trade_path, "a") as f:
        f.write(_trade_line("2026-01-01T00:05:00Z", "sell", "BTC_USD", 0.05, 110.0) + "\n")

    deltas2 = a._get_all_deltas(trade_path, cache_path)
    assert len(deltas2) == 2
    assert calls[-1] > 0  # second call resumed from a nonzero cached offset, not 0


def test_get_all_deltas_restarts_on_truncation(tmp_path):
    trade_path = tmp_path / "trade_history.jsonl"
    cache_path = tmp_path / "account_ledger_cache.json"
    trade_path.write_text(
        _trade_line("2026-01-01T00:00:00Z", "buy", "BTC_USD", 0.1, 100.0) + "\n"
        + _trade_line("2026-01-01T00:05:00Z", "sell", "BTC_USD", 0.05, 110.0) + "\n"
    )
    assert len(a._get_all_deltas(trade_path, cache_path)) == 2

    # simulate truncation/rotation: file shrinks below the cached offset
    trade_path.write_text(_trade_line("2026-01-02T00:00:00Z", "buy", "ETH_USD", 1.0, 2000.0) + "\n")
    deltas = a._get_all_deltas(trade_path, cache_path)
    assert len(deltas) == 1
    assert deltas[0].coin == "ETH"


# ---------------------------------------------------------------------------
# get_trade_history
# ---------------------------------------------------------------------------


def test_get_trade_history_filters_skip_and_by_coin(tmp_path):
    path = tmp_path / "trade_history.jsonl"
    path.write_text("\n".join([
        _trade_line("2026-01-01T00:00:00Z", "buy", "BTC_USD", 0.1, 100.0),
        _trade_line("2026-01-01T00:01:00Z", "buy", "ETH_USD", 1.0, 2000.0),
        json.dumps({"ts": "2026-01-01T00:02:00Z", "side": "skip", "tag": "SKIP", "symbol": "ETH_USD", "qty": 0, "price": None, "notional_usd": None, "reason": "x"}),
    ]) + "\n")

    all_rows = a.get_trade_history(path)
    assert len(all_rows) == 2

    btc_only = a.get_trade_history(path, coin="BTC")
    assert len(btc_only) == 1
    assert btc_only[0]["symbol"] == "BTC_USD"


# ---------------------------------------------------------------------------
# get_coin_realized_pnl
# ---------------------------------------------------------------------------


def _sell_line(ts, symbol, realized_profit_usd, pnl_pct, tag=None):
    return json.dumps({
        "ts": ts, "side": "sell", "tag": tag, "symbol": symbol, "qty": 1.0, "price": 1.0,
        "notional_usd": 1.0, "net_usd": 1.0, "avg_cost_basis": 1.0,
        "pnl_pct": pnl_pct, "fees_usd": 0.0, "fees_missing": False,
        "fees_fallback_applied_usd": 0.0, "realized_profit_usd": realized_profit_usd,
        "order_id": "x", "position_cost_used_usd": None, "position_cost_after_usd": None,
    })


def test_get_coin_realized_pnl_sums_across_round_trips(tmp_path):
    # Matches the user-described scenario: 3 closed round trips at 4.5%, 5.5%, 3.6%.
    path = tmp_path / "trade_history.jsonl"
    path.write_text("\n".join([
        _trade_line("2026-01-01T00:00:00Z", "buy", "ADA_USD", 100.0, 0.25),
        _sell_line("2026-01-01T01:00:00Z", "ADA_USD", 4.50, 4.5),
        _trade_line("2026-01-02T00:00:00Z", "buy", "ADA_USD", 100.0, 0.25),
        _sell_line("2026-01-02T01:00:00Z", "ADA_USD", 5.50, 5.5),
        _trade_line("2026-01-03T00:00:00Z", "buy", "ADA_USD", 100.0, 0.25),
        _sell_line("2026-01-03T01:00:00Z", "ADA_USD", 3.60, 3.6),
    ]) + "\n")

    result = a.get_coin_realized_pnl(path, "ADA")
    assert result["realized_usd"] == pytest.approx(4.50 + 5.50 + 3.60)
    assert result["realized_pct"] == pytest.approx(4.5 + 5.5 + 3.6)
    assert result["trade_count"] == 3


def test_get_coin_realized_pnl_ignores_open_position_no_sell(tmp_path):
    path = tmp_path / "trade_history.jsonl"
    path.write_text(_trade_line("2026-01-01T00:00:00Z", "buy", "BTC_USD", 0.1, 100.0) + "\n")
    result = a.get_coin_realized_pnl(path, "BTC")
    assert result == {"realized_usd": 0.0, "realized_pct": 0.0, "trade_count": 0}


def test_get_coin_realized_pnl_excludes_lth_tagged_sells(tmp_path):
    path = tmp_path / "trade_history.jsonl"
    path.write_text("\n".join([
        _sell_line("2026-01-01T00:00:00Z", "BTC_USD", 100.0, 10.0, tag="LTH"),
        _sell_line("2026-01-02T00:00:00Z", "BTC_USD", 5.0, 2.0, tag="TRAIL_SELL"),
    ]) + "\n")
    result = a.get_coin_realized_pnl(path, "BTC")
    assert result["realized_usd"] == pytest.approx(5.0)
    assert result["realized_pct"] == pytest.approx(2.0)
    assert result["trade_count"] == 1


# ---------------------------------------------------------------------------
# reconstruct_ledger
# ---------------------------------------------------------------------------


def test_reconstruct_ledger_cumulative_cash_and_qty():
    seed_ts = 1000.0
    deltas = [
        a.TradeDelta(ts=1010.0, cash_delta=-10.0, coin="BTC", qty_delta=0.1, price=100.0, notional_usd=10.0),
        a.TradeDelta(ts=1020.0, cash_delta=5.5, coin="BTC", qty_delta=-0.05, price=110.0, notional_usd=5.5),
        a.TradeDelta(ts=1030.0, cash_delta=-20.0, coin="ETH", qty_delta=0.5, price=40.0, notional_usd=20.0),
    ]
    ledger = a.reconstruct_ledger(seed_ts, 1000.0, deltas)

    assert list(ledger.index) == [1000.0, 1010.0, 1020.0, 1030.0]
    assert ledger["cash"].iloc[0] == 1000.0
    assert ledger["cash"].iloc[-1] == pytest.approx(1000.0 - 10.0 + 5.5 - 20.0)
    assert ledger["qty_BTC"].iloc[-1] == pytest.approx(0.05)
    assert ledger["qty_ETH"].iloc[-1] == pytest.approx(0.5)
    # ETH column should be 0 (not NaN) before ETH's first trade
    assert ledger["qty_ETH"].iloc[0] == 0.0


def test_reconstruct_ledger_ignores_tag_field_entirely():
    # LTH-tagged trades must contribute identically to untagged ones —
    # tag only gates the separate P&L ledger in pt_trader.py, not the
    # trade record itself, and TradeDelta doesn't even carry tag.
    seed_ts = 1000.0
    lth_delta = a.TradeDelta(ts=1010.0, cash_delta=-500.0, coin="BTC", qty_delta=1.0, price=500.0, notional_usd=500.0)
    ledger = a.reconstruct_ledger(seed_ts, 1000.0, [lth_delta])
    assert ledger["qty_BTC"].iloc[-1] == pytest.approx(1.0)
    assert ledger["cash"].iloc[-1] == pytest.approx(500.0)


def test_get_coin_entry_baseline():
    deltas = [
        a.TradeDelta(ts=1010.0, cash_delta=-10.0, coin="BTC", qty_delta=0.1, price=100.0, notional_usd=10.0),
        a.TradeDelta(ts=1020.0, cash_delta=-5.0, coin="BTC", qty_delta=0.05, price=100.0, notional_usd=5.0),
    ]
    assert a.get_coin_entry_baseline(deltas, "BTC") == pytest.approx(10.0)  # first trade's notional
    assert a.get_coin_entry_baseline(deltas, "ETH") is None


# ---------------------------------------------------------------------------
# get_coin_value_series / get_total_value_series
# ---------------------------------------------------------------------------


def _price_df(prices: dict) -> pd.DataFrame:
    idx = pd.to_datetime(list(prices.keys()), unit="s", utc=True)
    return pd.DataFrame({"close": list(prices.values())}, index=idx).rename_axis("timestamp")


def test_get_coin_value_series_basic():
    ledger = a.reconstruct_ledger(1000.0, 1000.0, [
        a.TradeDelta(ts=1000.0, cash_delta=-100.0, coin="BTC", qty_delta=1.0, price=100.0, notional_usd=100.0),
    ])
    price_df = _price_df({1000.0: 100.0, 1060.0: 110.0, 1120.0: 120.0})
    out, warning = a.get_coin_value_series(ledger, "BTC", price_df, tf_minutes=1, start_ts=1000.0, end_ts=1120.0)
    assert warning is None
    assert out["qty"].iloc[0] == pytest.approx(1.0)
    assert out["value"].iloc[-1] == pytest.approx(1.0 * out["price"].iloc[-1])


def test_get_coin_value_series_pre_candle_gap_uses_fill_price_and_warns():
    deltas = [a.TradeDelta(ts=900.0, cash_delta=-50.0, coin="BTC", qty_delta=1.0, price=50.0, notional_usd=50.0)]
    ledger = a.reconstruct_ledger(900.0, 100.0, deltas)
    # candles only start at ts=1000, but the trade happened at ts=900
    price_df = _price_df({1000.0: 100.0, 1060.0: 110.0})
    out, warning = a.get_coin_value_series(
        ledger, "BTC", price_df, tf_minutes=1, start_ts=900.0, end_ts=1060.0, entry_deltas=deltas,
    )
    assert warning is not None
    assert "earliest available candle" in warning
    # pre-candle bucket should use the trade's own fill price (50.0), not NaN/0
    first_bucket_price = out["price"].iloc[0]
    assert first_bucket_price == pytest.approx(50.0)


def test_bucket_grid_always_includes_end_ts_even_when_not_grid_aligned():
    # tf=1day (86400s) step; end_ts deliberately NOT a multiple of the step
    # from start_ts, mirroring the real chart-vs-summary-table discrepancy:
    # without forcing end_ts onto the grid, the last point could be stale by
    # almost a full bucket width.
    start_ts = 1_700_000_000.0
    end_ts = start_ts + 86400 * 2.5  # 2.5 days later — not grid-aligned
    grid = a._bucket_grid(start_ts, end_ts, tf_minutes=1440)
    assert grid[-1] == end_ts
    assert end_ts - grid[-2] > 1  # the regular grid point before it is earlier


def test_get_coin_value_series_last_point_is_current_not_stale(tmp_path):
    # Same scenario at the get_coin_value_series level: a coarse tf (1day)
    # shouldn't leave the last point stale relative to end_ts ("now").
    ledger = a.reconstruct_ledger(1000.0, 1000.0, [
        a.TradeDelta(ts=1000.0, cash_delta=-100.0, coin="BTC", qty_delta=1.0, price=100.0, notional_usd=100.0),
    ])
    end_ts = 1000.0 + 86400 * 2.5
    price_df = _price_df({1000.0: 100.0, end_ts: 999.0})
    out, _ = a.get_coin_value_series(ledger, "BTC", price_df, tf_minutes=1440, start_ts=1000.0, end_ts=end_ts)
    assert out.index[-1] == end_ts
    assert out["price"].iloc[-1] == pytest.approx(999.0)


def test_get_total_value_series_sums_cash_and_holdings():
    deltas = [
        a.TradeDelta(ts=1000.0, cash_delta=-100.0, coin="BTC", qty_delta=1.0, price=100.0, notional_usd=100.0),
    ]
    ledger = a.reconstruct_ledger(1000.0, 1000.0, deltas)
    price_sources = {"BTC": _price_df({1000.0: 100.0, 1060.0: 100.0})}
    out, warnings = a.get_total_value_series(ledger, price_sources, tf_minutes=1, start_ts=1000.0, end_ts=1060.0, entry_deltas=deltas)
    assert warnings == []
    # cash = 1000 - 100 = 900, holdings = 1 * 100 = 100 -> total = 1000
    assert out["total_account_value"].iloc[-1] == pytest.approx(1000.0)


# ---------------------------------------------------------------------------
# get_pct_series
# ---------------------------------------------------------------------------


def test_get_pct_series_portfolio_style_baseline():
    values = pd.Series([1000.0, 1100.0, 900.0])
    pct = a.get_pct_series(values, baseline=1000.0)
    assert pct.tolist() == pytest.approx([0.0, 10.0, -10.0])


def test_get_pct_series_none_baseline_returns_zero():
    values = pd.Series([50.0, 60.0])
    pct = a.get_pct_series(values, baseline=None)
    assert pct.tolist() == [0.0, 0.0]


# ---------------------------------------------------------------------------
# validate_against_live
# ---------------------------------------------------------------------------


def test_validate_against_live_within_tolerance():
    result = a.validate_against_live(1000.0, 1005.0, tol_pct=1.0)
    assert result["ok"] is True


def test_validate_against_live_outside_tolerance():
    result = a.validate_against_live(1000.0, 1100.0, tol_pct=1.0)
    assert result["ok"] is False
    assert result["diff_pct"] == pytest.approx(-9.0909, abs=1e-3)


def test_validate_against_live_zero_live_value():
    result = a.validate_against_live(0.0, 0.0)
    assert result["ok"] is True
    result2 = a.validate_against_live(5.0, 0.0)
    assert result2["ok"] is False
