"""
Account-value reconstruction — pure functions + a thin I/O wrapper.

Rebuilds a continuous cash + per-coin-position ledger by walking forward
from account inception through trade_history.jsonl, then marks held coins
to KuCoin candle prices (pt_pricesource.ArcticPriceSource) to produce a
continuous $/% account value series — replacing the periodically-written,
gap-prone account_value_history.jsonl snapshot as the chart's data source.

Reconstruction anchor: every exchange's account_value_history.jsonl has a
first entry that predates its first trade — at that instant there are no
coin holdings, so that single number is a fully-decomposed seed (cash =
that value, every coin qty = 0). Walking forward from there through every
trade gives a continuous curve without depending on the current live
snapshot being untainted by anything outside the bot's own recorded trades.
The live trader_status.json snapshot is used only as an end-to-end
validation check (validate_against_live), never as the anchor.

Layering: stream_trade_deltas() (and _iter_raw_lines()) are the only code
that touches the raw JSONL file; every atom above them works on parsed
TradeDelta objects / DataFrames with no file-format awareness, so a future
storage-format change only requires reimplementing that one function.

No web/UI dependencies — only pt_env (path helpers) and pt_pricesource
(price data). Callers in pt_web.py do the HTTP-facing wiring.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

from pt_env import utc_to_ts
from pt_pricesource import ArcticPriceSource, PriceSource

MAX_LINE_BYTES = 64 * 1024  # generous guard; real trade_history.jsonl lines are ~180B
_CHUNK_SIZE = 1024 * 1024   # 1MB read chunks — never buffer the whole file


# ---------------------------------------------------------------------------
# Streaming / low-level reconstruction — the only file-format-aware code
# ---------------------------------------------------------------------------


@dataclass
class TradeDelta:
    ts: float
    cash_delta: float
    coin: str
    qty_delta: float
    price: float
    notional_usd: float


def apply_fee_fallback_adjustment(row: dict) -> float:
    """net_usd adjusted for the fee-fallback drift: fees_fallback_applied_usd
    is baked into realized_profit_usd but never into net_usd (pt_trader.py's
    _record_trade), so on a sell where fees_missing is true, subtract it here
    to match real cash flow. No-op for demo/shadow (always fees_missing=False)
    and for kraken's normal case (real reported fees are already in net_usd)."""
    net = float(row.get("net_usd") or 0.0)
    if row.get("side") == "sell" and row.get("fees_missing"):
        net -= float(row.get("fees_fallback_applied_usd") or 0.0)
    return net


def _iter_raw_lines(path: Path, start_offset: int = 0) -> Iterator[tuple[bytes, int]]:
    """Chunked binary line reader. Yields (line_bytes, offset_after_line).
    Never buffers the whole file; a runaway line with no newline for
    MAX_LINE_BYTES is dropped and parsing resyncs at the next newline,
    rather than growing the buffer unboundedly."""
    if not path.exists():
        return
    with open(path, "rb") as f:
        f.seek(start_offset)
        buf = b""
        pos = start_offset
        while True:
            chunk = f.read(_CHUNK_SIZE)
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                pos += len(line) + 1
                yield line, pos
            if len(buf) > MAX_LINE_BYTES:
                pos += len(buf)
                buf = b""
        # trailing partial line (no terminating newline yet) is intentionally
        # not yielded — it may still be mid-write; picked up on the next call.


def stream_trade_deltas(path: Path, start_offset: int = 0) -> Iterator[tuple[TradeDelta, int]]:
    """Stream-parse trade_history.jsonl forward from a byte offset, yielding
    (delta, byte_offset_after_line) for every non-skip, well-formed buy/sell
    row. Malformed lines are skipped, never raised."""
    for raw, offset in _iter_raw_lines(path, start_offset):
        if not raw.strip():
            continue
        if b'"side": "skip"' in raw or b'"side":"skip"' in raw:
            continue
        try:
            row = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        side = row.get("side")
        if side not in ("buy", "sell"):
            continue
        try:
            ts = utc_to_ts(row["ts"])
            coin = str(row["symbol"]).split("_", 1)[0].upper()
            qty = float(row.get("qty") or 0.0)
            price = float(row.get("price") or 0.0)
            notional = float(row.get("notional_usd") or (price * qty))
        except (KeyError, ValueError, TypeError):
            continue
        yield TradeDelta(
            ts=ts,
            cash_delta=apply_fee_fallback_adjustment(row),
            coin=coin,
            qty_delta=qty if side == "buy" else -qty,
            price=price,
            notional_usd=notional,
        ), offset


# ---------------------------------------------------------------------------
# Trade history atom
# ---------------------------------------------------------------------------


def get_trade_history(
    trade_history_path: Path,
    coin: Optional[str] = None,
    start_ts: Optional[float] = None,
    end_ts: Optional[float] = None,
) -> list[dict]:
    """Filtered, chronological trade records (excludes 'skip' rows). Pure
    file read + filter, independent of the web/model layer."""
    out = []
    for raw, _ in _iter_raw_lines(trade_history_path):
        if not raw.strip() or b'"side": "skip"' in raw or b'"side":"skip"' in raw:
            continue
        try:
            row = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if coin is not None and not str(row.get("symbol", "")).upper().startswith(f"{coin.upper()}_"):
            continue
        try:
            ts = utc_to_ts(row["ts"])
        except (KeyError, ValueError, TypeError):
            continue
        if start_ts is not None and ts < start_ts:
            continue
        if end_ts is not None and ts > end_ts:
            continue
        out.append(row)
    return out


# ---------------------------------------------------------------------------
# Ledger reconstruction
# ---------------------------------------------------------------------------


def reconstruct_ledger(seed_ts: float, seed_cash: float, deltas: list[TradeDelta]) -> pd.DataFrame:
    """Pure forward cumulative fold, seeded at account inception (cash =
    seed_cash, every coin qty = 0). Event-level output (one row per trade,
    plus the seed row), wide-format: index=ts, columns=[cash, qty_<COIN>...].
    Small regardless of raw file size."""
    ordered = sorted(deltas, key=lambda d: d.ts)
    cash = seed_cash
    qty: dict[str, float] = {}
    rows = [{"ts": seed_ts, "cash": cash}]
    for d in ordered:
        cash += d.cash_delta
        qty[d.coin] = qty.get(d.coin, 0.0) + d.qty_delta
        rows.append({"ts": d.ts, "cash": cash, **{f"qty_{c}": v for c, v in qty.items()}})
    df = pd.DataFrame(rows).set_index("ts").sort_index()
    return df.fillna(0.0)


def get_coin_entry_baseline(deltas: list[TradeDelta], coin: str) -> Optional[float]:
    """Notional value at that coin's first trade — the per-coin % baseline.
    None if the coin was never traded."""
    coin_deltas = [d for d in deltas if d.coin == coin]
    if not coin_deltas:
        return None
    first = min(coin_deltas, key=lambda d: d.ts)
    return abs(first.notional_usd)


# ---------------------------------------------------------------------------
# Price atom
# ---------------------------------------------------------------------------


def get_price_series(
    coin: str,
    tf_minutes: int,
    start_ts: Optional[float] = None,
    end_ts: Optional[float] = None,
    price_source: Optional[PriceSource] = None,
) -> pd.DataFrame:
    """Thin, stable wrapper around ArcticPriceSource.get_candles."""
    src = price_source or ArcticPriceSource()
    df = src.get_candles(coin, tf_minutes)
    if df.empty:
        return df
    if start_ts is not None:
        df = df[df.index >= pd.Timestamp(start_ts, unit="s", tz="UTC")]
    if end_ts is not None:
        df = df[df.index <= pd.Timestamp(end_ts, unit="s", tz="UTC")]
    return df


# ---------------------------------------------------------------------------
# Bucketing helpers
# ---------------------------------------------------------------------------


def _bucket_grid(start_ts: float, end_ts: float, tf_minutes: int) -> pd.Index:
    """Float-unix-seconds bucket grid. Everything in this module stays in
    float-seconds space (matching reconstruct_ledger's index) rather than
    pd.Timestamp, so merge_asof never hits a dtype mismatch between the
    ledger (float index) and price data (tz-aware DatetimeIndex)."""
    step = tf_minutes * 60
    n = int((end_ts - start_ts) // step) + 1
    return pd.Index([start_ts + i * step for i in range(max(n, 0))], dtype="float64", name="ts")


def _index_to_float_seconds(series: pd.Series) -> pd.Series:
    """Normalize a Series' index to float unix seconds, whether it's already
    float (ledger-derived) or a tz-aware DatetimeIndex (price data)."""
    if isinstance(series.index, pd.DatetimeIndex):
        s = series.copy()
        s.index = pd.Index(series.index.asi8 / 1e9, dtype="float64", name="ts")
        return s
    return series


def _asof_into_grid(grid: pd.Index, series: pd.Series, fill: float = 0.0) -> pd.Series:
    """merge_asof a (possibly empty) series onto a bucket grid, backward
    direction (each bucket gets the last known value at-or-before it)."""
    if series.empty:
        return pd.Series(fill, index=grid)
    s = _index_to_float_seconds(series).sort_index()
    left = pd.DataFrame({"ts": grid})
    right = s.rename("value").reset_index().rename(columns={s.index.name or "index": "ts"})
    merged = pd.merge_asof(left, right, on="ts", direction="backward")
    return merged.set_index("ts")["value"].fillna(fill)


# ---------------------------------------------------------------------------
# Per-coin atoms
# ---------------------------------------------------------------------------


def get_coin_position_series(ledger: pd.DataFrame, coin: str) -> pd.Series:
    """qty(t) for one coin. Floored at 0 — a real spot balance can never go
    negative, but the cumulative trade fold can dip below zero if
    trade_history.jsonl contains a duplicate/erroneous record (observed in
    practice: a duplicate TRAIL_SELL logged twice for the same fill). The
    raw ledger itself is left unclamped so that kind of data issue stays
    inspectable; this is just the display-facing floor."""
    col = f"qty_{coin}"
    if col not in ledger.columns:
        return pd.Series(dtype=float, name=col)
    return ledger[col].clip(lower=0.0)


def get_coin_value_series(
    ledger: pd.DataFrame,
    coin: str,
    price_df: pd.DataFrame,
    tf_minutes: int,
    start_ts: float,
    end_ts: float,
    entry_deltas: Optional[list[TradeDelta]] = None,
) -> tuple[pd.DataFrame, Optional[str]]:
    """$ position value(t) = qty(t) * close_price(t), bucketed at tf_minutes.
    Returns (DataFrame[qty, price, value], warning|None). If the coin traded
    before its earliest available candle, that gap is filled with the
    trade's own recorded fill price (from entry_deltas) rather than NaN/$0,
    and a warning is returned rather than silently swallowed."""
    grid = _bucket_grid(start_ts, end_ts, tf_minutes)
    qty = _asof_into_grid(grid, get_coin_position_series(ledger, coin))

    warning = None
    if price_df.empty:
        fallback_price = 0.0
        if entry_deltas:
            coin_deltas = [d for d in entry_deltas if d.coin == coin]
            if coin_deltas:
                fallback_price = min(coin_deltas, key=lambda d: d.ts).price
        price = pd.Series(fallback_price, index=grid)
        if (qty != 0).any():
            warning = f"No KuCoin candle data available for {coin}; using trade fill price"
    else:
        close = price_df["close"]
        price = _asof_into_grid(grid, close, fill=float("nan"))
        earliest_candle_ts = price_df.index.min()
        earliest_candle_float = earliest_candle_ts.value / 1e9
        pre_candle_mask = grid < earliest_candle_float
        if pre_candle_mask.any() and (qty[pre_candle_mask] != 0).any():
            fallback_price = close.iloc[0]
            if entry_deltas:
                coin_deltas = [d for d in entry_deltas if d.coin == coin]
                if coin_deltas:
                    fallback_price = min(coin_deltas, key=lambda d: d.ts).price
            price = price.where(~pre_candle_mask, fallback_price)
            warning = (
                f"{coin} traded before its earliest available candle "
                f"({earliest_candle_ts}); used trade fill price for that gap"
            )
        price = price.ffill().fillna(0.0)

    out = pd.DataFrame({"qty": qty, "price": price})
    out["value"] = out["qty"] * out["price"]
    return out, warning


# ---------------------------------------------------------------------------
# Portfolio-wide atoms
# ---------------------------------------------------------------------------


def get_cash_series(ledger: pd.DataFrame, tf_minutes: int, start_ts: float, end_ts: float) -> pd.Series:
    grid = _bucket_grid(start_ts, end_ts, tf_minutes)
    return _asof_into_grid(grid, ledger["cash"])


def get_total_value_series(
    ledger: pd.DataFrame,
    price_sources_by_coin: dict[str, pd.DataFrame],
    tf_minutes: int,
    start_ts: float,
    end_ts: float,
    entry_deltas: Optional[list[TradeDelta]] = None,
) -> tuple[pd.DataFrame, list[str]]:
    """total_account_value(t) = cash(t) + sum of every held coin's value(t).
    Returns (DataFrame[cash, holdings_value, total_account_value], warnings)."""
    cash = get_cash_series(ledger, tf_minutes, start_ts, end_ts)
    coins = [c[4:] for c in ledger.columns if c.startswith("qty_")]

    holdings_value = pd.Series(0.0, index=cash.index)
    warnings: list[str] = []
    for coin in coins:
        price_df = price_sources_by_coin.get(coin, pd.DataFrame())
        coin_out, warning = get_coin_value_series(
            ledger, coin, price_df, tf_minutes, start_ts, end_ts, entry_deltas
        )
        holdings_value = holdings_value.add(coin_out["value"], fill_value=0.0)
        if warning:
            warnings.append(warning)

    out = pd.DataFrame({"cash": cash, "holdings_value": holdings_value})
    out["total_account_value"] = out["cash"] + out["holdings_value"]
    return out, warnings


# ---------------------------------------------------------------------------
# Shared conversion + validation
# ---------------------------------------------------------------------------


def get_pct_series(value_series: pd.Series, baseline: float) -> pd.Series:
    """% change vs. a fixed baseline. Reused for both scopes: portfolio %
    uses the account-inception seed value; per-coin % uses
    get_coin_entry_baseline() for that coin."""
    if not baseline:
        return pd.Series(0.0, index=value_series.index)
    return (value_series - baseline) / baseline * 100.0


def validate_against_live(reconstructed_last: float, live_total_account_value: float, tol_pct: float = 1.0) -> dict:
    """Pure comparison between the forward-reconstructed value near "now"
    and the actual current live snapshot. Never silently trusted either way
    — callers surface {"ok": False, ...} as a visible warning."""
    if live_total_account_value == 0:
        return {"ok": reconstructed_last == 0, "diff_usd": reconstructed_last, "diff_pct": None}
    diff_usd = reconstructed_last - live_total_account_value
    diff_pct = diff_usd / live_total_account_value * 100.0
    return {"ok": abs(diff_pct) <= tol_pct, "diff_usd": diff_usd, "diff_pct": diff_pct}


# ---------------------------------------------------------------------------
# I/O wrappers (env/xk-aware; only these + stream_trade_deltas touch disk)
# ---------------------------------------------------------------------------


def _read_seed(account_history_path: Path) -> Optional[tuple[float, float]]:
    """First line of account_value_history.jsonl -> (seed_ts, seed_cash)."""
    for raw, _ in _iter_raw_lines(account_history_path):
        if not raw.strip():
            continue
        try:
            row = json.loads(raw)
            return utc_to_ts(row["ts"]), float(row["total_account_value"])
        except (json.JSONDecodeError, KeyError, ValueError, TypeError, UnicodeDecodeError):
            continue
    return None


def _load_ledger_cache(cache_path: Path) -> tuple[int, list[TradeDelta]]:
    if not cache_path.exists():
        return 0, []
    try:
        data = json.loads(cache_path.read_text())
        deltas = [TradeDelta(**d) for d in data.get("deltas", [])]
        return int(data.get("byte_offset", 0)), deltas
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return 0, []


def _save_ledger_cache(cache_path: Path, byte_offset: int, deltas: list[TradeDelta]) -> None:
    tmp = cache_path.with_suffix(".tmp")
    payload = {"byte_offset": byte_offset, "deltas": [d.__dict__ for d in deltas]}
    tmp.write_text(json.dumps(payload))
    tmp.replace(cache_path)


def _get_all_deltas(trade_history_path: Path, cache_path: Path) -> list[TradeDelta]:
    """Incremental, cached trade-delta list. Only bytes written since the
    last call are re-parsed; falls back to a full reparse if the file was
    truncated/rotated underneath the cache."""
    byte_offset, deltas = _load_ledger_cache(cache_path)
    file_size = trade_history_path.stat().st_size if trade_history_path.exists() else 0
    if file_size < byte_offset:
        byte_offset, deltas = 0, []
    new_offset = byte_offset
    for delta, offset in stream_trade_deltas(trade_history_path, start_offset=byte_offset):
        deltas.append(delta)
        new_offset = offset
    if new_offset != byte_offset:
        _save_ledger_cache(cache_path, new_offset, deltas)
    return deltas


def _default_price_source(env) -> PriceSource:
    """ArcticPriceSource pointed at this deployment's actual KuCoin store
    (env.historic_data_dir, i.e. pt_config.json's kucoin_local_data_dir —
    the same store pt_trainer.py reads for training), not
    ArcticPriceSource's own generic ~/dev/data/arcticdb default, which is a
    different store and typically doesn't hold this project's candles."""
    return ArcticPriceSource(arctic_url=f"lmdb:///{env.historic_data_dir}")


def build_account_series(
    env,
    xk: str,
    tf_minutes: int,
    coin: Optional[str] = None,
    start_ts: Optional[float] = None,
    end_ts: Optional[float] = None,
    price_source: Optional[PriceSource] = None,
) -> dict:
    """coin=None -> portfolio total; coin='BTC' -> that coin's $/% series.
    Returns {"points": [...], "baseline": float, "warning": {...}|None}."""
    price_source = price_source or _default_price_source(env)
    seed = _read_seed(env.account_history_path(xk))
    if seed is None:
        return {"points": [], "baseline": None, "warning": {"message": f"No account_value_history seed for {xk}"}}
    seed_ts, seed_cash = seed

    cache_path = env.hub_data_xk_dir(xk) / "account_ledger_cache.json"
    deltas = _get_all_deltas(env.trade_history_path(xk), cache_path)
    ledger = reconstruct_ledger(seed_ts, seed_cash, deltas)

    range_start = start_ts if start_ts is not None else seed_ts
    range_end = end_ts if end_ts is not None else pd.Timestamp.utcnow().timestamp()

    coins = [c[4:] for c in ledger.columns if c.startswith("qty_")]
    # Deliberately don't pass range_start here: get_coin_value_series needs
    # each coin's FULL available candle history (not just the requested
    # display window) to correctly tell "no candles before this trade" apart
    # from "we just didn't fetch that far back". _bucket_grid/_asof_into_grid
    # already scope the final output to [range_start, range_end] regardless.
    price_sources = {
        c: get_price_series(c, tf_minutes, price_source=price_source)
        for c in coins
    }

    warning = None
    if coin is not None:
        coin_out, w = get_coin_value_series(
            ledger, coin, price_sources.get(coin, pd.DataFrame()),
            tf_minutes, range_start, range_end, deltas,
        )
        baseline = get_coin_entry_baseline(deltas, coin)
        value = coin_out["value"]
        if w:
            warning = {"message": w}
    else:
        total_out, warnings_list = get_total_value_series(
            ledger, price_sources, tf_minutes, range_start, range_end, deltas
        )
        baseline = seed_cash
        value = total_out["total_account_value"]
        if warnings_list:
            warning = {"message": "; ".join(warnings_list)}

        live = _read_live_total(env, xk)
        if live is not None and not value.empty:
            check = validate_against_live(float(value.iloc[-1]), live)
            if not check["ok"]:
                msg = (
                    f"Reconstructed total (${value.iloc[-1]:,.2f}) differs from live "
                    f"snapshot (${live:,.2f}) by {check['diff_pct']:.2f}%"
                )
                warning = {"message": msg} if warning is None else {"message": warning["message"] + "; " + msg}

    points = [{"ts": int(ts), "value": float(v)} for ts, v in value.items() if pd.notna(v)]
    return {"points": points, "baseline": baseline, "warning": warning}


def _read_live_total(env, xk: str) -> Optional[float]:
    path = env.trader_status_path(xk)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return float(data.get("account", {}).get("total_account_value"))
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def build_account_summary(env, xk: str) -> dict:
    """{"total": {value, pct}, "coins": {SYM: {value, pct}, ...}} — latest
    point only, cheap. Powers the Accounts-tab table."""
    price_source = _default_price_source(env)
    seed = _read_seed(env.account_history_path(xk))
    if seed is None:
        return {"total": None, "coins": {}}
    seed_ts, seed_cash = seed

    cache_path = env.hub_data_xk_dir(xk) / "account_ledger_cache.json"
    deltas = _get_all_deltas(env.trade_history_path(xk), cache_path)
    ledger = reconstruct_ledger(seed_ts, seed_cash, deltas)

    now_ts = pd.Timestamp.utcnow().timestamp()
    coins = [c[4:] for c in ledger.columns if c.startswith("qty_")]

    result_coins = {}
    holdings_total = 0.0
    for coin in coins:
        qty_series = get_coin_position_series(ledger, coin)
        qty = float(qty_series.iloc[-1]) if not qty_series.empty else 0.0

        # A single current point, not a time series: a small recent window is
        # enough for a current price, no need for get_coin_value_series's
        # full-history pre-candle-gap machinery here.
        price_df = get_price_series(coin, 60, now_ts - 86400, now_ts, price_source)
        if not price_df.empty:
            current_price = float(price_df["close"].iloc[-1])
        else:
            coin_deltas = [d for d in deltas if d.coin == coin]
            current_price = coin_deltas[-1].price if coin_deltas else 0.0

        value = qty * current_price
        baseline = get_coin_entry_baseline(deltas, coin)
        pct = float(get_pct_series(pd.Series([value]), baseline).iloc[0]) if baseline else None
        result_coins[coin] = {"value": value, "pct": pct}
        holdings_total += value

    cash = float(ledger["cash"].iloc[-1]) if not ledger.empty else seed_cash
    total_value = cash + holdings_total
    total_pct = float(get_pct_series(pd.Series([total_value]), seed_cash).iloc[0])

    return {"total": {"value": total_value, "pct": total_pct}, "coins": result_coins}
