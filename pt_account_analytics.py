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
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

from pt_env import utc_to_ts
from pt_pricesource import ArcticPriceSource, PriceSource

MAX_LINE_BYTES = 64 * 1024  # generous guard; real trade_history.jsonl lines are ~180B
_CHUNK_SIZE = 1024 * 1024   # 1MB read chunks — never buffer the whole file

# Always mark-to-market using hourly candles, regardless of a chart's own
# display granularity (tf_minutes there only controls x-axis bucket spacing).
# A coarser candle library (e.g. kucoin1440) often doesn't have "today"'s row
# until the day closes, so pricing directly off the display tf made a 1-day
# chart's current point stale by up to ~24h relative to what
# build_account_summary (which always uses 60min) reports for the same
# instant. 75000 60-minute candles is ~8.5 years — comfortably bounded.
PRICE_FETCH_TF_MINUTES = 60


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
    tag: Optional[str] = None
    fees_usd: float = 0.0


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
            tag=row.get("tag"),
            fees_usd=float(row.get("fees_usd") or 0.0),
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


def get_total_fees_paid(trade_history_path: Path) -> float:
    """Cumulative fees_usd (+ any fees_fallback_applied_usd) across every
    trade — every side, every coin, LTH included, since it's a real cost
    regardless of which bucket a trade belongs to. Plain context for why
    Σ Coins doesn't reconcile to TOTAL to the penny: only buy-side fees are
    excluded from the per-coin cost basis (see reconstruct_ledger's
    docstring), so this figure runs somewhat higher than that gap, not
    equal to it — it's not meant to be an exact reconciling term."""
    total = 0.0
    for row in get_trade_history(trade_history_path):
        total += float(row.get("fees_usd") or 0.0)
        total += float(row.get("fees_fallback_applied_usd") or 0.0)
    return total


def get_coin_fees_paid(trade_history_path: Path, coin: str) -> float:
    """Cumulative fees_usd (+ fees_fallback_applied_usd) for one coin, all
    sides, LTH included — same convention as get_total_fees_paid, scoped to
    a single coin."""
    total = 0.0
    for row in get_trade_history(trade_history_path, coin=coin):
        total += float(row.get("fees_usd") or 0.0)
        total += float(row.get("fees_fallback_applied_usd") or 0.0)
    return total


def get_coin_realized_pnl(trade_history_path: Path, coin: str) -> dict:
    """Cumulative realized PnL across every closed round-trip sell for one
    coin — the sum of each sell's own realized_profit_usd and pnl_pct
    (already computed and stored per-trade by pt_trader.py's cost-basis
    ledger, proportionally per partial sell), not a recomputation. Ignores
    currently-open positions (only sells realize PnL) and LTH-tagged trades
    (a separate long-term bucket, not part of the active trading strategy's
    round trips). Returns {"realized_usd", "realized_pct", "trade_count"}."""
    realized_usd = 0.0
    realized_pct = 0.0
    trade_count = 0
    for row in get_trade_history(trade_history_path, coin=coin):
        if row.get("side") != "sell":
            continue
        if str(row.get("tag") or "").upper() == "LTH":
            continue
        realized_usd += float(row.get("realized_profit_usd") or 0.0)
        realized_pct += float(row.get("pnl_pct") or 0.0)
        trade_count += 1
    return {"realized_usd": realized_usd, "realized_pct": realized_pct, "trade_count": trade_count}


# ---------------------------------------------------------------------------
# Ledger reconstruction
# ---------------------------------------------------------------------------


def _apply_cost_basis(cost_before: float, qty_before: float, qty_delta: float, notional_usd: float) -> float:
    """Average-cost accounting for one trade against a cost-basis figure:
    a buy adds gross notional (excl. fees); a sell reduces cost
    proportionally to the fraction of qty_before being sold, snapping to
    exactly zero once the remaining qty is negligible. qty_before is the
    position's quantity immediately before this trade — the caller tracks
    quantity itself; this only derives the sell fraction from it."""
    if qty_delta > 0:
        return cost_before + notional_usd
    sell_qty = -qty_delta
    frac = min(1.0, sell_qty / qty_before) if qty_before > 0 else 1.0
    remaining_qty = qty_before + qty_delta
    if remaining_qty <= 1e-8:
        return 0.0
    return cost_before - (cost_before * frac)


def reconstruct_ledger(seed_ts: float, seed_cash: float, deltas: list[TradeDelta]) -> pd.DataFrame:
    """Pure forward cumulative fold, seeded at account inception (cash =
    seed_cash, every coin qty/cost/fee_pool = 0, realized_fee_drag = 0).
    Event-level output (one row per trade, plus the seed row),
    wide-format: index=ts, columns=[cash, realized_fee_drag, qty_<COIN>...,
    cost_<COIN>..., fee_pool_<COIN>..., bot_qty_<COIN>..., bot_cost_<COIN>...].
    Small regardless of raw file size.

    Two quantity/cost tracks per coin, because pt_trader.py itself treats
    LTH holdings as a walled-off bucket the bot's own accounting never
    sees:
    - qty_<COIN> / cost_<COIN>: every buy/sell, LTH included — the real
      total held on the exchange and its cost basis, needed for
      get_total_value_series (account net worth must include LTH
      holdings' value) and for an LTH-inclusive floating-PnL breakdown
      (LTH qty/cost = qty_<COIN>/cost_<COIN> minus bot_qty_<COIN>/
      bot_cost_<COIN>).
    - bot_qty_<COIN> / bot_cost_<COIN>: LTH-tagged trades excluded
      entirely, mirroring pt_trader.py's _record_trade exactly (`if tag_u
      != "LTH":` gates its whole open_positions update) — this is what
      get_coin_mtm_pnl_series marks unrealized PnL against, matching how
      get_coin_realized_pnl already excludes LTH sells from realized PnL.
      Without this split, LTH buys would inflate the per-coin cost basis
      the bot never actually carries, and per-coin PnL would drift even
      further from the portfolio's total PnL.

    cost_<COIN>/bot_cost_<COIN> accumulate buys' gross notional_usd, not
    the fee-inclusive cash_delta — this matches pt_trader.py's own
    cost-basis ledger (confirmed against real trade records'
    position_cost_used_usd), which excludes buy-side fees from cost. One
    consequence: summing every coin's realized+unrealized PnL runs above
    the portfolio's true cash-based total PnL, by exactly the account's
    cumulative buy-side trading fees, inherited from how pt_trader.py
    itself has always computed realized_profit_usd.

    fee_pool_<COIN> / realized_fee_drag exist to locate exactly how much
    of that gap sits in still-open positions versus already-closed ones —
    the same average-cost accounting as cost_<COIN>, run on each buy's fee
    instead of its notional: a buy adds its own fee to fee_pool_<COIN>; a
    sell consumes a share of that coin's pool proportional to the fraction
    of the position being sold (via _apply_cost_basis, reused directly —
    its sell branch already computes exactly this fraction and ignores
    the 4th argument it's not using notional_usd for here), and whatever
    is consumed accumulates into realized_fee_drag. So at any point,
    fee_pool_<COIN> is the buy-side fee still embedded in that coin's open
    position (the portion inflating Floating PnL), and realized_fee_drag
    is the buy-side fee already consumed by past sells (the portion that
    inflated Realized PnL for trades already closed) — the two always sum
    to the account's cumulative buy-side fees paid so far."""
    ordered = sorted(deltas, key=lambda d: d.ts)
    cash = seed_cash
    realized_fee_drag = 0.0
    qty: dict[str, float] = {}
    cost: dict[str, float] = {}
    fee_pool: dict[str, float] = {}
    bot_qty: dict[str, float] = {}
    bot_cost: dict[str, float] = {}
    rows = [{"ts": seed_ts, "cash": cash, "realized_fee_drag": realized_fee_drag}]
    for d in ordered:
        cash += d.cash_delta
        prev_qty = qty.get(d.coin, 0.0)
        qty[d.coin] = prev_qty + d.qty_delta
        cost[d.coin] = _apply_cost_basis(cost.get(d.coin, 0.0), prev_qty, d.qty_delta, d.notional_usd)

        prev_fee_pool = fee_pool.get(d.coin, 0.0)
        fee_pool[d.coin] = _apply_cost_basis(prev_fee_pool, prev_qty, d.qty_delta, d.fees_usd)
        if d.qty_delta < 0:
            realized_fee_drag += prev_fee_pool - fee_pool[d.coin]

        if str(d.tag or "").upper() != "LTH":
            prev_bot_qty = bot_qty.get(d.coin, 0.0)
            bot_cost[d.coin] = _apply_cost_basis(bot_cost.get(d.coin, 0.0), prev_bot_qty, d.qty_delta, d.notional_usd)
            remaining_bot_qty = prev_bot_qty + d.qty_delta
            bot_qty[d.coin] = remaining_bot_qty if remaining_bot_qty > 1e-8 else 0.0

        rows.append({
            "ts": d.ts, "cash": cash, "realized_fee_drag": realized_fee_drag,
            **{f"qty_{c}": v for c, v in qty.items()},
            **{f"cost_{c}": v for c, v in cost.items()},
            **{f"fee_pool_{c}": v for c, v in fee_pool.items()},
            **{f"bot_qty_{c}": v for c, v in bot_qty.items()},
            **{f"bot_cost_{c}": v for c, v in bot_cost.items()},
        })
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
    points = [start_ts + i * step for i in range(max(n, 0))]
    # Always include end_ts itself as the final point. Without this, the
    # last regular grid point can be stale by up to one full bucket width
    # (e.g. up to 24h at tf=1day) relative to "now" -- which is exactly why
    # the chart's last point, build_account_summary's Total, and
    # validate_against_live's live comparison could each land on a
    # different moment in time and disagree. Forcing the same end_ts
    # instant as the final point everywhere is what keeps them consistent.
    if not points or points[-1] < end_ts:
        points.append(end_ts)
    return pd.Index(points, dtype="float64", name="ts")


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


def get_coin_mtm_pnl_series(
    ledger: pd.DataFrame,
    coin: str,
    price_df: pd.DataFrame,
    tf_minutes: int,
    start_ts: float,
    end_ts: float,
    trade_history_path: Path,
    entry_deltas: Optional[list[TradeDelta]] = None,
) -> tuple[pd.DataFrame, Optional[str]]:
    """Cumulative mark-to-market PnL(t) for one coin — realized PnL from
    every closed round trip up to t, PLUS the currently-open position's
    unrealized PnL at t (marked to price_df using the same cost-basis
    reconstruct_ledger tracks), if any. This is the "how has my PnL on
    this coin evolved" view, distinct from get_coin_value_series (raw
    position value, ignores cost basis) and get_coin_realized_pnl (a
    single now-snapshot, ignores any currently-open position).

    $ is a plain sum (realized-to-date + unrealized). % is additive across
    round trips (matching get_coin_realized_pnl's semantics — the user's
    own definition: 3 trades at 4.5/5.5/3.6% sum to 13.6%, not compounded),
    plus the open position's own current % on top, uncompounded with the
    rest. Unrealized PnL is marked against the bot-only qty/cost-basis
    (bot_qty_<COIN>/bot_cost_<COIN>, LTH excluded) — not the LTH-inclusive
    qty_<COIN> get_coin_value_series itself reports — so that summing this
    across every coin tracks the portfolio's total $ PnL closely (not
    exactly — see reconstruct_ledger's docstring for the small residual
    from buy-side fees excluded from cost basis); see
    reconstruct_ledger's docstring for why the two qty tracks exist.
    Returns (DataFrame[pnl_usd, pnl_pct], warning|None)."""
    value_out, warning = get_coin_value_series(
        ledger, coin, price_df, tf_minutes, start_ts, end_ts, entry_deltas
    )
    grid = value_out.index
    price = value_out["price"]

    bot_qty_col = f"bot_qty_{coin}"
    bot_qty_series = ledger[bot_qty_col] if bot_qty_col in ledger.columns else pd.Series(dtype=float)
    qty = _asof_into_grid(grid, bot_qty_series, fill=0.0)

    sells = [
        r for r in get_trade_history(trade_history_path, coin=coin)
        if r.get("side") == "sell" and str(r.get("tag") or "").upper() != "LTH"
    ]
    sells.sort(key=lambda r: utc_to_ts(r["ts"]))
    if sells:
        sell_ts = [utc_to_ts(r["ts"]) for r in sells]
        realized_usd_series = pd.Series(
            pd.Series([float(r.get("realized_profit_usd") or 0.0) for r in sells]).cumsum().values,
            index=sell_ts,
        )
        realized_pct_series = pd.Series(
            pd.Series([float(r.get("pnl_pct") or 0.0) for r in sells]).cumsum().values,
            index=sell_ts,
        )
    else:
        realized_usd_series = pd.Series(dtype=float)
        realized_pct_series = pd.Series(dtype=float)

    realized_usd = _asof_into_grid(grid, realized_usd_series, fill=0.0)
    realized_pct = _asof_into_grid(grid, realized_pct_series, fill=0.0)

    cost_col = f"bot_cost_{coin}"
    cost_series = ledger[cost_col] if cost_col in ledger.columns else pd.Series(dtype=float)
    cost = _asof_into_grid(grid, cost_series, fill=0.0)

    is_open = qty > 1e-12
    avg_cost_basis = (cost / qty).where(is_open, 0.0)
    unrealized_usd = (qty * (price - avg_cost_basis)).where(is_open, 0.0)
    unrealized_pct = (
        (price / avg_cost_basis - 1.0) * 100.0
    ).where(is_open & (avg_cost_basis > 0), 0.0)

    out = pd.DataFrame({
        "pnl_usd": realized_usd + unrealized_usd,
        "pnl_pct": realized_pct + unrealized_pct,
    }, index=grid)
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


# Bump whenever TradeDelta gains/renames/removes a field. A cache written
# under an older version is discarded and fully reparsed rather than being
# loaded via TradeDelta(**d) — which would otherwise silently fall back to
# a field's dataclass default (e.g. tag=None) for every already-cached
# delta, permanently misclassifying it (confirmed live: kraken's cache
# predated `tag` being tracked, so 12 of BTC's 14 real LTH-tagged buys were
# cached as tag=None and wrongly excluded from the LTH bucket).
_LEDGER_CACHE_SCHEMA_VERSION = 3


def _load_ledger_cache(cache_path: Path) -> tuple[int, list[TradeDelta]]:
    if not cache_path.exists():
        return 0, []
    try:
        data = json.loads(cache_path.read_text())
        if data.get("schema_version") != _LEDGER_CACHE_SCHEMA_VERSION:
            return 0, []
        deltas = [TradeDelta(**d) for d in data.get("deltas", [])]
        return int(data.get("byte_offset", 0)), deltas
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return 0, []


def _save_ledger_cache(cache_path: Path, byte_offset: int, deltas: list[TradeDelta]) -> None:
    tmp = cache_path.with_suffix(".tmp")
    payload = {
        "schema_version": _LEDGER_CACHE_SCHEMA_VERSION,
        "byte_offset": byte_offset,
        "deltas": [d.__dict__ for d in deltas],
    }
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


def _get_price_df_and_current(
    coin: str, deltas: list[TradeDelta], price_source: PriceSource, now_ts: float,
) -> tuple[pd.DataFrame, float]:
    """Latest 24h price series for a coin, plus its current price (last
    close, falling back to the coin's most recent trade price if the feed
    has nothing — e.g. a delisted/renamed pair). Shared by
    build_account_summary (which also needs the raw series for
    get_coin_mtm_pnl_series) and _build_account_breakdown_row (which only
    needs the scalar), so "current price for a coin" is defined exactly
    once."""
    price_df = get_price_series(coin, PRICE_FETCH_TF_MINUTES, now_ts - 86400, now_ts, price_source)
    if not price_df.empty:
        current_price = float(price_df["close"].iloc[-1])
    else:
        coin_deltas = [d for d in deltas if d.coin == coin]
        current_price = coin_deltas[-1].price if coin_deltas else 0.0
    return price_df, current_price


_price_source_cache: dict[str, PriceSource] = {}
_price_source_cache_lock = threading.Lock()


def _default_price_source(env) -> PriceSource:
    """ArcticPriceSource pointed at this deployment's actual KuCoin store
    (env.historic_data_dir, i.e. pt_config.json's kucoin_local_data_dir —
    the same store pt_trainer.py reads for training), not
    ArcticPriceSource's own generic ~/dev/data/arcticdb default, which is a
    different store and typically doesn't hold this project's candles.

    Cached module-level per store path: constructing ArcticPriceSource opens
    the LMDB store (adb.Arctic(...)), which is real per-call overhead if
    rebuilt on every request — this is called from every account-summary/
    account-history request, including the Accounts tab's 10s poll.

    Locked because callers run on separate asyncio.to_thread worker
    threads: an unlocked check-then-construct-then-store here let two
    threads both see an empty cache and each open their own Arctic/LMDB
    instance over the same path — LMDB explicitly documents that as
    unsupported (undefined behavior, since it's mmap-backed), and it's
    what produced this exact warning live: "LMDB path ... has already
    been opened in this process which is not supported by LMDB.\""""
    key = str(env.historic_data_dir)
    with _price_source_cache_lock:
        src = _price_source_cache.get(key)
        if src is None:
            src = ArcticPriceSource(arctic_url=f"lmdb:///{env.historic_data_dir}")
            _price_source_cache[key] = src
        return src


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

    # Clamped to seed_ts even when the caller asks for an earlier window (the
    # chart's pan/zoom prefetch pads blindly, with no notion of inception):
    # the bucket grid below fills any point before the ledger's first row
    # with $0 (nothing to asof backward onto), which drew an ugly flat-zero
    # lead-in before the account existed. Starting the grid at inception
    # instead means the series' first point is exactly the seed value, so %
    # mode's baseline-relative math (0% at that point) stays correct too.
    range_start = max(start_ts, seed_ts) if start_ts is not None else seed_ts
    range_end = end_ts if end_ts is not None else pd.Timestamp.utcnow().timestamp()

    coins = [c[4:] for c in ledger.columns if c.startswith("qty_")]
    # Deliberately don't pass range_start here: get_coin_value_series needs
    # each coin's FULL available candle history (not just the requested
    # display window) to correctly tell "no candles before this trade" apart
    # from "we just didn't fetch that far back". _bucket_grid/_asof_into_grid
    # already scope the final output to [range_start, range_end] regardless.
    price_sources = {
        c: get_price_series(c, PRICE_FETCH_TF_MINUTES, price_source=price_source)
        for c in coins
    }

    if coin is not None:
        # Per-coin: mark-to-market PnL (realized-to-date + unrealized on any
        # currently-open position), not raw position value. $ and % are each
        # independently meaningful here (% is an additive sum across round
        # trips, not a baseline-relative fraction of $) so both are returned
        # per point rather than one value + a client-side baseline division.
        pnl_out, w = get_coin_mtm_pnl_series(
            ledger, coin, price_sources.get(coin, pd.DataFrame()),
            tf_minutes, range_start, range_end, env.trade_history_path(xk), deltas,
        )
        warning = {"message": w} if w else None
        points = [
            {"ts": int(ts), "value": float(row["pnl_usd"]), "pct": float(row["pnl_pct"])}
            for ts, row in pnl_out.iterrows() if pd.notna(row["pnl_usd"])
        ]
        return {"points": points, "baseline": None, "warning": warning}

    total_out, warnings_list = get_total_value_series(
        ledger, price_sources, tf_minutes, range_start, range_end, deltas
    )
    baseline = seed_cash
    value = total_out["total_account_value"]
    warning = {"message": "; ".join(warnings_list)} if warnings_list else None

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


def build_static_hold_series(
    env,
    mirror_xk: str,
    coin: str = "BTC",
    tf_minutes: int = 1440,
    start_ts: Optional[float] = None,
    end_ts: Optional[float] = None,
    price_source: Optional[PriceSource] = None,
) -> dict:
    """Synthetic 'bought `coin` at mirror_xk's inception and held' benchmark
    — same starting capital and start time as mirror_xk (its own seed), no
    trade ledger of its own: value(t) = seed_cash * close(t) / close(seed_ts).
    Same return shape as build_account_series's coin=None case, so the
    chart can treat it as just another total-value series:
    {"points": [...], "baseline": float, "warning": {...}|None}."""
    price_source = price_source or _default_price_source(env)
    seed = _read_seed(env.account_history_path(mirror_xk))
    if seed is None:
        return {"points": [], "baseline": None, "warning": {"message": f"No account_value_history seed for {mirror_xk}"}}
    seed_ts, seed_cash = seed

    price_df = get_price_series(coin, PRICE_FETCH_TF_MINUTES, price_source=price_source)
    if price_df.empty:
        return {"points": [], "baseline": seed_cash, "warning": {"message": f"No price history for {coin}"}}
    close = price_df["close"]

    # Entry price at seed_ts, asof-backward like every other lookup in this
    # module. If mirror_xk's inception predates the earliest available
    # candle, fall back to that earliest close instead of NaN — same
    # "gap filled, not silently dropped" approach get_coin_value_series
    # uses for a coin traded before its first candle.
    seed_grid = pd.Index([seed_ts], dtype="float64", name="ts")
    entry_price = _asof_into_grid(seed_grid, close, fill=float("nan")).iloc[0]
    warning = None
    if pd.isna(entry_price):
        entry_price = float(close.iloc[0])
        warning = f"{coin} candle history starts after {mirror_xk}'s inception; using earliest available price"
    else:
        entry_price = float(entry_price)
    if not entry_price:
        return {"points": [], "baseline": seed_cash, "warning": {"message": f"No usable price for {coin}"}}

    range_start = max(start_ts, seed_ts) if start_ts is not None else seed_ts
    range_end = end_ts if end_ts is not None else pd.Timestamp.utcnow().timestamp()
    grid = _bucket_grid(range_start, range_end, tf_minutes)
    price = _asof_into_grid(grid, close, fill=float("nan")).ffill().fillna(entry_price)

    value = seed_cash * price / entry_price
    points = [{"ts": int(ts), "value": float(v)} for ts, v in value.items() if pd.notna(v)]
    return {"points": points, "baseline": seed_cash, "warning": {"message": warning} if warning else None}


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
    """{"total": {value, pct}, "coins": {SYM: {value, pct, trade_count,
    fees_paid}, ...}, "fees_paid": float} — latest point only, cheap. Powers
    the Accounts-tab table. Both fees_paid fields are all-time cumulative;
    the top-level one is the whole account (see get_total_fees_paid), the
    per-coin one is scoped to that coin (see get_coin_fees_paid).

    Total reflects current account value (cash + open holdings marked to
    market), same concept as the portfolio chart. Per-coin reflects
    mark-to-market PnL: cumulative realized PnL across closed round-trip
    sells, PLUS the currently-open position's unrealized PnL if any — the
    same "now" point as get_coin_mtm_pnl_series's chart series, not a
    separately computed snapshot. pct is None only when there's neither a
    closed round trip nor an open position (nothing to report yet)."""
    price_source = _default_price_source(env)
    seed = _read_seed(env.account_history_path(xk))
    if seed is None:
        return {"total": None, "coins": {}}
    seed_ts, seed_cash = seed

    trade_history_path = env.trade_history_path(xk)
    cache_path = env.hub_data_xk_dir(xk) / "account_ledger_cache.json"
    deltas = _get_all_deltas(trade_history_path, cache_path)
    ledger = reconstruct_ledger(seed_ts, seed_cash, deltas)

    now_ts = pd.Timestamp.utcnow().timestamp()
    coins = [c[4:] for c in ledger.columns if c.startswith("qty_")]

    result_coins = {}
    holdings_total = 0.0
    for coin in coins:
        qty_series = get_coin_position_series(ledger, coin)
        qty = float(qty_series.iloc[-1]) if not qty_series.empty else 0.0

        # Current notional still needed for the Total row below, even though
        # the per-coin row itself shows mark-to-market PnL, not this.
        price_df, current_price = _get_price_df_and_current(coin, deltas, price_source, now_ts)
        holdings_total += qty * current_price

        # Single "now" point of the same mark-to-market series the per-coin
        # chart shows — deliberately the same function, not a separately
        # hand-rolled "current" computation, so the table and the chart can
        # never drift apart the way Total's two paths once did.
        mtm_out, _ = get_coin_mtm_pnl_series(
            ledger, coin, price_df, tf_minutes=1, start_ts=now_ts, end_ts=now_ts,
            trade_history_path=trade_history_path, entry_deltas=deltas,
        )
        trade_count = get_coin_realized_pnl(trade_history_path, coin)["trade_count"]
        result_coins[coin] = {
            "value": float(mtm_out["pnl_usd"].iloc[-1]),
            "pct": float(mtm_out["pnl_pct"].iloc[-1]) if (trade_count > 0 or qty > 1e-12) else None,
            "trade_count": trade_count,
            "fees_paid": get_coin_fees_paid(trade_history_path, coin),
        }

    cash = float(ledger["cash"].iloc[-1]) if not ledger.empty else seed_cash
    total_value = cash + holdings_total
    total_pct = float(get_pct_series(pd.Series([total_value]), seed_cash).iloc[0])

    fees_paid = get_total_fees_paid(trade_history_path)

    return {
        "total": {"value": total_value, "pct": total_pct},
        "coins": result_coins,
        "fees_paid": fees_paid,
    }


def _build_account_breakdown_row(env, xk: str) -> Optional[dict]:
    """One exchange's current TOTAL, decomposed two ways:
      Balance sheet: Cash + Holdings (Tradable) + Holdings (LTH) = TOTAL
      Attribution:   Seed + Realized PnL + Floating PnL + Rounding = TOTAL

    Both sides reconcile to TOTAL exactly, by construction. Realized PnL
    and Floating PnL are both fully fee-adjusted, not pt_trader.py's raw
    cost-basis figures (which exclude buy-side fees from cost — see
    reconstruct_ledger's docstring): each is reduced by the buy-side fees
    embedded in it, via reconstruct_ledger's fee_pool_<COIN>/
    realized_fee_drag tracking, which knows exactly how much of the
    account's cumulative buy-side fees sits in still-open positions
    (deducted from Floating PnL) versus already-closed ones (deducted
    from Realized PnL). Sell-side fees need no equivalent adjustment here:
    they're already subtracted from proceeds before realized_profit_usd
    is computed (pt_trader.py's _record_trade), so Realized PnL only ever
    needed the buy-side correction to be fully fee-accurate. A currently
    open position's eventual sell fee is unknown until it actually sells,
    at which point it flows into Realized PnL the normal way — Floating
    PnL was never adjusted for it, since there's nothing yet to adjust.

    Rounding is whatever's left: TOTAL − (Seed + Realized + Floating).
    Since both PnL figures are now exact (not estimates), Rounding is
    exact too — a genuine reconciliation residual unrelated to fees, most
    likely accumulated floating-point precision effects across many
    trades rather than one identifiable source.

    Buy Fees Paid and Sell Fees Paid are shown separately as reference
    figures only — both are already folded into Realized/Floating PnL
    above (buy fees via the adjustment described above, sell fees via
    realized_profit_usd itself), so they don't participate in the
    reconciliation arithmetic a second time. Fees Paid is just their sum.
    For a zero-fee account (e.g. shadow) every fee-related figure reduces
    to ~0.

    %Total is the account's overall return since inception, relative to
    Seed: (TOTAL − Seed) / Seed × 100 (None if Seed is 0). Total (check)
    re-derives TOTAL from this row's own Attribution fields (Seed +
    Realized + Floating + Rounding) — always equal to TOTAL by
    construction, kept as a literal second calculation (not just TOTAL
    displayed twice) so a future bug in either path would actually be
    caught by comparing them. Every derived figure here is computed once,
    server-side — callers (the API layer, the web frontend) must never
    recompute any of it independently, or the same number can end up
    wrong in one place and right in another.

    Returns None if the account has no seed yet (nothing traded)."""
    price_source = _default_price_source(env)
    seed = _read_seed(env.account_history_path(xk))
    if seed is None:
        return None
    seed_ts, seed_cash = seed

    trade_history_path = env.trade_history_path(xk)
    cache_path = env.hub_data_xk_dir(xk) / "account_ledger_cache.json"
    deltas = _get_all_deltas(trade_history_path, cache_path)
    ledger = reconstruct_ledger(seed_ts, seed_cash, deltas)

    now_ts = pd.Timestamp.utcnow().timestamp()
    coins = [c[4:] for c in ledger.columns if c.startswith("qty_")]

    holdings_tradable = 0.0
    holdings_lth = 0.0
    floating_pnl_raw = 0.0
    floating_fee_drag = 0.0
    for coin in coins:
        qty = float(ledger[f"qty_{coin}"].iloc[-1])
        cost = float(ledger[f"cost_{coin}"].iloc[-1])
        bot_qty = float(ledger.get(f"bot_qty_{coin}", pd.Series([0.0])).iloc[-1])
        fee_pool = float(ledger.get(f"fee_pool_{coin}", pd.Series([0.0])).iloc[-1])

        _, price = _get_price_df_and_current(coin, deltas, price_source, now_ts)

        holdings_tradable += bot_qty * price
        holdings_lth += (qty - bot_qty) * price
        floating_pnl_raw += qty * price - cost
        floating_fee_drag += fee_pool

    cash = float(ledger["cash"].iloc[-1]) if not ledger.empty else seed_cash
    total = cash + holdings_tradable + holdings_lth

    all_trades = get_trade_history(trade_history_path)
    realized_pnl_raw = sum(
        float(row.get("realized_profit_usd") or 0.0)
        for row in all_trades if row.get("side") == "sell"
    )
    realized_fee_drag = float(ledger["realized_fee_drag"].iloc[-1]) if not ledger.empty else 0.0

    # Both fully fee-adjusted: Realized PnL nets out the buy-side fees on
    # lots that have actually sold (sell-side fees are already netted into
    # realized_profit_usd itself); Floating PnL nets out the buy-side fees
    # still sitting in open positions — see this function's docstring.
    realized_pnl = realized_pnl_raw - realized_fee_drag
    floating_pnl = floating_pnl_raw - floating_fee_drag
    rounding = total - (seed_cash + realized_pnl + floating_pnl)

    buy_fees_paid = sum(
        float(row.get("fees_usd") or 0.0)
        for row in all_trades if row.get("side") == "buy"
    )
    # fees_fallback_applied_usd only ever applies to sells (pt_trader.py's
    # _record_trade gates it on side == "sell"), so it's omitted for buys
    # above but included here — this is the same total get_total_fees_paid
    # would report for sells, just isolated to one side.
    sell_fees_paid = sum(
        float(row.get("fees_usd") or 0.0) + float(row.get("fees_fallback_applied_usd") or 0.0)
        for row in all_trades if row.get("side") == "sell"
    )

    # %Total: overall return since inception, relative to Seed.
    pct_total = ((total - seed_cash) / seed_cash * 100) if seed_cash > 0 else None
    # Recomputed independently from this row's own Attribution fields —
    # always equals TOTAL by construction (Rounding is defined precisely
    # to make that true), but computing it via the same formula here
    # rather than just re-displaying TOTAL keeps it a genuine cross-check
    # against a future bug, not just a relabeled duplicate.
    total_check = seed_cash + realized_pnl + floating_pnl + rounding

    return {
        "Cash": cash,
        "Holdings (Tradable)": holdings_tradable,
        "Holdings (LTH)": holdings_lth,
        "TOTAL": total,
        "%Total": pct_total,
        "Seed": seed_cash,
        "Realized PnL": realized_pnl,
        "Floating PnL": floating_pnl,
        "Rounding": rounding,
        "Total (check)": total_check,
        "Buy Fees Paid": buy_fees_paid,
        "Sell Fees Paid": sell_fees_paid,
        "Fees Paid": buy_fees_paid + sell_fees_paid,
    }


_ACCOUNT_BREAKDOWN_ROWS = [
    "Cash", "Holdings (Tradable)", "Holdings (LTH)", "TOTAL", "%Total",
    "Seed", "Realized PnL", "Floating PnL", "Rounding", "Total (check)",
    "Buy Fees Paid", "Sell Fees Paid", "Fees Paid",
]


def build_account_breakdown_table(env, xks: list[str]) -> pd.DataFrame:
    """Kraken-vs-shadow (or any set of accounts) breakdown of current total
    account value — see _build_account_breakdown_row's docstring for the
    two decompositions (both reconcile to TOTAL exactly; Realized PnL and
    Floating PnL are both fully fee-adjusted, not raw cost-basis figures,
    and Rounding is an exact residual, not an estimate — see that
    docstring for how). Rows: Cash, Holdings (Tradable), Holdings (LTH),
    TOTAL, %Total, Seed, Realized PnL, Floating PnL, Rounding, Total
    (check), Buy Fees Paid, Sell Fees Paid, Fees Paid. Every derived
    figure — %Total, the Total (check) cross-check, and the combined
    Fees Paid — is computed here, not by callers, so a REPL/notebook use
    and the web layer can never compute it two different ways. One
    column per xk; an un-seeded account gets an all-NaN column rather
    than being dropped, so callers can rely on every requested xk
    appearing.

    Handy standalone (prints cleanly in a REPL/notebook) as well as being
    what the Accounts tab's totals table is built from.

    Shadow never receives LTH-tagged trades (ShadowedExchange.place_buy
    skips mirroring them entirely), so Holdings (LTH) — and any LTH
    contribution to Realized/Floating PnL — is always exactly 0 for shadow
    by construction, not a data gap."""
    columns = {}
    for xk in xks:
        row = _build_account_breakdown_row(env, xk)
        columns[xk] = pd.Series(row, index=_ACCOUNT_BREAKDOWN_ROWS) if row else pd.Series(index=_ACCOUNT_BREAKDOWN_ROWS, dtype=float)
    return pd.DataFrame(columns)


def account_total_delta(df: pd.DataFrame, xks: list[str]) -> Optional[float]:
    """TOTAL of the second account minus TOTAL of the first, from a
    build_account_breakdown_table DataFrame — the topbar's "Δ" column.
    None if fewer than two xks are given, or either TOTAL is NaN
    (un-seeded account). Exists so this comparison is computed once,
    here, rather than in the web layer — same reasoning as every other
    derived figure in this module."""
    if len(xks) < 2:
        return None
    t0, t1 = df[xks[0]]["TOTAL"], df[xks[1]]["TOTAL"]
    if pd.isna(t0) or pd.isna(t1):
        return None
    return float(t1 - t0)
