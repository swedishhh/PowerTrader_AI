"""
Live mid-price fetcher for the paper/demo adapter.

Uses public REST APIs only — no credentials required.
Source is selected via the `live_price_source` config key ("kraken" or "kucoin").
"""

from __future__ import annotations

import time
from typing import Optional, Tuple

import pt_errors

_last_emit_ts: dict = {}
_EMIT_THROTTLE_SECONDS = 600.0


def _throttled_emit(key: str, level: str, message: str, detail: str) -> None:
    now = time.time()
    if now - _last_emit_ts.get(key, 0.0) < _EMIT_THROTTLE_SECONDS:
        return
    _last_emit_ts[key] = now
    pt_errors.emit("price-source", level=level, message=message, detail=detail)


# Kept short: get_mid_price() runs synchronously in per-coin, per-tick loops
# (e.g. the demo/shadow trading loop), so each source gets only a brief,
# backed-off retry budget to smooth over a quick blip — not enough to stall
# that loop badly if a source is genuinely down (the two-source fallback in
# get_mid_price(), not a long retry here, is what handles a real outage).
_RETRY_TIMEOUT_SECONDS = 3.0


def _kraken_mid_price(base: str) -> Tuple[Optional[float], Optional[str]]:
    def _fetch() -> float:
        import ccxt
        _kraken = ccxt.kraken({"enableRateLimit": True})
        ticker = _kraken.fetch_ticker(f"{base}/USDT")
        bid = float(ticker.get("bid", 0) or 0)
        ask = float(ticker.get("ask", 0) or 0)
        if bid <= 0 or ask <= 0:
            raise ValueError("empty bid/ask from Kraken")
        return (bid + ask) / 2.0

    try:
        return pt_errors.retry(_fetch, timeout=_RETRY_TIMEOUT_SECONDS, start_interval=0.5, interval_ramp=2.0), None
    except Exception as e:
        return None, f"Kraken error: {e}"


def _kucoin_mid_price(base: str) -> Tuple[Optional[float], Optional[str]]:
    def _fetch() -> float:
        from kucoin.client import Market
        market = Market(url="https://api.kucoin.com")
        data = market.get_kline(f"{base}-USDT", "1min")
        if not data or len(data) == 0 or len(data[0]) < 3:
            raise ValueError("empty kline response from KuCoin")
        return float(data[0][2])

    try:
        return pt_errors.retry(_fetch, timeout=_RETRY_TIMEOUT_SECONDS, start_interval=0.5, interval_ramp=2.0), None
    except Exception as e:
        return None, f"KuCoin error: {e}"


def get_mid_price(base: str, source: str) -> Optional[float]:
    """Return mid-price for *base* (e.g. "BTC") using the configured source.

    Falls back to the other source on failure. Only surfaces an error to the
    UI when BOTH sources fail — a single-source hiccup with a working
    fallback isn't actionable, and this is called often enough (every price
    refresh, for every coin) that emitting on every partial failure would
    flood the Errors tab.
    """
    primary, secondary = (
        (_kucoin_mid_price, _kraken_mid_price)
        if source == "kucoin"
        else (_kraken_mid_price, _kucoin_mid_price)
    )
    price, err1 = primary(base)
    if price:
        return price
    price, err2 = secondary(base)
    if price:
        return price

    _throttled_emit(
        f"price:{base}",
        level="warning",
        message=f"No live price available for {base} from either source",
        detail=(
            f"Primary/fallback both failed — {err1 or 'no data'}; {err2 or 'no data'}. "
            f"Anything pricing {base} this cycle (shadow account, demo trading) will "
            "treat it as unavailable. Will retry automatically on the next call; "
            "this warning is throttled to once per 10 min per coin."
        ),
    )
    return None
