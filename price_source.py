"""
Live mid-price fetcher for the paper/demo adapter.

Uses public REST APIs only — no credentials required.
Source is selected via the `live_price_source` config key ("kraken" or "kucoin").
"""

from __future__ import annotations

from typing import Optional


def _kraken_mid_price(base: str) -> Optional[float]:
    try:
        import ccxt
        _kraken = ccxt.kraken({"enableRateLimit": True})
        ticker = _kraken.fetch_ticker(f"{base}/USDT")
        bid = float(ticker.get("bid", 0) or 0)
        ask = float(ticker.get("ask", 0) or 0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
    except Exception:
        pass
    return None


def _kucoin_mid_price(base: str) -> Optional[float]:
    try:
        from kucoin.client import Market
        market = Market(url="https://api.kucoin.com")
        data = market.get_kline(f"{base}-USDT", "1min")
        if data and len(data) > 0 and len(data[0]) >= 3:
            return float(data[0][2])
    except Exception:
        pass
    return None


def get_mid_price(base: str, source: str) -> Optional[float]:
    """Return mid-price for *base* (e.g. "BTC") using the configured source.

    Falls back to the other source on failure.
    """
    if source == "kucoin":
        return _kucoin_mid_price(base) or _kraken_mid_price(base)
    return _kraken_mid_price(base) or _kucoin_mid_price(base)
