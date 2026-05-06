"""
Binance exchange adapter (stub).

To implement: install ccxt (`pip install ccxt`) and fill in the methods below.
API key and secret are read from gui_settings.json fields:
  binance_api_key, binance_api_secret
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from exchange_api import ExchangeAdapter, OrderResult


class BinanceAdapter(ExchangeAdapter):

    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret

    def to_exchange_symbol(self, canonical: str) -> str:
        # Binance uses BTCUSDT format
        base, quote = canonical.split("_")
        return f"{base}{quote}T" if quote == "USD" else f"{base}{quote}"

    def to_canonical_symbol(self, exchange_sym: str) -> str:
        if exchange_sym.endswith("USDT"):
            return exchange_sym[:-4] + "_USD"
        return exchange_sym

    def get_account_value(self) -> Optional[float]:
        raise NotImplementedError("Binance adapter not yet implemented")

    def get_buying_power(self) -> Optional[float]:
        raise NotImplementedError("Binance adapter not yet implemented")

    def get_holdings(self) -> Dict[str, float]:
        raise NotImplementedError("Binance adapter not yet implemented")

    def get_price(self, symbols: List[str]) -> Tuple[Dict[str, float], Dict[str, float], List[str]]:
        raise NotImplementedError("Binance adapter not yet implemented")

    def place_buy(self, symbol: str, amount_usd: float) -> Optional[OrderResult]:
        raise NotImplementedError("Binance adapter not yet implemented")

    def place_sell(self, symbol: str, qty: float) -> Optional[OrderResult]:
        raise NotImplementedError("Binance adapter not yet implemented")

    def get_orders(self, symbol: str) -> dict:
        raise NotImplementedError("Binance adapter not yet implemented")


def create_adapter() -> BinanceAdapter:
    import json
    import os

    base_dir = os.path.dirname(os.path.abspath(__file__))
    keys_path = os.path.join(base_dir, "binance_api_keys.json")
    api_key = ""
    api_secret = ""
    try:
        if os.path.isfile(keys_path):
            with open(keys_path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            api_key = str(data.get("api_key", "")).strip()
            api_secret = str(data.get("api_secret", "")).strip()
    except Exception:
        pass

    return BinanceAdapter(api_key=api_key, api_secret=api_secret)
