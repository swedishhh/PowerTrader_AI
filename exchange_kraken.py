"""
Kraken exchange adapter (stub).

To implement: install ccxt (`pip install ccxt`) and fill in the methods below.
API key and secret are read from gui_settings.json fields:
  kraken_api_key, kraken_api_secret
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from exchange_api import ExchangeAdapter, OrderResult


class KrakenAdapter(ExchangeAdapter):

    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret

    def to_exchange_symbol(self, canonical: str) -> str:
        # Kraken uses XBT for BTC, and slash-separated pairs
        base, quote = canonical.split("_")
        if base == "BTC":
            base = "XBT"
        return f"{base}/{quote}"

    def to_canonical_symbol(self, exchange_sym: str) -> str:
        base, quote = exchange_sym.split("/")
        if base == "XBT":
            base = "BTC"
        return f"{base}_{quote}"

    def get_account_value(self) -> Optional[float]:
        raise NotImplementedError("Kraken adapter not yet implemented")

    def get_buying_power(self) -> Optional[float]:
        raise NotImplementedError("Kraken adapter not yet implemented")

    def get_holdings(self) -> Dict[str, float]:
        raise NotImplementedError("Kraken adapter not yet implemented")

    def get_price(self, symbols: List[str]) -> Tuple[Dict[str, float], Dict[str, float], List[str]]:
        raise NotImplementedError("Kraken adapter not yet implemented")

    def place_buy(self, symbol: str, amount_usd: float) -> Optional[OrderResult]:
        raise NotImplementedError("Kraken adapter not yet implemented")

    def place_sell(self, symbol: str, qty: float) -> Optional[OrderResult]:
        raise NotImplementedError("Kraken adapter not yet implemented")

    def get_orders(self, symbol: str) -> dict:
        raise NotImplementedError("Kraken adapter not yet implemented")


def create_adapter() -> KrakenAdapter:
    import json
    import os

    settings_path = os.environ.get("POWERTRADER_GUI_SETTINGS") or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "gui_settings.json"
    )
    api_key = "/unqGUatSqD1e4qWVNWEIjSceH7Ewr3FnaJqwETrNAfzscLURgFwnwwf"
    api_secret = "yP+T51E88SLn6bsfsb0z/jas0zYGPxTPLpUWZqM0Er32uA42iE2eOvBWQJxuFfCzM0JNc1NMrgLm+ATqwyGMsQ=="
    try:
        if os.path.isfile(settings_path):
            with open(settings_path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            api_key = str(data.get("kraken_api_key", "")).strip()
            api_secret = str(data.get("kraken_api_secret", "")).strip()
    except Exception:
        pass

    return KrakenAdapter(api_key=api_key, api_secret=api_secret)
