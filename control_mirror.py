"""
ControlMirror: records synthetic zero-fee trades on the control exchange,
driven synchronously by Kraken fills.

Design:
  - BUY:  same USD notional as the Kraken fill, but at KuCoin mid-price → different qty.
  - SELL: liquidates the entire control position for that coin at KuCoin mid-price.
  - Zero fees, zero spread (mid-price only).
  - Writes to control state files: exchange_state, pnl_ledger, trade_history, account_value_history.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Optional


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


class ControlMirror:

    def __init__(self, hub_data_dir: str):
        self._hub = Path(hub_data_dir)
        self._hub.mkdir(parents=True, exist_ok=True)

        self._state_path = self._hub / "control_exchange_state.json"
        self._ledger_path = self._hub / "pnl_ledger_control.json"
        self._history_path = self._hub / "trade_history_control.jsonl"
        self._acct_history_path = self._hub / "account_value_history_control.jsonl"
        self._status_path = self._hub / "trader_status_control.json"

        self._state = self._load_json(self._state_path) or {
            "usd_balance": 0.0,
            "holdings": {},
            "orders": {},
        }
        self._ledger = self._load_json(self._ledger_path) or {
            "total_realized_profit_usd": 0.0,
            "open_positions": {},
            "pending_orders": {},
            "lth_profit_bucket_usd": 0.0,
            "lth_last_buy": None,
        }

    def reload(self) -> None:
        """Re-read state from disk (call after external reset)."""
        self._state = self._load_json(self._state_path) or {
            "usd_balance": 0.0, "holdings": {}, "orders": {},
        }
        self._ledger = self._load_json(self._ledger_path) or {
            "total_realized_profit_usd": 0.0, "open_positions": {},
            "pending_orders": {}, "lth_profit_bucket_usd": 0.0, "lth_last_buy": None,
        }

    def _load_json(self, path: Path) -> Optional[dict]:
        try:
            if path.exists():
                return json.loads(path.read_text())
        except Exception:
            pass
        return None

    def _save_state(self) -> None:
        self._atomic_write(self._state_path, self._state)

    def _save_ledger(self) -> None:
        self._ledger["last_updated_ts"] = time.time()
        self._atomic_write(self._ledger_path, self._ledger)

    def _atomic_write(self, path: Path, data: dict) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(data, indent=2))
            tmp.replace(path)
        except Exception:
            pass

    def _append_history(self, entry: dict) -> None:
        try:
            with open(self._history_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception:
            pass

    def _append_account_value(self, total: float) -> None:
        try:
            with open(self._acct_history_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": time.time(), "total_account_value": total}) + "\n")
        except Exception:
            pass

    def _get_account_value(self) -> float:
        total = float(self._state.get("usd_balance", 0))
        ok = True
        for base, qty in (self._state.get("holdings") or {}).items():
            if qty <= 0:
                continue
            price = _kucoin_mid_price(base)
            if price and price > 0:
                total += qty * price
            else:
                ok = False
        if not ok and hasattr(self, "_last_good_account_value"):
            return self._last_good_account_value
        if ok:
            self._last_good_account_value = total
        return total

    def mirror_buy(self, coin: str, notional_usd: float,
                   tag: Optional[str] = None) -> bool:
        """Mirror a Kraken buy: spend the same USD notional at KuCoin mid-price."""
        price = _kucoin_mid_price(coin)
        if not price or price <= 0:
            print(f"[ControlMirror] No KuCoin price for {coin}, skipping mirror buy")
            return False

        qty = notional_usd / price

        holdings = self._state.setdefault("holdings", {})
        holdings[coin] = float(holdings.get(coin, 0)) + qty

        self._state["usd_balance"] = float(self._state.get("usd_balance", 0)) - notional_usd

        order_id = str(uuid.uuid4())
        symbol = f"{coin}_USD"
        order_rec = {
            "id": order_id, "side": "buy", "symbol": symbol,
            "state": "filled", "qty": qty, "price": price,
            "notional": notional_usd, "fees": 0.0, "ts": time.time(),
        }
        self._state.setdefault("orders", {}).setdefault(symbol, []).append(order_rec)
        self._save_state()

        open_pos = self._ledger.setdefault("open_positions", {})
        pos = open_pos.get(coin)
        if not isinstance(pos, dict):
            pos = {"usd_cost": 0.0, "qty": 0.0}
            open_pos[coin] = pos
        pos["usd_cost"] = float(pos.get("usd_cost", 0)) + notional_usd
        pos["qty"] = float(pos.get("qty", 0)) + qty
        self._save_ledger()

        ts = time.time()
        self._append_history({
            "ts": ts, "side": "buy", "tag": tag or "",
            "symbol": symbol, "qty": qty, "price": price,
            "notional_usd": notional_usd, "net_usd": -notional_usd,
            "avg_cost_basis": None, "pnl_pct": None,
            "fees_usd": 0.0, "fees_missing": False,
            "order_id": order_id, "exchange": "control",
        })

        self._append_account_value(self._get_account_value())

        print(f"[ControlMirror] BUY {coin}: ${notional_usd:.2f} @ {price:.6g} "
              f"→ {qty:.8g} units (tag={tag})")
        return True

    def mirror_sell(self, coin: str, tag: Optional[str] = None) -> bool:
        """Mirror a Kraken sell: liquidate the entire control position at KuCoin mid."""
        holdings = self._state.get("holdings") or {}
        qty = float(holdings.get(coin, 0))
        if qty <= 1e-12:
            print(f"[ControlMirror] No {coin} holdings to sell")
            return False

        price = _kucoin_mid_price(coin)
        if not price or price <= 0:
            print(f"[ControlMirror] No KuCoin price for {coin}, skipping mirror sell")
            return False

        proceeds = qty * price

        self._state["holdings"].pop(coin, None)
        self._state["usd_balance"] = float(self._state.get("usd_balance", 0)) + proceeds
        symbol = f"{coin}_USD"
        order_id = str(uuid.uuid4())
        order_rec = {
            "id": order_id, "side": "sell", "symbol": symbol,
            "state": "filled", "qty": qty, "price": price,
            "notional": proceeds, "fees": 0.0, "ts": time.time(),
        }
        self._state.setdefault("orders", {}).setdefault(symbol, []).append(order_rec)
        self._save_state()

        open_pos = self._ledger.setdefault("open_positions", {})
        pos = open_pos.get(coin)
        pos_cost = float(pos.get("usd_cost", 0)) if isinstance(pos, dict) else 0.0
        realized = proceeds - pos_cost
        pnl_pct = (realized / pos_cost * 100) if pos_cost > 0 else None
        avg_cost = pos_cost / qty if qty > 0 else None

        open_pos.pop(coin, None)
        self._ledger["total_realized_profit_usd"] = float(
            self._ledger.get("total_realized_profit_usd", 0)
        ) + realized
        self._save_ledger()

        ts = time.time()
        self._append_history({
            "ts": ts, "side": "sell", "tag": tag or "",
            "symbol": symbol, "qty": qty, "price": price,
            "notional_usd": proceeds, "net_usd": proceeds,
            "avg_cost_basis": avg_cost, "pnl_pct": pnl_pct,
            "fees_usd": 0.0, "fees_missing": False,
            "order_id": order_id, "exchange": "control",
        })

        self._append_account_value(self._get_account_value())

        pnl_str = f"{realized:+.2f}" if realized is not None else "?"
        print(f"[ControlMirror] SELL {coin}: {qty:.8g} @ {price:.6g} "
              f"→ ${proceeds:.2f} (PnL {pnl_str}, tag={tag})")
        return True

    def get_account_value(self) -> float:
        return self._get_account_value()

    def append_account_value(self, total: float) -> None:
        self._append_account_value(total)

    def write_status(self) -> None:
        """Write a trader_status_control.json from current control state."""
        ts = time.time()
        total = self._get_account_value()

        positions = {}
        for coin, qty in (self._state.get("holdings") or {}).items():
            if qty <= 1e-12:
                continue
            price = _kucoin_mid_price(coin)
            if not price or price <= 0:
                continue
            pos = (self._ledger.get("open_positions") or {}).get(coin, {})
            pos_cost = float(pos.get("usd_cost", 0)) if isinstance(pos, dict) else 0.0
            pos_qty = float(pos.get("qty", 0)) if isinstance(pos, dict) else qty
            avg_cost = pos_cost / pos_qty if pos_qty > 0 else 0.0
            value = qty * price
            pnl_pct = ((price - avg_cost) / avg_cost * 100) if avg_cost > 0 else 0.0
            positions[coin] = {
                "quantity": qty,
                "avg_cost_basis": avg_cost,
                "current_buy_price": price,
                "current_sell_price": price,
                "value_usd": value,
                "gain_loss_pct_buy": pnl_pct,
                "gain_loss_pct_sell": pnl_pct,
                "dca_triggered_stages": 0,
                "trail_active": False,
                "trail_line": 0.0,
            }

        status = {
            "timestamp": ts,
            "account": {
                "total_account_value": total,
                "buying_power": float(self._state.get("usd_balance", 0)),
                "holdings_sell_value": total - float(self._state.get("usd_balance", 0)),
                "holdings_buy_value": total - float(self._state.get("usd_balance", 0)),
                "percent_in_trade": (1 - float(self._state.get("usd_balance", 0)) / total) * 100 if total > 0 else 0,
                "pm_start_pct_no_dca": 0.0,
                "pm_start_pct_with_dca": 0.0,
                "trailing_gap_pct": 0.0,
            },
            "positions": positions,
        }
        self._atomic_write(self._status_path, status)
