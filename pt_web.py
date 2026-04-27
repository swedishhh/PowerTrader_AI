"""FastAPI web server for PowerTrader_AI.

Run:  python pt_web.py [--port 8080]

Provides REST API + WebSocket for real-time updates, serving the web/ frontend.
Supports multiple exchanges running simultaneously (e.g. control + kraken).
"""

import asyncio
import json
import time
import argparse
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from pt_env import PTEnv
from pt_models import CoinModel, AccountModel, SystemModel
from pt_controller import ProcessController
from control_mirror import ControlMirror

PROJECT_DIR = Path(__file__).resolve().parent
WEB_DIR = PROJECT_DIR / "web"

env = PTEnv(project_dir=PROJECT_DIR)
ctrl = ProcessController(env)
app = FastAPI(title="PowerTrader Web")


def _get_mirror() -> ControlMirror:
    return ControlMirror(str(env.hub_data_dir))

try:
    _get_mirror().write_status()
except Exception:
    pass

_adapters: dict = {}


def _get_adapter(xk: str):
    if xk not in _adapters:
        try:
            if xk == "kraken":
                from exchange_kraken import create_adapter
                _adapters[xk] = create_adapter()
            elif xk == "control":
                from exchange_control import create_adapter
                _adapters[xk] = create_adapter()
        except Exception as e:
            print(f"[Adapter] failed to create {xk}: {e}")
            return None
    return _adapters.get(xk)


# ── Static files ──

app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(WEB_DIR / "index.html"))


# ── Helpers ──

def _account_for_exchange(xk: str) -> dict:
    acct = AccountModel(env, xk)
    summary = acct.account_summary()
    pnl = acct.pnl()
    positions = acct.positions()
    unrealized = 0.0
    for sym, pos in positions.items():
        qty = float(pos.get("quantity", 0) or 0)
        cost_basis = float(pos.get("avg_cost_basis", 0) or 0)
        sell_price = float(pos.get("current_sell_price", 0) or 0)
        if qty > 0 and cost_basis > 0 and sell_price > 0:
            unrealized += (sell_price - cost_basis) * qty
    return {
        "exchange": xk,
        "account": summary,
        "pnl": {
            "total_realized_profit_usd": pnl.get("total_realized_profit_usd", 0),
            "unrealized_profit_usd": round(unrealized, 2),
            "lth_profit_bucket_usd": pnl.get("lth_profit_bucket_usd", 0),
        },
    }


def _downsample(raw: list[dict], max_points: int) -> list[dict]:
    if len(raw) <= max_points:
        return raw
    bucket_size = len(raw) / max_points
    out = []
    for i in range(max_points):
        start = int(i * bucket_size)
        end = int((i + 1) * bucket_size)
        chunk = raw[start:end]
        if chunk:
            avg_ts = sum(r["ts"] for r in chunk) / len(chunk)
            avg_val = sum(r["total_account_value"] for r in chunk) / len(chunk)
            out.append({"ts": avg_ts, "total_account_value": avg_val})
    return out


# ── REST API ──

@app.get("/api/status")
async def api_status():
    env.reload()
    sm = SystemModel(env)
    ctrl_status = ctrl.status_summary()
    rr = sm.runner_ready()

    exchanges_data = {}
    for xk in env.exchanges:
        exchanges_data[xk] = _account_for_exchange(xk)

    return {
        "system": {
            "neural_running": ctrl_status["neural_running"],
            "trader_running": ctrl_status["trader_running"],
            "traders": ctrl_status.get("traders", {}),
            "runner_ready": rr.get("ready", False),
            "runner_stage": rr.get("stage", "unknown"),
            "any_training_running": ctrl_status["any_training_running"],
        },
        "exchanges": exchanges_data,
        "exchange_list": env.exchanges,
        "coins": env.coins,
    }


@app.get("/api/coins")
async def api_coins():
    env.reload()
    coins = []
    ctrl_status = ctrl.status_summary()

    positions_by_xk = {}
    for xk in env.exchanges:
        acct = AccountModel(env, xk)
        positions_by_xk[xk] = acct.all_positions()

    for coin in env.coins:
        cm = CoinModel(env, coin)
        snap = cm.snapshot()

        snap["positions"] = {}
        mid_prices = []
        for xk in env.exchanges:
            pos = positions_by_xk[xk].get(coin, {})
            snap["positions"][xk] = pos if pos.get("quantity", 0) > 0 else None
            buy = pos.get("current_buy_price", 0)
            sell = pos.get("current_sell_price", 0)
            mid = (buy + sell) / 2 if (buy and sell) else buy or sell
            if mid > 0:
                mid_prices.append(mid)

        snap["mid_price"] = mid_prices[0] if mid_prices else 0
        snap["training_running"] = ctrl_status["training"].get(coin, {}).get("running", False)
        fail = cm.training_failure()
        if fail and fail.get("exception_type"):
            snap["training_failure"] = fail
        coins.append(snap)
    return {"coins": coins}


@app.get("/api/coins/{coin}")
async def api_coin_detail(coin: str):
    coin = coin.upper()
    cm = CoinModel(env, coin)

    result = {**cm.snapshot(), "positions": {}, "trades": {}}
    for xk in env.exchanges:
        acct = AccountModel(env, xk)
        positions = acct.all_positions()
        pos = positions.get(coin, {})
        result["positions"][xk] = pos if pos.get("quantity", 0) > 0 else None

        trades = acct.trade_history(limit=500)
        coin_trades = [t for t in trades if t.get("symbol", "").startswith(f"{coin}_")]
        result["trades"][xk] = coin_trades[-50:]

    return result


@app.get("/api/positions")
async def api_positions():
    out = {}
    dca = {}
    for xk in env.exchanges:
        acct = AccountModel(env, xk)
        out[xk] = acct.positions()
        dca[xk] = acct.dca_24h_by_coin()
    return {"positions": out, "dca_24h": dca}


@app.get("/api/trades")
async def api_trades(limit: int = 250, exchange: str = ""):
    result = {}
    targets = [exchange] if exchange else env.exchanges
    for xk in targets:
        acct = AccountModel(env, xk)
        result[xk] = acct.trade_history(limit=limit)
    return {"trades": result}


@app.get("/api/account-history")
async def api_account_history(hours: float = 0, max_points: int = 500):
    """Return account value history for all exchanges."""
    result = {}
    for xk in env.exchanges:
        acct = AccountModel(env, xk)
        raw = acct.account_value_history(limit=0)
        if hours > 0:
            cutoff = time.time() - hours * 3600
            raw = [r for r in raw if r.get("ts", 0) >= cutoff]
        result[xk] = _downsample(raw, max_points) if raw else []
    return {"history": result}


@app.get("/api/comparison")
async def api_comparison():
    """Per-coin comparison across exchanges."""
    env.reload()
    coins_out = []
    for coin in env.coins:
        row = {"coin": coin}
        for xk in env.exchanges:
            acct = AccountModel(env, xk)
            positions = acct.all_positions()
            pos = positions.get(coin, {})
            trades = acct.trade_history(limit=0)
            coin_trades = [t for t in trades if t.get("symbol", "").startswith(f"{coin}_")]
            total_fees = sum(float(t.get("fees_usd", 0) or 0) for t in coin_trades)
            pnl_pct = pos.get("gain_loss_pct_buy", 0) if pos.get("quantity", 0) > 0 else 0
            value = pos.get("value_usd", 0) if pos.get("quantity", 0) > 0 else 0
            row[xk] = {
                "value_usd": value,
                "pnl_pct": pnl_pct,
                "total_fees": total_fees,
                "trade_count": len(coin_trades),
            }
        coins_out.append(row)

    usdt_row = {"coin": "USDT"}
    for xk in env.exchanges:
        acct = AccountModel(env, xk)
        summary = acct.account_summary()
        usdt_row[xk] = {
            "value_usd": summary.get("buying_power", 0),
            "pnl_pct": 0,
            "total_fees": 0,
            "trade_count": 0,
        }
    coins_out.append(usdt_row)

    coins_out.sort(key=lambda r: r["coin"])

    totals = {}
    for xk in env.exchanges:
        acct = AccountModel(env, xk)
        pnl = acct.pnl()
        totals[xk] = {
            "realized_profit": pnl.get("total_realized_profit_usd", 0),
            "total_fees": sum(c[xk]["total_fees"] for c in coins_out),
        }

    return {"coins": coins_out, "totals": totals, "exchanges": env.exchanges}


@app.get("/api/settings")
async def api_settings():
    env.reload()
    return env.settings


@app.put("/api/settings")
async def api_save_settings(data: dict):
    ok = ctrl.save_settings(data)
    return {"ok": ok}


@app.post("/api/start-all")
async def api_start_all():
    return ctrl.start_all()


@app.post("/api/stop-all")
async def api_stop_all():
    ctrl.stop_all()
    return {"ok": True}


@app.post("/api/start-neural")
async def api_start_neural():
    ok = ctrl.start_neural()
    return {"ok": ok}


@app.post("/api/stop-neural")
async def api_stop_neural():
    ctrl.stop_neural()
    return {"ok": True}


@app.post("/api/start-trader")
async def api_start_trader():
    ok = ctrl.start_trader()
    return {"ok": ok}


@app.post("/api/stop-trader")
async def api_stop_trader():
    ctrl.stop_trader()
    return {"ok": True}


@app.post("/api/start-trader/{exchange}")
async def api_start_trader_xk(exchange: str):
    ok = ctrl.start_trader(exchange.lower())
    return {"ok": ok}


@app.post("/api/stop-trader/{exchange}")
async def api_stop_trader_xk(exchange: str):
    ctrl.stop_trader(exchange.lower())
    return {"ok": True}


@app.post("/api/train-all")
async def api_train_all():
    results = ctrl.train_all()
    return {"ok": True, "results": results}


@app.post("/api/train/{coin}")
async def api_train_coin(coin: str):
    ok = ctrl.start_training(coin.upper())
    return {"ok": ok}


@app.post("/api/stop-training/{coin}")
async def api_stop_training(coin: str):
    ctrl.stop_training(coin.upper())
    return {"ok": True}


@app.get("/api/logs/{script}")
async def api_logs(script: str, limit: int = 200):
    lines = ctrl.peek_logs(script, limit)
    return {"lines": lines}


def _refresh_exchange_balance(xk: str, write_history: bool = True):
    """Query adapter for current balance and update trader_status file."""
    adapter = _get_adapter(xk)
    if not adapter:
        return
    try:
        total_value = adapter.get_account_value() or 0
        buying_power = adapter.get_buying_power() or 0
        holdings_value = total_value - buying_power
        pct = (holdings_value / total_value * 100) if total_value > 0 else 0

        status_path = env.trader_status_path(xk)
        existing = {}
        if status_path.exists():
            try:
                with open(status_path) as f:
                    existing = json.load(f)
            except Exception:
                pass

        existing.setdefault("account", {}).update({
            "total_account_value": total_value,
            "buying_power": buying_power,
            "holdings_sell_value": holdings_value,
            "holdings_buy_value": holdings_value,
            "percent_in_trade": pct,
        })
        existing.setdefault("positions", {})

        status_path.parent.mkdir(parents=True, exist_ok=True)
        with open(status_path, "w") as f:
            json.dump(existing, f)

        if write_history and total_value > 0:
            hist_path = env.account_history_path(xk)
            with open(hist_path, "a") as f:
                f.write(json.dumps({"ts": time.time(), "total_account_value": total_value}) + "\n")
    except Exception as e:
        print(f"[Balance] {xk} refresh failed: {e}")


@app.post("/api/clear-account-history")
async def api_clear_account_history():
    """Delete account value history files for all exchanges."""
    for xk in env.exchanges:
        path = env.account_history_path(xk)
        try:
            if path.exists():
                path.write_text("")
        except Exception:
            pass
    return {"ok": True}


@app.post("/api/reset-all")
async def api_reset_all():
    """Stop everything, close all positions, wipe all state, reset to Kraken USD balance."""
    ctrl.stop_all()

    for xk in env.exchanges:
        adapter = _get_adapter(xk)
        if adapter:
            for coin, qty in adapter.get_holdings().items():
                try:
                    adapter.place_sell(f"{coin}_USD", qty)
                except Exception:
                    pass

    adapter_kr = _get_adapter("kraken")
    balance = adapter_kr.get_buying_power() if adapter_kr else 0

    for xk in env.exchanges:
        for path in [env.trade_history_path(xk), env.account_history_path(xk)]:
            try:
                path.write_text("")
            except Exception:
                pass
        try:
            env.pnl_ledger_path(xk).write_text(json.dumps({
                "total_realized_profit_usd": 0.0,
                "open_positions": {},
                "lth_profit_bucket_usd": 0.0,
            }, indent=2))
        except Exception:
            pass
        bot_ids = env.hub_data_dir / f"bot_order_ids_{xk}.json"
        try:
            bot_ids.write_text("{}")
        except Exception:
            pass
        try:
            env.trader_status_path(xk).write_text(json.dumps(
                {"account": {}, "positions": {}}, indent=2
            ))
        except Exception:
            pass

    ctrl_state = env.hub_data_dir / "control_exchange_state.json"
    try:
        ctrl_state.write_text(json.dumps({
            "usd_balance": balance or 0,
            "holdings": {},
            "orders": {},
        }, indent=2))
    except Exception:
        pass

    _adapters.pop("control", None)
    _get_mirror().reload()
    for xk in env.exchanges:
        _refresh_exchange_balance(xk)

    return {"ok": True, "balance": balance}


@app.post("/api/close-all")
async def api_close_all():
    """Sell all positions on all exchanges, return to USDT."""
    ctrl.stop_trader()
    results = {}
    for xk in env.exchanges:
        if xk == "control":
            continue
        adapter = _get_adapter(xk)
        if not adapter:
            results[xk] = {"ok": False, "error": "no adapter"}
            continue
        holdings = adapter.get_holdings()
        trades = []
        for coin, qty in holdings.items():
            symbol = f"{coin}_USD"
            result = adapter.place_sell(symbol, qty)
            sold = result is not None
            trades.append({"coin": coin, "qty": qty, "sold": sold})
            if sold:
                _record_close_trade(xk, coin, symbol, qty, result)
                _get_mirror().mirror_sell(coin, tag="CLOSE_ALL")
        _refresh_exchange_balance(xk)
        _clear_trader_positions(xk)
        results[xk] = {"ok": True, "trades": trades}
    _clear_trader_positions("control")
    return {"ok": True, "results": results}


@app.post("/api/close-coin/{coin}/{exchange}")
async def api_close_coin(coin: str, exchange: str):
    """Sell a single coin's position on a specific exchange."""
    coin = coin.upper()
    xk = exchange.lower()

    if xk == "control":
        return {"ok": False, "error": "Control positions close automatically when Kraken closes"}

    if xk not in env.exchanges:
        return {"ok": False, "error": f"Unknown exchange: {xk}"}

    adapter = _get_adapter(xk)
    if not adapter:
        return {"ok": False, "error": "No adapter"}

    holdings = adapter.get_holdings()
    qty = holdings.get(coin, 0)
    symbol = f"{coin}_USD"

    if qty > 0:
        result = adapter.place_sell(symbol, qty)
        if not result:
            return {"ok": False, "error": "Sell failed"}
        _record_close_trade(xk, coin, symbol, qty, result, tag="CLOSE")
        if xk == "kraken":
            _get_mirror().mirror_sell(coin, tag="CLOSE")
    else:
        ledger = {}
        try:
            lp = env.pnl_ledger_path(xk)
            if lp.exists():
                ledger = json.loads(lp.read_text())
        except Exception:
            pass
        has_ledger = float(
            (ledger.get("open_positions") or {}).get(coin, {}).get("qty", 0) or 0
        ) > 1e-12
        status = {}
        try:
            sp = env.trader_status_path(xk)
            if sp.exists():
                status = json.loads(sp.read_text())
        except Exception:
            pass
        has_status = float(
            (status.get("positions") or {}).get(coin, {}).get("quantity", 0) or 0
        ) > 1e-12
        if not has_ledger and not has_status:
            return {"ok": False, "error": f"No {coin} position on {xk}"}
        if has_ledger:
            (ledger.get("open_positions") or {}).pop(coin, None)
            try:
                env.pnl_ledger_path(xk).write_text(json.dumps(ledger, indent=2))
            except Exception:
                pass
        if xk == "kraken":
            _get_mirror().mirror_sell(coin, tag="CLOSE")

    _clear_coin_position(xk, coin)
    if xk == "kraken":
        _clear_coin_position("control", coin)
    _refresh_exchange_balance(xk)

    return {"ok": True, "coin": coin, "exchange": xk, "qty": qty}


def _clear_coin_position(xk: str, coin: str):
    """Zero out a single coin's position in trader_status file."""
    path = env.trader_status_path(xk)
    try:
        data = json.loads(path.read_text()) if path.exists() else {}
        pos = (data.get("positions") or {}).get(coin)
        if isinstance(pos, dict):
            pos["quantity"] = 0
            pos["value_usd"] = 0
            pos["avg_cost_basis"] = 0
            pos["gain_loss_pct_buy"] = 0
            pos["gain_loss_pct_sell"] = 0
        path.write_text(json.dumps(data, indent=2))
    except Exception:
        pass


def _clear_trader_positions(xk: str):
    """Zero out positions in trader_status file after close-all."""
    path = env.trader_status_path(xk)
    try:
        data = json.loads(path.read_text()) if path.exists() else {}
        for pos in (data.get("positions") or {}).values():
            if isinstance(pos, dict):
                pos["quantity"] = 0
                pos["value_usd"] = 0
                pos["avg_cost_basis"] = 0
                pos["gain_loss_pct_buy"] = 0
                pos["gain_loss_pct_sell"] = 0
        path.write_text(json.dumps(data, indent=2))
    except Exception:
        pass


def _record_close_trade(xk: str, coin: str, symbol: str, qty: float, result,
                        tag: str = "CLOSE_ALL"):
    """Record a close sell in trade history and update PnL ledger."""
    ts = time.time()
    price = result.avg_price
    notional = result.notional_usd or (price * qty if price else None)
    fees = result.fees_usd

    ledger_path = env.pnl_ledger_path(xk)
    try:
        ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {}
    except Exception:
        ledger = {}

    pos = (ledger.get("open_positions") or {}).get(coin)
    pos_qty = float(pos.get("qty", 0) or 0) if isinstance(pos, dict) else 0.0
    pos_cost = float(pos.get("usd_cost", 0) or 0) if isinstance(pos, dict) else 0.0

    frac = min(1.0, qty / pos_qty) if pos_qty > 0 else 1.0
    cost_used = pos_cost * frac
    avg_cost = cost_used / qty if qty > 0 else None

    realized = None
    if notional is not None:
        fee_val = float(fees) if fees is not None else 0.0
        realized = (notional - fee_val) - cost_used

    pnl_pct = None
    if realized is not None and cost_used > 0:
        pnl_pct = (realized / cost_used) * 100

    if isinstance(pos, dict):
        pos["usd_cost"] = pos_cost - cost_used
        pos["qty"] = pos_qty - qty
        if float(pos.get("qty", 0) or 0) <= 1e-12:
            ledger.get("open_positions", {}).pop(coin, None)

    if realized is not None:
        ledger["total_realized_profit_usd"] = float(
            ledger.get("total_realized_profit_usd", 0) or 0
        ) + realized

    try:
        ledger_path.write_text(json.dumps(ledger, indent=2))
    except Exception:
        pass

    entry = {
        "ts": ts,
        "side": "sell",
        "tag": tag,
        "symbol": symbol,
        "qty": qty,
        "price": price,
        "notional_usd": notional,
        "net_usd": (notional - (float(fees) if fees else 0.0)) if notional else None,
        "avg_cost_basis": avg_cost,
        "pnl_pct": pnl_pct,
        "fees_usd": fees,
        "fees_missing": fees is None,
        "fees_fallback_applied_usd": 0.0,
        "realized_profit_usd": realized,
        "order_id": result.order_id,
        "position_cost_used_usd": cost_used,
        "position_cost_after_usd": float((ledger.get("open_positions") or {}).get(coin, {}).get("usd_cost", 0) or 0),
    }
    history_path = env.trade_history_path(xk)
    try:
        with open(history_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception:
        pass


@app.post("/api/sync-control")
async def api_sync_control():
    """Reset control balance to match kraken USDT. Requires traders stopped, no positions."""
    if ctrl.trader_running:
        return {"ok": False, "error": "Traders must be stopped"}

    for xk in env.exchanges:
        a = _get_adapter(xk)
        if a:
            holdings = a.get_holdings()
            pnl = json.loads(env.pnl_ledger_path(xk).read_text()) if env.pnl_ledger_path(xk).exists() else {}
            open_pos = pnl.get("open_positions") or {}
            has_real = any(
                float((open_pos.get(coin) or {}).get("qty", 0) or 0) > 1e-12
                for coin in holdings
            )
            if has_real:
                return {"ok": False, "error": f"{xk} has open positions"}

    adapter_kr = _get_adapter("kraken")
    if not adapter_kr:
        return {"ok": False, "error": "Cannot connect to Kraken"}

    buying_power = adapter_kr.get_buying_power()
    if not buying_power or buying_power <= 0:
        return {"ok": False, "error": "Kraken has no USD balance"}

    state_path = env.hub_data_dir / "control_exchange_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w") as f:
        json.dump({"usd_balance": buying_power, "holdings": {}, "orders": {}}, f, indent=2)

    _adapters.pop("control", None)
    _get_mirror().reload()
    _refresh_exchange_balance("control")
    print(f"[Sync] Control balance set to ${buying_power:,.2f} from Kraken")
    return {"ok": True, "balance": buying_power}


@app.get("/api/candles/{coin}")
async def api_candles(coin: str, timeframe: str = "1hour", limit: int = 250,
                      source: str = ""):
    """Fetch candle data for charting. Source: kraken or kucoin (auto from settings)."""
    coin = coin.upper()
    env.reload()
    price_source = source or env.settings.get("live_price_source", "kraken")

    if price_source == "kraken":
        return _candles_kraken(coin, timeframe, limit)
    return _candles_kucoin(coin, timeframe, limit)


def _candles_kraken(coin: str, timeframe: str, limit: int) -> dict:
    tf_map = {
        "1min": "1m", "5min": "5m", "15min": "15m", "30min": "30m",
        "1hour": "1h", "2hour": "2h", "4hour": "4h",
        "8hour": "8h", "12hour": "12h", "1day": "1d", "1week": "1w",
    }
    ccxt_tf = tf_map.get(timeframe, "1h")
    try:
        import ccxt
        kraken = ccxt.kraken({"enableRateLimit": True})
        ohlcv = kraken.fetch_ohlcv(f"{coin}/USDT", ccxt_tf, limit=limit)
        candles = []
        for bar in ohlcv:
            candles.append({
                "time": int(bar[0] / 1000),
                "open": float(bar[1]),
                "high": float(bar[2]),
                "low": float(bar[3]),
                "close": float(bar[4]),
                "volume": float(bar[5]),
            })
        return {"candles": candles}
    except Exception as e:
        return _candles_kucoin(coin, timeframe, limit)


def _candles_kucoin(coin: str, timeframe: str, limit: int) -> dict:
    tf_map = {
        "1min": "1min", "5min": "5min", "15min": "15min", "30min": "30min",
        "1hour": "1hour", "2hour": "2hour", "4hour": "4hour",
        "8hour": "8hour", "12hour": "12hour", "1day": "1day", "1week": "1week",
    }
    kc_tf = tf_map.get(timeframe, "1hour")
    try:
        import requests as _req
        url = f"https://api.kucoin.com/api/v1/market/candles?type={kc_tf}&symbol={coin}-USDT&pageSize={limit}"
        resp = _req.get(url, timeout=10)
        data = resp.json()
        klines = data.get("data", [])
        candles = []
        for k in reversed(klines):
            candles.append({
                "time": int(k[0]),
                "open": float(k[1]),
                "close": float(k[2]),
                "high": float(k[3]),
                "low": float(k[4]),
                "volume": float(k[5]),
            })
        return {"candles": candles}
    except Exception as e:
        return {"candles": [], "error": str(e)}


# ── WebSocket for real-time updates ──

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


ws_manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)


async def _file_watcher():
    """Poll key files and broadcast changes to WebSocket clients."""
    mtimes: dict[str, float] = {}
    balance_tick = 0

    def _check(path: Path, key: str) -> bool:
        try:
            mt = path.stat().st_mtime
        except FileNotFoundError:
            return False
        old = mtimes.get(key, 0)
        if mt != old:
            mtimes[key] = mt
            return True
        return False

    while True:
        if not ws_manager.active:
            await asyncio.sleep(2)
            continue

        try:
            env.reload()

            for xk in env.exchanges:
                if _check(env.trader_status_path(xk), f"trader_status_{xk}"):
                    acct = AccountModel(env, xk)
                    ts = acct.trader_status()
                    if ts:
                        await ws_manager.broadcast({
                            "type": "trader_status",
                            "exchange": xk,
                            "data": ts,
                        })
                        xd = _account_for_exchange(xk)
                        await ws_manager.broadcast({
                            "type": "pnl",
                            "exchange": xk,
                            "data": xd["pnl"],
                        })

                if _check(env.pnl_ledger_path(xk), f"pnl_{xk}"):
                    xd = _account_for_exchange(xk)
                    await ws_manager.broadcast({
                        "type": "pnl",
                        "exchange": xk,
                        "data": xd["pnl"],
                    })

            signals_changed = False
            for coin in env.coins:
                if _check(env.long_signal_path(coin), f"long_{coin}"):
                    signals_changed = True
                if _check(env.short_signal_path(coin), f"short_{coin}"):
                    signals_changed = True

            if signals_changed:
                coins_data = {}
                for coin in env.coins:
                    cm = CoinModel(env, coin)
                    coins_data[coin] = {
                        "long": cm.long_signal(),
                        "short": cm.short_signal(),
                        "long_prices": cm.long_price_levels(),
                        "short_prices": cm.short_price_levels(),
                    }
                await ws_manager.broadcast({"type": "signals", "data": coins_data})

            if _check(env.runner_ready_path(), "runner_ready"):
                sm = SystemModel(env)
                rr = sm.runner_ready()
                await ws_manager.broadcast({"type": "runner_ready", "data": rr})
                if rr.get("ready") and ctrl.neural_running:
                    ctrl.poll_ready_and_start_trader()

            traders_status = {}
            for xk in env.exchanges:
                traders_status[xk] = {"running": ctrl.trader_running_for(xk)}
            sys_status = {
                "neural_running": ctrl.neural_running,
                "trader_running": ctrl.trader_running,
                "traders": traders_status,
                "any_training_running": ctrl.any_training_running(),
            }
            await ws_manager.broadcast({"type": "system", "data": sys_status})

            balance_tick += 1
            if balance_tick >= 20:
                balance_tick = 0
                if not ctrl.trader_running_for("kraken"):
                    try:
                        _get_mirror().write_status()
                    except Exception:
                        pass
                    _refresh_exchange_balance("control", write_history=False)
                    _refresh_exchange_balance("kraken", write_history=False)

        except Exception:
            pass

        await asyncio.sleep(1.5)


def _init_exchange_balances():
    """Seed initial control state if needed, then refresh all balances."""
    env.reload()

    ctrl_state = env.hub_data_dir / "control_exchange_state.json"
    if not ctrl_state.exists():
        starting = float(env.settings.get("control_starting_usd", 0))
        if starting <= 0:
            kr = _get_adapter("kraken")
            starting = (kr.get_buying_power() or 0) if kr else 0
            print(f"[Init] control starting balance from kraken: ${starting:,.2f}")
        if starting > 0:
            ctrl_state.parent.mkdir(parents=True, exist_ok=True)
            with open(ctrl_state, "w") as f:
                json.dump({"usd_balance": starting, "holdings": {}, "orders": {}}, f)
            print(f"[Init] wrote {ctrl_state}")

    for xk in env.exchanges:
        _refresh_exchange_balance(xk)
        status_path = env.trader_status_path(xk)
        if status_path.exists():
            try:
                with open(status_path) as f:
                    data = json.load(f)
                total = data.get("account", {}).get("total_account_value", 0)
                print(f"[Init] {xk}: ${total:,.2f}")
            except Exception:
                pass


from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    _init_exchange_balances()
    asyncio.create_task(_file_watcher())
    yield

app.router.lifespan_context = lifespan


# ── Entry point ──

if __name__ == "__main__":
    import uvicorn
    parser = argparse.ArgumentParser(description="PowerTrader Web UI")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)
