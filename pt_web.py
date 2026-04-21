"""FastAPI web server for PowerTrader_AI.

Run:  python pt_web.py [--port 8080]

Provides REST API + WebSocket for real-time updates, serving the web/ frontend.
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

PROJECT_DIR = Path(__file__).resolve().parent
WEB_DIR = PROJECT_DIR / "web"

env = PTEnv(project_dir=PROJECT_DIR)
ctrl = ProcessController(env)
app = FastAPI(title="PowerTrader Web")


# ── Static files ──

app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(WEB_DIR / "index.html"))


# ── REST API ──

@app.get("/api/status")
async def api_status():
    env.reload()
    sm = SystemModel(env)
    acct = AccountModel(env)
    summary = acct.account_summary()
    pnl = acct.pnl()
    ctrl_status = ctrl.status_summary()
    rr = sm.runner_ready()
    return {
        "system": {
            "neural_running": ctrl_status["neural_running"],
            "trader_running": ctrl_status["trader_running"],
            "runner_ready": rr.get("ready", False),
            "runner_stage": rr.get("stage", "unknown"),
            "any_training_running": ctrl_status["any_training_running"],
        },
        "account": summary,
        "pnl": {
            "total_realized_profit_usd": pnl.get("total_realized_profit_usd", 0),
            "lth_profit_bucket_usd": pnl.get("lth_profit_bucket_usd", 0),
        },
        "exchange": env.exchange,
        "coins": env.coins,
    }


@app.get("/api/coins")
async def api_coins():
    env.reload()
    coins = []
    acct = AccountModel(env)
    positions = acct.all_positions()
    ctrl_status = ctrl.status_summary()

    for coin in env.coins:
        cm = CoinModel(env, coin)
        snap = cm.snapshot()
        pos = positions.get(coin, {})
        snap["position"] = pos if pos.get("quantity", 0) > 0 else None
        snap["training_running"] = ctrl_status["training"].get(coin, {}).get("running", False)
        coins.append(snap)
    return {"coins": coins}


@app.get("/api/coins/{coin}")
async def api_coin_detail(coin: str):
    coin = coin.upper()
    cm = CoinModel(env, coin)
    acct = AccountModel(env)
    positions = acct.all_positions()
    pos = positions.get(coin, {})

    trades = acct.trade_history(limit=500)
    coin_trades = [t for t in trades if t.get("symbol", "").startswith(f"{coin}_")]

    return {
        **cm.snapshot(),
        "position": pos if pos.get("quantity", 0) > 0 else None,
        "trades": coin_trades[-50:],
    }


@app.get("/api/positions")
async def api_positions():
    acct = AccountModel(env)
    return {"positions": acct.positions()}


@app.get("/api/trades")
async def api_trades(limit: int = 250):
    acct = AccountModel(env)
    return {"trades": acct.trade_history(limit=limit)}


@app.get("/api/account-history")
async def api_account_history(hours: float = 0, max_points: int = 500):
    """Return account value history, optionally filtered by hours and downsampled."""
    acct = AccountModel(env)
    raw = acct.account_value_history(limit=0)

    if hours > 0:
        cutoff = time.time() - hours * 3600
        raw = [r for r in raw if r.get("ts", 0) >= cutoff]

    if not raw:
        return {"history": []}

    if len(raw) <= max_points:
        return {"history": raw}

    bucket_size = len(raw) / max_points
    downsampled = []
    for i in range(max_points):
        start = int(i * bucket_size)
        end = int((i + 1) * bucket_size)
        chunk = raw[start:end]
        if chunk:
            avg_ts = sum(r["ts"] for r in chunk) / len(chunk)
            avg_val = sum(r["total_account_value"] for r in chunk) / len(chunk)
            downsampled.append({"ts": avg_ts, "total_account_value": avg_val})
    return {"history": downsampled}


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


@app.get("/api/candles/{coin}")
async def api_candles(coin: str, timeframe: str = "1hour", limit: int = 250):
    """Fetch candle data from KuCoin for charting."""
    coin = coin.upper()
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

            if _check(env.trader_status_path(), "trader_status"):
                acct = AccountModel(env)
                ts = acct.trader_status()
                if ts:
                    await ws_manager.broadcast({"type": "trader_status", "data": ts})

            if _check(env.pnl_ledger_path(), "pnl"):
                acct = AccountModel(env)
                pnl = acct.pnl()
                await ws_manager.broadcast({"type": "pnl", "data": pnl})

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
                if rr.get("ready"):
                    ctrl.poll_ready_and_start_trader()

            sys_status = {
                "neural_running": ctrl.neural_running,
                "trader_running": ctrl.trader_running,
                "any_training_running": ctrl.any_training_running(),
            }
            await ws_manager.broadcast({"type": "system", "data": sys_status})

        except Exception:
            pass

        await asyncio.sleep(1.5)


from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
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
