"""Business logic for PowerTrader_AI: subprocess management, training, settings.

Designed to be shared by pt_hub.py (tkinter) and pt_web.py (web app).
"""

import glob
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from pt_env import PTEnv
from pt_models import CoinModel, SystemModel


@dataclass
class ProcHandle:
    name: str
    proc: subprocess.Popen | None = None
    log_q: queue.Queue = field(default_factory=lambda: queue.Queue(maxsize=4000))
    _thread: threading.Thread | None = field(default=None, repr=False)

    @property
    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None


def _reader_thread(proc: subprocess.Popen, q: queue.Queue, prefix: str):
    try:
        while True:
            line = proc.stdout.readline() if proc.stdout else ""
            if not line:
                if proc.poll() is not None:
                    break
                time.sleep(0.05)
                continue
            msg = f"{time.strftime('%H:%M:%S')} {prefix}{line.rstrip()}"
            if q.full():
                try:
                    q.get_nowait()
                except queue.Empty:
                    pass
            try:
                q.put_nowait(msg)
            except queue.Full:
                pass
    except Exception:
        pass


class ProcessController:
    """Manages thinker, trader, and trainer subprocesses."""

    def __init__(self, env: PTEnv):
        self.env = env
        self._neural = ProcHandle(name="neural")
        self._trader = ProcHandle(name="trader")
        self._trainers: dict[str, ProcHandle] = {}
        self._lock = threading.Lock()

    def _make_env(self) -> dict:
        e = os.environ.copy()
        e["POWERTRADER_HUB_DIR"] = str(self.env.hub_data_dir)
        e["POWERTRADER_EXCHANGE"] = self.env.exchange
        return e

    def _launch(self, handle: ProcHandle, script_path: str, args: list[str] | None = None,
                cwd: str | None = None, prefix: str = "") -> bool:
        if handle.alive:
            return True
        if not os.path.isfile(script_path):
            return False
        cmd = [sys.executable, "-u", script_path] + (args or [])
        try:
            handle.proc = subprocess.Popen(
                cmd,
                cwd=cwd or str(self.env.project_dir),
                env=self._make_env(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            handle._thread = threading.Thread(
                target=_reader_thread,
                args=(handle.proc, handle.log_q, prefix),
                daemon=True,
            )
            handle._thread.start()
            return True
        except Exception:
            return False

    def _stop(self, handle: ProcHandle):
        if handle.alive:
            try:
                handle.proc.terminate()
            except Exception:
                pass

    # -- Neural (thinker) --

    def start_neural(self) -> bool:
        rr_path = self.env.runner_ready_path()
        try:
            rr_path.parent.mkdir(parents=True, exist_ok=True)
            with open(rr_path, "w") as f:
                json.dump({"timestamp": time.time(), "ready": False, "stage": "starting"}, f)
        except Exception:
            pass

        ar_path = self.env.neural_autorestart_path()
        try:
            with open(ar_path, "w") as f:
                json.dump({
                    "timestamp": time.time(),
                    "should_be_running": True,
                    "user_stopped_from_hub": False,
                    "last_auto_restart_ts": 0.0,
                }, f)
        except Exception:
            pass

        return self._launch(
            self._neural,
            str(self.env.script_path("thinker")),
            prefix="[RUNNER] ",
        )

    def stop_neural(self):
        self._stop(self._neural)
        ar_path = self.env.neural_autorestart_path()
        try:
            with open(ar_path, "w") as f:
                json.dump({
                    "timestamp": time.time(),
                    "should_be_running": False,
                    "user_stopped_from_hub": True,
                    "last_auto_restart_ts": 0.0,
                }, f)
        except Exception:
            pass

    @property
    def neural_running(self) -> bool:
        return self._neural.alive

    # -- Trader --

    def start_trader(self) -> bool:
        return self._launch(
            self._trader,
            str(self.env.script_path("trader")),
            prefix="[TRADER] ",
        )

    def stop_trader(self):
        self._stop(self._trader)

    @property
    def trader_running(self) -> bool:
        return self._trader.alive

    # -- Start/stop all --

    def start_all(self) -> dict:
        """Start neural, wait for ready, then start trader. Returns status."""
        ok_neural = self.start_neural()
        if not ok_neural:
            return {"ok": False, "error": "Failed to start neural"}
        return {"ok": True, "message": "Neural started, trader will start when ready"}

    def poll_ready_and_start_trader(self) -> bool:
        """Check runner_ready and start trader if ready. Returns True if trader started."""
        sm = SystemModel(self.env)
        rr = sm.runner_ready()
        if rr.get("ready"):
            if not self.trader_running:
                return self.start_trader()
            return True
        return False

    def stop_all(self):
        self.stop_trader()
        self.stop_neural()

    # -- Training --

    def start_training(self, coin: str) -> bool:
        with self._lock:
            if coin in self._trainers and self._trainers[coin].alive:
                return True

            coin_dir = self.env.coin_dir(coin)
            coin_dir.mkdir(parents=True, exist_ok=True)

            trainer_name = os.path.basename(
                self.env.settings.get("script_neural_trainer", "pt_trainer.py")
            )
            src = self.env.project_dir / trainer_name
            dst = coin_dir / trainer_name
            if src.is_file():
                shutil.copy2(str(src), str(dst))

            if not dst.is_file():
                return False

            patterns = [
                "trainer_last_training_time.txt", "trainer_status.json",
                "trainer_failure_info.json", "trainer_last_start_time.txt",
                "killer.txt", "memories_*.txt", "memory_weights_*.txt",
                "neural_perfect_threshold_*.txt",
            ]
            for pat in patterns:
                for fp in glob.glob(str(coin_dir / pat)):
                    try:
                        os.remove(fp)
                    except Exception:
                        pass

            handle = ProcHandle(name=f"Trainer-{coin}")
            ok = self._launch(
                handle,
                str(dst),
                args=[coin],
                cwd=str(coin_dir),
                prefix=f"[{coin}] ",
            )
            if ok:
                self._trainers[coin] = handle
            return ok

    def train_all(self) -> dict[str, bool]:
        self.stop_neural()
        results = {}
        for coin in self.env.coins:
            results[coin] = self.start_training(coin)
        return results

    def stop_training(self, coin: str):
        with self._lock:
            handle = self._trainers.get(coin)
            if handle:
                self._stop(handle)
            killer = self.env.killer_path(coin)
            try:
                killer.write_text("1")
            except Exception:
                pass

    def stop_all_training(self):
        for coin in list(self._trainers.keys()):
            self.stop_training(coin)

    def training_running(self, coin: str) -> bool:
        h = self._trainers.get(coin)
        return h.alive if h else False

    def any_training_running(self) -> bool:
        return any(h.alive for h in self._trainers.values())

    # -- Logs --

    def get_logs(self, script: str, limit: int = 200) -> list[str]:
        if script == "neural":
            q = self._neural.log_q
        elif script == "trader":
            q = self._trader.log_q
        elif script.startswith("trainer-"):
            coin = script.split("-", 1)[1].upper()
            h = self._trainers.get(coin)
            q = h.log_q if h else None
        else:
            return []

        if not q:
            return []
        lines = []
        while not q.empty() and len(lines) < limit:
            try:
                lines.append(q.get_nowait())
            except queue.Empty:
                break
        return lines

    def peek_logs(self, script: str, limit: int = 200) -> list[str]:
        """Read logs without consuming them."""
        if script == "neural":
            q = self._neural.log_q
        elif script == "trader":
            q = self._trader.log_q
        elif script.startswith("trainer-"):
            coin = script.split("-", 1)[1].upper()
            h = self._trainers.get(coin)
            q = h.log_q if h else None
        else:
            return []

        if not q:
            return []
        with q.mutex:
            items = list(q.queue)
        return items[-limit:]

    # -- Settings --

    def save_settings(self, data: dict) -> bool:
        try:
            path = self.env.settings_path
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
            self.env.reload()
            return True
        except Exception:
            return False

    # -- Status summary --

    def status_summary(self) -> dict:
        training = {}
        for coin in self.env.coins:
            cm = CoinModel(self.env, coin)
            ts = cm.training_status()
            training[coin] = {
                "state": ts.get("state", "UNKNOWN"),
                "is_trained": cm.is_trained(),
                "running": self.training_running(coin),
            }

        return {
            "neural_running": self.neural_running,
            "trader_running": self.trader_running,
            "any_training_running": self.any_training_running(),
            "training": training,
        }
