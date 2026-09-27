"""
Singleton ArcticDB access server.

pt_web, pt_trainer, and pt_data_manager each independently opened their own
arcticdb.Arctic() handle onto the same live LMDB store (state/historic_data),
which is itself on the Windows-bind-mounted powertrader_live volume. Multiple
OS processes mmap'ing the same LMDB files concurrently over that cross-OS
mount is the suspected cause of a string of "impossible" memory-corruption
bugs (see pt_account_analytics._default_price_source's docstring for an
earlier, in-process version of the same class of bug) and a confirmed native
segfault in pt_web on 2026-09-27.

This process is now the ONLY thing in the container allowed to open
arcticdb.Arctic() against the live store. Everything else goes through
pt_arctic_client.py, which talks to this server over a local Unix socket.
Launched as a subprocess by pt_controller.ProcessController, alongside
thinker/trader/data_manager.
"""

from __future__ import annotations

import os
import threading
from multiprocessing.managers import BaseManager
from typing import Optional

import pandas as pd

SOCKET_PATH = "/tmp/pt_arctic.sock"
AUTHKEY = b"pt-arctic-ipc"


class ArcticService:
    """Flat, minimal wrapper around the handful of ArcticDB operations this
    codebase actually uses (list/read/write/append/create). Used directly
    for local/backtest access (no server involved), and wrapped by
    _ArcticManager for the live singleton — same class, same behavior,
    either way.

    All access is serialized behind one lock. This class exists specifically
    to be the *only* thing touching a given ArcticDB store, but
    BaseManager's server handles each client connection in its own thread —
    it does not serialize calls — so without this lock, moving from
    multiple *processes* concurrently hitting arcticdb.Arctic() to multiple
    *threads* concurrently hitting one shared Arctic/Library object would
    just be the same unsynchronized-access hazard one level down (see
    pt_account_analytics._default_price_source's docstring for the
    in-process version of this bug class that motivated this server in the
    first place)."""

    def __init__(self, arctic_url: str):
        import arcticdb as adb

        self._arctic = adb.Arctic(arctic_url)
        self._lib_cache: dict = {}
        self._lock = threading.Lock()

    def _lib(self, lib_name: str):
        if lib_name not in self._lib_cache:
            self._lib_cache[lib_name] = self._arctic.get_library(lib_name)
        return self._lib_cache[lib_name]

    def list_libraries(self) -> list[str]:
        with self._lock:
            return self._arctic.list_libraries()

    def create_library(self, lib_name: str) -> None:
        with self._lock:
            self._arctic.create_library(lib_name)

    def list_symbols(self, lib_name: str) -> list[str]:
        with self._lock:
            return self._lib(lib_name).list_symbols()

    def read(
        self,
        lib_name: str,
        symbol: str,
        date_range: Optional[tuple] = None,
        row_range: Optional[tuple] = None,
        columns: Optional[list] = None,
    ) -> Optional[pd.DataFrame]:
        kwargs = {}
        if date_range is not None:
            kwargs["date_range"] = date_range
        if row_range is not None:
            kwargs["row_range"] = row_range
        if columns is not None:
            kwargs["columns"] = columns
        with self._lock:
            return self._lib(lib_name).read(symbol, **kwargs).data

    def write(self, lib_name: str, symbol: str, df: pd.DataFrame,
              prune_previous_versions: bool = True) -> None:
        with self._lock:
            self._lib(lib_name).write(symbol, df, prune_previous_versions=prune_previous_versions)

    def append(self, lib_name: str, symbol: str, df: pd.DataFrame) -> None:
        with self._lock:
            self._lib(lib_name).append(symbol, df)


class _ArcticManager(BaseManager):
    pass


_ArcticManager.register("get_service")


def serve(arctic_url: str, socket_path: str = SOCKET_PATH) -> None:
    try:
        os.unlink(socket_path)
    except FileNotFoundError:
        pass

    service = ArcticService(arctic_url)
    _ArcticManager.register("get_service", callable=lambda: service)
    manager = _ArcticManager(address=socket_path, authkey=AUTHKEY)
    server = manager.get_server()
    print(f"[ArcticServer] serving {arctic_url!r} on {socket_path}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    from pt_env import PTEnv

    env = PTEnv(os.path.dirname(os.path.abspath(__file__)))
    env.historic_data_dir.mkdir(parents=True, exist_ok=True)
    serve(f"lmdb:///{env.historic_data_dir}")
