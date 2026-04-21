"""
Robinhood API credential management UI.

Extracted from pt_hub.py — provides the setup wizard (Ed25519 key generation,
credential testing, file storage) and the settings-row section that shows
credential status with Setup / Open Folder / Clear buttons.
"""

from __future__ import annotations

import base64
import os
import shutil
import subprocess
import sys
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable, Tuple


def _api_paths(project_dir: str) -> Tuple[str, str]:
    key_path = os.path.join(project_dir, "r_key.txt")
    secret_path = os.path.join(project_dir, "r_secret.txt")
    return key_path, secret_path


def _read_api_files(project_dir: str) -> Tuple[str, str]:
    key_path, secret_path = _api_paths(project_dir)
    try:
        with open(key_path, "r", encoding="utf-8") as f:
            k = (f.read() or "").strip()
    except Exception:
        k = ""
    try:
        with open(secret_path, "r", encoding="utf-8") as f:
            s = (f.read() or "").strip()
    except Exception:
        s = ""
    return k, s


def _open_robinhood_api_wizard(
    parent: tk.Toplevel,
    project_dir: str,
    scaled_geometry_fn: Callable,
    theme: dict,
    on_credentials_changed: Callable,
) -> None:
    """
    Beginner-friendly wizard that creates + stores Robinhood Crypto Trading API credentials.

    What we store:
      - r_key.txt    = your Robinhood *API Key* (safe-ish to store, still treat as sensitive)
      - r_secret.txt = your *PRIVATE key* (treat like a password — never share it)
    """
    import webbrowser
    import time
    from datetime import datetime

    try:
        from cryptography.hazmat.primitives.asymmetric import ed25519
        from cryptography.hazmat.primitives import serialization
    except Exception:
        messagebox.showerror(
            "Missing dependency",
            "The 'cryptography' package is required for Robinhood API setup.\n\n"
            "Fix: open a Command Prompt / Terminal in this folder and run:\n"
            "  pip install cryptography\n\n"
            "Then re-open this Setup Wizard.",
        )
        return

    try:
        import requests
    except Exception:
        requests = None

    DARK_BG = theme["DARK_BG"]
    DARK_PANEL = theme["DARK_PANEL"]
    DARK_FG = theme["DARK_FG"]
    DARK_BORDER = theme["DARK_BORDER"]

    wiz = tk.Toplevel(parent)
    wiz.title("Robinhood API Setup")
    _sw, _sh = scaled_geometry_fn(980, 720)
    wiz.geometry(f"{_sw}x{_sh}")
    _mw, _mh = scaled_geometry_fn(860, 620)
    wiz.minsize(_mw, _mh)
    wiz.configure(bg=DARK_BG)

    viewport = ttk.Frame(wiz)
    viewport.pack(fill="both", expand=True, padx=12, pady=12)
    viewport.grid_rowconfigure(0, weight=1)
    viewport.grid_columnconfigure(0, weight=1)

    wiz_canvas = tk.Canvas(
        viewport,
        bg=DARK_BG,
        highlightthickness=1,
        highlightbackground=DARK_BORDER,
        bd=0,
    )
    wiz_canvas.grid(row=0, column=0, sticky="nsew")

    wiz_scroll = ttk.Scrollbar(
        viewport, orient="vertical", command=wiz_canvas.yview
    )
    wiz_scroll.grid(row=0, column=1, sticky="ns")
    wiz_canvas.configure(yscrollcommand=wiz_scroll.set)

    container = ttk.Frame(wiz_canvas)
    wiz_window = wiz_canvas.create_window((0, 0), window=container, anchor="nw")
    container.columnconfigure(0, weight=1)

    def _update_wiz_scrollbars(event=None) -> None:
        try:
            c = wiz_canvas
            win_id = wiz_window

            c.update_idletasks()
            bbox = c.bbox(win_id)
            if not bbox:
                wiz_scroll.grid_remove()
                return

            c.configure(scrollregion=bbox)
            content_h = int(bbox[3] - bbox[1])
            view_h = int(c.winfo_height())

            if content_h > (view_h + 1):
                wiz_scroll.grid()
            else:
                wiz_scroll.grid_remove()
                try:
                    c.yview_moveto(0)
                except Exception:
                    pass
        except Exception:
            pass

    def _on_wiz_canvas_configure(e) -> None:
        try:
            wiz_canvas.itemconfigure(wiz_window, width=int(e.width))
        except Exception:
            pass
        _update_wiz_scrollbars()

    wiz_canvas.bind("<Configure>", _on_wiz_canvas_configure, add="+")
    container.bind("<Configure>", _update_wiz_scrollbars, add="+")

    def _wheel(e):
        try:
            if wiz_scroll.winfo_ismapped():
                wiz_canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
        except Exception:
            pass

    wiz_canvas.bind("<Enter>", lambda _e: wiz_canvas.focus_set(), add="+")
    wiz_canvas.bind("<MouseWheel>", _wheel, add="+")
    wiz_canvas.bind(
        "<Button-4>", lambda _e: wiz_canvas.yview_scroll(-3, "units"), add="+"
    )
    wiz_canvas.bind(
        "<Button-5>", lambda _e: wiz_canvas.yview_scroll(3, "units"), add="+"
    )

    key_path, secret_path = _api_paths(project_dir)

    existing_api_key, existing_private_b64 = _read_api_files(project_dir)
    private_b64_state = {"value": (existing_private_b64 or "").strip()}

    # ---- Helpers ----

    def _open_in_file_manager(path: str) -> None:
        try:
            p = os.path.abspath(path)
            if os.name == "nt":
                os.startfile(p)  # type: ignore[attr-defined]
                return
            if sys.platform == "darwin":
                subprocess.Popen(["open", p])
                return
            subprocess.Popen(["xdg-open", p])
        except Exception as e:
            messagebox.showerror(
                "Couldn't open folder", f"Tried to open:\n{path}\n\nError:\n{e}"
            )

    def _copy_to_clipboard(txt: str, title: str = "Copied") -> None:
        try:
            wiz.clipboard_clear()
            wiz.clipboard_append(txt)
            messagebox.showinfo(title, "Copied to clipboard.")
        except Exception:
            pass

    def _mask_path(p: str) -> str:
        try:
            return os.path.abspath(p)
        except Exception:
            return p

    # ---- Instructions ----

    intro = (
        "This trader uses Robinhood's Crypto Trading API credentials.\n\n"
        "You only do this once. When finished, pt_trader.py can authenticate automatically.\n\n"
        "✅ What you will do in this window:\n"
        "  1) Generate a Public Key + Private Key (Ed25519).\n"
        "  2) Copy the PUBLIC key and paste it into Robinhood to create an API credential.\n"
        "  3) Robinhood will show you an API Key (usually starts with 'rh...'). Copy it.\n"
        "  4) Paste that API Key back here and click Save.\n\n"
        "🧭 EXACTLY where to paste the Public Key on Robinhood (desktop web is best):\n"
        "  A) Log in to Robinhood on a computer.\n"
        "  B) Click Account (top-right) → Settings.\n"
        "  C) Click Crypto.\n"
        "  D) Scroll down to API Trading and click + Add Key (or Add key).\n"
        "  E) Paste the Public Key into the Public key field.\n"
        "  F) Give it any name (example: PowerTrader).\n"
        "  G) Permissions: this TRADER needs READ + TRADE. (READ-only cannot place orders.)\n"
        "  H) Click Save. Robinhood shows your API Key — copy it right away (it may only show once).\n\n"
        "📱 Mobile note: if you can't find API Trading in the app, use robinhood.com in a browser.\n\n"
        "This wizard will save two files in the same folder as pt_hub.py:\n"
        "  - r_key.txt    (your API Key)\n"
        "  - r_secret.txt (your PRIVATE key in base64)  ← keep this secret like a password\n"
    )

    intro_lbl = ttk.Label(container, text=intro, justify="left")
    intro_lbl.grid(row=0, column=0, sticky="ew", pady=(0, 10))

    top_btns = ttk.Frame(container)
    top_btns.grid(row=1, column=0, sticky="ew", pady=(0, 10))
    top_btns.columnconfigure(0, weight=1)

    def open_robinhood_page():
        webbrowser.open("https://robinhood.com/account/crypto")

    ttk.Button(
        top_btns,
        text="Open Robinhood API Credentials page (Crypto)",
        command=open_robinhood_page,
    ).pack(side="left")
    ttk.Button(
        top_btns,
        text="Open Robinhood Crypto Trading API docs",
        command=lambda: webbrowser.open(
            "https://docs.robinhood.com/crypto/trading/"
        ),
    ).pack(side="left", padx=8)
    ttk.Button(
        top_btns,
        text="Open Folder With r_key.txt / r_secret.txt",
        command=lambda: _open_in_file_manager(project_dir),
    ).pack(side="left", padx=8)

    # ---- Step 1 — Generate keys ----

    step1 = ttk.LabelFrame(
        container, text="Step 1 — Generate your keys (click once)"
    )
    step1.grid(row=2, column=0, sticky="nsew", pady=(0, 10))
    step1.columnconfigure(0, weight=1)

    ttk.Label(
        step1, text="Public Key (this is what you paste into Robinhood):"
    ).grid(row=0, column=0, sticky="w", padx=10, pady=(8, 0))

    pub_box = tk.Text(step1, height=4, wrap="none")
    pub_box.grid(row=1, column=0, sticky="nsew", padx=10, pady=(6, 10))
    pub_box.configure(bg=DARK_PANEL, fg=DARK_FG, insertbackground=DARK_FG)

    def _render_public_from_private_b64(priv_b64: str) -> str:
        try:
            raw = base64.b64decode(priv_b64)
            if len(raw) == 64:
                seed = raw[:32]
            elif len(raw) == 32:
                seed = raw
            else:
                return ""
            pk = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
            pub_raw = pk.public_key().public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw,
            )
            return base64.b64encode(pub_raw).decode("utf-8")
        except Exception:
            return ""

    def _set_pub_text(txt: str) -> None:
        try:
            pub_box.delete("1.0", "end")
            pub_box.insert("1.0", txt or "")
        except Exception:
            pass

    if private_b64_state["value"]:
        _set_pub_text(
            _render_public_from_private_b64(private_b64_state["value"])
        )

    def generate_keys():
        priv = ed25519.Ed25519PrivateKey.generate()
        pub = priv.public_key()

        seed = priv.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        pub_raw = pub.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )

        private_b64_state["value"] = base64.b64encode(seed).decode("utf-8")
        _set_pub_text(base64.b64encode(pub_raw).decode("utf-8"))

        messagebox.showinfo(
            "Step 1 complete",
            "Public/Private keys generated.\n\n"
            "Next (Robinhood):\n"
            "  1) Click 'Copy Public Key' in this window\n"
            "  2) On Robinhood (desktop web): Account → Settings → Crypto\n"
            "  3) Scroll to 'API Trading' → click '+ Add Key'\n"
            "  4) Paste the Public Key (base64) into the 'Public key' field\n"
            "  5) Enable permissions READ + TRADE (this trader needs both), then Save\n"
            "  6) Robinhood shows an API Key (usually starts with 'rh...') — copy it right away\n\n"
            "Then come back here and paste that API Key into the 'API Key' box.",
        )

    def copy_public_key():
        txt = (pub_box.get("1.0", "end") or "").strip()
        if not txt:
            messagebox.showwarning(
                "Nothing to copy", "Click 'Generate Keys' first."
            )
            return
        _copy_to_clipboard(txt, title="Public Key copied")

    step1_btns = ttk.Frame(step1)
    step1_btns.grid(row=2, column=0, sticky="w", padx=10, pady=(0, 10))
    ttk.Button(step1_btns, text="Generate Keys", command=generate_keys).pack(
        side="left"
    )
    ttk.Button(
        step1_btns, text="Copy Public Key", command=copy_public_key
    ).pack(side="left", padx=8)

    # ---- Step 2 — Paste API key (from Robinhood) ----

    step2 = ttk.LabelFrame(
        container, text="Step 2 — Paste your Robinhood API Key here"
    )
    step2.grid(row=3, column=0, sticky="nsew", pady=(0, 10))
    step2.columnconfigure(0, weight=1)

    step2_help = (
        "In Robinhood, after you add the Public Key, Robinhood will show an API Key.\n"
        "Paste that API Key below. (It often starts with 'rh.'.)"
    )
    ttk.Label(step2, text=step2_help, justify="left").grid(
        row=0, column=0, sticky="w", padx=10, pady=(8, 0)
    )

    api_key_var = tk.StringVar(value=existing_api_key or "")
    api_ent = ttk.Entry(step2, textvariable=api_key_var)
    api_ent.grid(row=1, column=0, sticky="ew", padx=10, pady=(6, 10))

    def _test_credentials() -> None:
        api_key = (api_key_var.get() or "").strip()
        priv_b64 = (private_b64_state.get("value") or "").strip()

        if not requests:
            messagebox.showerror(
                "Missing dependency",
                "The 'requests' package is required for the Test button.\n\n"
                "Fix: pip install requests\n\n"
                "(You can still Save without testing.)",
            )
            return

        if not priv_b64:
            messagebox.showerror(
                "Missing private key", "Step 1: click 'Generate Keys' first."
            )
            return
        if not api_key:
            messagebox.showerror(
                "Missing API key",
                "Paste the API key from Robinhood into Step 2 first.",
            )
            return

        base_url = "https://trading.robinhood.com"
        path = "/api/v1/crypto/marketdata/best_bid_ask/?symbol=BTC-USD"
        method = "GET"
        body = ""
        ts = int(time.time())
        msg = f"{api_key}{ts}{path}{method}{body}".encode("utf-8")

        try:
            raw = base64.b64decode(priv_b64)
            if len(raw) == 64:
                seed = raw[:32]
            elif len(raw) == 32:
                seed = raw
            else:
                raise ValueError(
                    f"Unexpected private key length: {len(raw)} bytes (expected 32 or 64)"
                )
            pk = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
            sig_b64 = base64.b64encode(pk.sign(msg)).decode("utf-8")
        except Exception as e:
            messagebox.showerror(
                "Bad private key",
                f"Couldn't use your private key (r_secret.txt).\n\nError:\n{e}",
            )
            return

        headers = {
            "x-api-key": api_key,
            "x-timestamp": str(ts),
            "x-signature": sig_b64,
            "Content-Type": "application/json",
        }

        try:
            resp = requests.get(
                f"{base_url}{path}", headers=headers, timeout=10
            )
            if resp.status_code >= 400:
                hint = ""
                if resp.status_code in (401, 403):
                    hint = (
                        "\n\nCommon fixes:\n"
                        "  • Make sure you pasted the API Key (not the public key).\n"
                        "  • In Robinhood, ensure the key has permissions READ + TRADE.\n"
                        "  • If you just created the key, wait 30–60 seconds and try again.\n"
                    )
                messagebox.showerror(
                    "Test failed",
                    f"Robinhood returned HTTP {resp.status_code}.\n\n{resp.text}{hint}",
                )
                return

            data = resp.json()
            ask = None
            try:
                if data.get("results"):
                    ask = data["results"][0].get("ask_inclusive_of_buy_spread")
            except Exception:
                pass

            messagebox.showinfo(
                "Test successful",
                "✅ Your API Key + Private Key worked!\n\n"
                "Robinhood responded successfully.\n"
                f"BTC-USD ask (example): {ask if ask is not None else 'received'}\n\n"
                "Next: click Save.",
            )
        except Exception as e:
            messagebox.showerror(
                "Test failed", f"Couldn't reach Robinhood.\n\nError:\n{e}"
            )

    step2_btns = ttk.Frame(step2)
    step2_btns.grid(row=2, column=0, sticky="w", padx=10, pady=(0, 10))
    ttk.Button(
        step2_btns,
        text="Test Credentials (safe, no trading)",
        command=_test_credentials,
    ).pack(side="left")

    # ---- Step 3 — Save ----

    step3 = ttk.LabelFrame(container, text="Step 3 — Save to files (required)")
    step3.grid(row=4, column=0, sticky="nsew")
    step3.columnconfigure(0, weight=1)

    ack_var = tk.BooleanVar(value=False)
    ack = ttk.Checkbutton(
        step3,
        text="I understand r_secret.txt is PRIVATE and I will not share it.",
        variable=ack_var,
    )
    ack.grid(row=0, column=0, sticky="w", padx=10, pady=(10, 6))

    save_btns = ttk.Frame(step3)
    save_btns.grid(row=1, column=0, sticky="w", padx=10, pady=(0, 12))

    def do_save():
        api_key = (api_key_var.get() or "").strip()
        priv_b64 = (private_b64_state.get("value") or "").strip()

        if not priv_b64:
            messagebox.showerror(
                "Missing private key", "Step 1: click 'Generate Keys' first."
            )
            return

        try:
            raw = base64.b64decode(priv_b64)
            if len(raw) == 64:
                raw = raw[:32]
                priv_b64 = base64.b64encode(raw).decode("utf-8")
                private_b64_state["value"] = priv_b64
            elif len(raw) != 32:
                messagebox.showerror(
                    "Bad private key",
                    f"Your private key decodes to {len(raw)} bytes, but it must be 32 bytes.\n\n"
                    "Click 'Generate Keys' again to create a fresh keypair.",
                )
                return
        except Exception as e:
            messagebox.showerror(
                "Bad private key",
                f"Couldn't decode the private key as base64.\n\nError:\n{e}",
            )
            return

        if not api_key:
            messagebox.showerror(
                "Missing API key",
                "Step 2: paste your API key from Robinhood first.",
            )
            return
        if not bool(ack_var.get()):
            messagebox.showwarning(
                "Please confirm",
                "For safety, please check the box confirming you understand r_secret.txt is private.",
            )
            return

        if len(api_key) < 10:
            if not messagebox.askyesno(
                "API key looks short",
                "That API key looks unusually short. Are you sure you pasted the API Key from Robinhood?",
            ):
                return

        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            if os.path.isfile(key_path):
                shutil.copy2(key_path, f"{key_path}.bak_{ts}")
            if os.path.isfile(secret_path):
                shutil.copy2(secret_path, f"{secret_path}.bak_{ts}")
        except Exception:
            pass

        try:
            with open(key_path, "w", encoding="utf-8") as f:
                f.write(api_key)
            with open(secret_path, "w", encoding="utf-8") as f:
                f.write(priv_b64)
        except Exception as e:
            messagebox.showerror(
                "Save failed",
                f"Couldn't write the credential files.\n\nError:\n{e}",
            )
            return

        on_credentials_changed()
        messagebox.showinfo(
            "Saved",
            "✅ Saved!\n\n"
            "The trader will automatically read these files next time it starts:\n"
            f"  API Key → {_mask_path(key_path)}\n"
            f"  Private Key → {_mask_path(secret_path)}\n\n"
            "Next steps:\n"
            "  1) Close this window\n"
            "  2) Start the trader (pt_trader.py)\n"
            "If something fails, come back here and click 'Test Credentials'.",
        )
        wiz.destroy()

    ttk.Button(save_btns, text="Save", command=do_save).pack(side="left")
    ttk.Button(save_btns, text="Close", command=wiz.destroy).pack(
        side="left", padx=8
    )


def build_robinhood_section(
    settings_frame: ttk.Frame,
    row: int,
    settings_window: tk.Toplevel,
    project_dir: str,
    scaled_geometry_fn: Callable,
    theme: dict,
) -> ttk.Frame:
    """
    Build the Robinhood API credential section in the settings dialog.

    Returns the rh_section frame so the caller can show/hide it based on
    the exchange selection.
    """
    api_status_var = tk.StringVar(value="")

    def _refresh_api_status() -> None:
        key_path, secret_path = _api_paths(project_dir)
        k, s = _read_api_files(project_dir)

        missing = []
        if not k:
            missing.append("r_key.txt (API Key)")
        if not s:
            missing.append("r_secret.txt (PRIVATE key)")

        if missing:
            api_status_var.set(
                "Not configured ❌ (missing " + ", ".join(missing) + ")"
            )
        else:
            api_status_var.set("Configured ✅ (credentials found)")

    def _open_api_folder() -> None:
        try:
            folder = os.path.abspath(project_dir)
            if os.name == "nt":
                os.startfile(folder)  # type: ignore[attr-defined]
                return
            if sys.platform == "darwin":
                subprocess.Popen(["open", folder])
                return
            subprocess.Popen(["xdg-open", folder])
        except Exception as e:
            messagebox.showerror(
                "Couldn't open folder",
                f"Tried to open:\n{project_dir}\n\nError:\n{e}",
            )

    def _clear_api_files() -> None:
        key_path, secret_path = _api_paths(project_dir)
        if not messagebox.askyesno(
            "Delete API credentials?",
            "This will delete:\n"
            f"  {key_path}\n"
            f"  {secret_path}\n\n"
            "After deleting, the trader can NOT authenticate until you run the setup wizard again.\n\n"
            "Are you sure you want to delete these files?",
        ):
            return

        try:
            if os.path.isfile(key_path):
                os.remove(key_path)
            if os.path.isfile(secret_path):
                os.remove(secret_path)
        except Exception as e:
            messagebox.showerror(
                "Delete failed", f"Couldn't delete the files:\n\n{e}"
            )
            return

        _refresh_api_status()
        messagebox.showinfo("Deleted", "Deleted r_key.txt and r_secret.txt.")

    rh_section = ttk.Frame(settings_frame)
    rh_section.grid(row=row, column=0, columnspan=3, sticky="ew")
    rh_section.columnconfigure(1, weight=1)

    ttk.Label(rh_section, text="Robinhood API:").grid(
        row=0, column=0, sticky="w", padx=(0, 10), pady=6
    )

    api_row = ttk.Frame(rh_section)
    api_row.grid(row=0, column=1, columnspan=2, sticky="ew", pady=6)
    api_row.columnconfigure(0, weight=1)

    ttk.Label(api_row, textvariable=api_status_var).grid(
        row=0, column=0, sticky="w"
    )
    ttk.Button(
        api_row,
        text="Setup Wizard",
        command=lambda: _open_robinhood_api_wizard(
            settings_window, project_dir, scaled_geometry_fn, theme,
            _refresh_api_status,
        ),
    ).grid(row=0, column=1, sticky="e", padx=(10, 0))
    ttk.Button(api_row, text="Open Folder", command=_open_api_folder).grid(
        row=0, column=2, sticky="e", padx=(8, 0)
    )
    ttk.Button(api_row, text="Clear", command=_clear_api_files).grid(
        row=0, column=3, sticky="e", padx=(8, 0)
    )

    _refresh_api_status()

    return rh_section
