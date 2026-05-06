#!/usr/bin/env python3
"""
Reconciliation: compare baseline (pre-refactor) vs refactored pt_trainer outputs.

Tests both from-scratch training and retraining on existing state.

Usage:
    python run_pt_trainer_reconciliation.py BTC
    python run_pt_trainer_reconciliation.py BTC --source kucoin
    python run_pt_trainer_reconciliation.py BTC --skip-baseline  # refactored only

Structure:
    reconciliation/<coin>/from_scratch/baseline/   — old trainer, empty start
    reconciliation/<coin>/from_scratch/refactored/ — new trainer, empty start
    reconciliation/<coin>/retrain/baseline/        — old trainer on from_scratch baseline
    reconciliation/<coin>/retrain/refactored/      — new trainer on from_scratch refactored

Outputs go to reconciliation/ — state/ is never modified.
Baseline trainer: reconciliation/pt_trainer_baseline.py (extracted from main, execl disabled).
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time as time_mod
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent
BASELINE_TRAINER = PROJECT_ROOT / "reconciliation" / "pt_trainer_baseline.py"

sys.path.insert(0, str(PROJECT_ROOT))

QUANT_ROOT = PROJECT_ROOT.parent / "secret"
if QUANT_ROOT.exists():
    sys.path.insert(0, str(QUANT_ROOT))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Reconciliation: baseline vs refactored trainer comparison",
        epilog="Outputs go to reconciliation/<coin>/. state/ is never modified.",
    )
    parser.add_argument("coin", help="Coin to train (e.g. BTC, ETH)")
    parser.add_argument("--source", default="kucoin_live_api",
                        choices=["kucoin_live_api", "kucoin", "binance", "kraken"],
                        help="Data source (default: kucoin_live_api)")
    parser.add_argument("--skip-baseline", action="store_true",
                        help="Skip baseline (old code) runs — only run refactored")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# State loading and comparison
# ---------------------------------------------------------------------------

def load_state(directory: Path, tf_name: str) -> dict:
    """Load training outputs from a directory."""
    result = {"exists": False}

    mem_path = directory / f"memories_{tf_name}.txt"
    if not mem_path.exists():
        return result

    result["exists"] = True

    text = mem_path.read_text(encoding="utf-8", errors="ignore")
    entries = [e.strip() for e in text.split("~") if e.strip()]
    result["memory_count"] = len(entries)

    patterns = []
    for entry in entries:
        parts = entry.split("{}")
        if len(parts) >= 1:
            vals = parts[0].strip().split()
            try:
                patterns.append(tuple(round(float(v), 6) for v in vals if v.strip()))
            except ValueError:
                continue
    result["patterns"] = patterns

    thresh_path = directory / f"neural_perfect_threshold_{tf_name}.txt"
    if thresh_path.exists():
        try:
            result["threshold"] = float(thresh_path.read_text().strip())
        except ValueError:
            result["threshold"] = None
    else:
        result["threshold"] = None

    for prefix in ("memory_weights", "memory_weights_high", "memory_weights_low"):
        w_path = directory / f"{prefix}_{tf_name}.txt"
        if w_path.exists():
            vals = [float(x) for x in w_path.read_text().split() if x.strip()]
            result[prefix] = vals
        else:
            result[prefix] = []

    return result


def verify_thinker_parseable(directory: Path, tf_name: str) -> bool:
    """Verify output files can be parsed exactly as pt_thinker does."""
    try:
        mem_path = directory / f"memories_{tf_name}.txt"
        if not mem_path.exists():
            return False

        text = mem_path.read_text()
        memory_list = (
            text.replace("'", "").replace(",", "").replace('"', "")
            .replace("]", "").replace("[", "").split("~")
        )

        for entry in memory_list:
            if not entry.strip():
                continue
            parts = entry.split("{}")
            if len(parts) < 3:
                return False
            pattern = parts[0].strip().split(" ")
            _ = float(pattern[-1])
            _ = float(parts[1].strip()) / 100
            _ = float(parts[2].strip()) / 100

        for prefix in ("memory_weights", "memory_weights_high", "memory_weights_low"):
            w_path = directory / f"{prefix}_{tf_name}.txt"
            if w_path.exists():
                text = w_path.read_text()
                vals = (text.replace("'", "").replace(",", "").replace('"', "")
                        .replace("]", "").replace("[", "").split(" "))
                for v in vals:
                    if v.strip():
                        _ = float(v)

        return True
    except Exception as e:
        print(f"    Parse error: {e}")
        return False


def compare_states(a: dict, b: dict, tf_name: str) -> dict:
    """Compare two training outputs (a=baseline, b=refactored)."""
    comparison = {"timeframe": tf_name}

    if not a["exists"]:
        comparison["status"] = "NO_OUTPUT"
        comparison["note"] = "baseline produced no output"
        return comparison

    if not b["exists"]:
        comparison["status"] = "NO_OUTPUT"
        comparison["note"] = "refactored produced no output"
        return comparison

    a_count = a["memory_count"]
    b_count = b["memory_count"]
    count_ratio = b_count / a_count if a_count > 0 else float("inf")
    comparison["memory_count_baseline"] = a_count
    comparison["memory_count_refactored"] = b_count
    comparison["memory_count_ratio"] = round(count_ratio, 3)

    a_set = set(a["patterns"])
    b_set = set(b["patterns"])
    intersection = a_set & b_set
    union = a_set | b_set
    jaccard = len(intersection) / len(union) if union else 1.0
    comparison["pattern_jaccard"] = round(jaccard, 4)
    comparison["patterns_shared"] = len(intersection)
    comparison["patterns_baseline_only"] = len(a_set - b_set)
    comparison["patterns_refactored_only"] = len(b_set - a_set)

    if a["threshold"] is not None and b["threshold"] is not None:
        comparison["threshold_baseline"] = round(a["threshold"], 4)
        comparison["threshold_refactored"] = round(b["threshold"], 4)
        comparison["threshold_diff"] = round(b["threshold"] - a["threshold"], 4)

    for prefix in ("memory_weights", "memory_weights_high", "memory_weights_low"):
        a_w = a.get(prefix, [])
        b_w = b.get(prefix, [])
        label = prefix.replace("memory_weights", "wt").replace("_", "")
        if a_w:
            comparison[f"{label}_baseline_mean"] = round(np.mean(a_w), 4)
        if b_w:
            comparison[f"{label}_refactored_mean"] = round(np.mean(b_w), 4)

    if jaccard >= 0.8 and 0.7 <= count_ratio <= 1.3:
        comparison["status"] = "PASS"
    elif jaccard >= 0.5 and 0.5 <= count_ratio <= 2.0:
        comparison["status"] = "ACCEPTABLE"
    else:
        comparison["status"] = "DIVERGED"

    return comparison


# ---------------------------------------------------------------------------
# Training runners
# ---------------------------------------------------------------------------

def run_baseline(coin: str, output_dir: Path):
    """Run the old (main branch) trainer as a subprocess.

    The old trainer writes all files relative to cwd — safe as long as
    cwd is the reconciliation output directory, not state/.
    """
    assert "reconciliation" in str(output_dir), \
        f"Safety check: baseline must run under reconciliation/, got {output_dir}"

    env = os.environ.copy()
    env["POWERTRADER_GUI_SETTINGS"] = str(PROJECT_ROOT / "gui_settings.json")

    proc = subprocess.run(
        [sys.executable, str(BASELINE_TRAINER), coin],
        cwd=str(output_dir), env=env,
        capture_output=True, text=True, timeout=1800,
    )

    if proc.returncode != 0 and "TRAINING FAILED" in (proc.stdout + proc.stderr):
        raise RuntimeError(
            f"Baseline training failed:\n{proc.stdout[-2000:]}\n{proc.stderr[-500:]}")

    return proc.stdout


def run_refactored(coin: str, source: str, output_dir: Path):
    """Run the refactored trainer in-process (all timeframes, matching baseline)."""
    import pt_trainer

    old_cwd = os.getcwd()
    os.chdir(str(output_dir))

    try:
        config = pt_trainer.TrainerConfig(coin=coin, data_source=source)
        loop = pt_trainer.TrainingLoop(config)
        loop.run()
    except SystemExit:
        pass
    finally:
        os.chdir(old_cwd)


def seed_dir(target: Path, source: Path):
    """Copy .txt training artifacts from source to target."""
    if not source.exists():
        return
    for src in source.glob("*.txt"):
        shutil.copy2(src, target / src.name)


# ---------------------------------------------------------------------------
# Phase runner
# ---------------------------------------------------------------------------

def run_phase(phase_name: str, coin: str, source: str, base_dir: Path,
              seed_from_baseline: Path = None,
              seed_from_refactored: Path = None, skip_baseline: bool = False):
    """Run one reconciliation phase (from_scratch or retrain).

    Returns (timing_dict, results_list).
    """
    baseline_dir = base_dir / "baseline"
    refactored_dir = base_dir / "refactored"

    for d in (baseline_dir, refactored_dir):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)

    if seed_from_baseline:
        seed_dir(baseline_dir, seed_from_baseline)
    if seed_from_refactored:
        seed_dir(refactored_dir, seed_from_refactored)

    timing = {}

    # --- Baseline ---
    if not skip_baseline:
        print(f"\n  [{phase_name}] Running BASELINE (old code)...")
        t0 = time_mod.time()
        try:
            run_baseline(coin, baseline_dir)
        except Exception as e:
            print(f"  [{phase_name}] Baseline FAILED: {e}")
            timing["baseline_seconds"] = round(time_mod.time() - t0, 1)
            timing["baseline_error"] = str(e)
            return timing, []
        timing["baseline_seconds"] = round(time_mod.time() - t0, 1)
        print(f"  [{phase_name}] Baseline done in {timing['baseline_seconds']:.1f}s")
    else:
        print(f"\n  [{phase_name}] Skipping baseline (--skip-baseline)")

    # --- Refactored ---
    print(f"  [{phase_name}] Running REFACTORED...")
    t0 = time_mod.time()
    try:
        run_refactored(coin, source, refactored_dir)
    except Exception as e:
        print(f"  [{phase_name}] Refactored FAILED: {e}")
        timing["refactored_seconds"] = round(time_mod.time() - t0, 1)
        timing["refactored_error"] = str(e)
        return timing, []
    timing["refactored_seconds"] = round(time_mod.time() - t0, 1)
    print(f"  [{phase_name}] Refactored done in {timing['refactored_seconds']:.1f}s")

    if timing.get("baseline_seconds") and timing["refactored_seconds"]:
        speedup = timing["baseline_seconds"] / timing["refactored_seconds"]
        timing["speedup"] = round(speedup, 2)
        print(f"  [{phase_name}] Speedup: {speedup:.2f}x")

    # --- Compare (all TFs, since both baseline and refactored run everything) ---
    from pt_trainer import TIMEFRAMES as ALL_TF
    compare_tfs = ALL_TF
    results = []

    for tf in compare_tfs:
        if skip_baseline:
            b = load_state(refactored_dir, tf)
            comp = {"timeframe": tf, "status": "SKIP_BASELINE"}
            if b["exists"]:
                comp["memory_count"] = b["memory_count"]
                comp["threshold"] = b.get("threshold")
                comp["thinker_parseable"] = verify_thinker_parseable(refactored_dir, tf)
            results.append(comp)
        else:
            a = load_state(baseline_dir, tf)
            b = load_state(refactored_dir, tf)
            comp = compare_states(a, b, tf)
            comp["thinker_parseable"] = verify_thinker_parseable(refactored_dir, tf)
            if not comp["thinker_parseable"]:
                comp["status"] = "FORMAT_ERROR"
            results.append(comp)

    return timing, results


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

STATUS_ICON = {"PASS": "✓", "ACCEPTABLE": "~", "DIVERGED": "✗",
               "FORMAT_ERROR": "!", "NO_OUTPUT": "✗", "SKIP_BASELINE": "→"}


def print_results(phase_name: str, timing: dict, results: list) -> bool:
    """Pretty-print comparison results. Returns True if all pass."""
    print(f"\n  {'─' * 60}")
    print(f"  {phase_name} RESULTS")

    parts = []
    if timing.get("baseline_seconds"):
        parts.append(f"baseline={timing['baseline_seconds']:.1f}s")
    if timing.get("refactored_seconds"):
        parts.append(f"refactored={timing['refactored_seconds']:.1f}s")
    if timing.get("speedup"):
        parts.append(f"speedup={timing['speedup']}x")
    if parts:
        print(f"  Timing: {'  '.join(parts)}")
    print(f"  {'─' * 60}")

    all_pass = True

    for r in results:
        icon = STATUS_ICON.get(r["status"], "?")
        print(f"\n    [{icon}] {r['timeframe']}: {r['status']}")

        if r.get("memory_count_baseline") is not None:
            print(f"        Memories:  baseline={r['memory_count_baseline']}  "
                  f"refactored={r['memory_count_refactored']}  "
                  f"ratio={r.get('memory_count_ratio', '?')}")
        elif r.get("memory_count") is not None:
            print(f"        Memories:  {r['memory_count']}")

        if r.get("pattern_jaccard") is not None:
            print(f"        Patterns:  jaccard={r['pattern_jaccard']}  "
                  f"shared={r['patterns_shared']}  "
                  f"baseline_only={r['patterns_baseline_only']}  "
                  f"refactored_only={r['patterns_refactored_only']}")

        if r.get("threshold_baseline") is not None:
            print(f"        Threshold: baseline={r['threshold_baseline']}  "
                  f"refactored={r['threshold_refactored']}  "
                  f"diff={r.get('threshold_diff', '?')}")
        elif r.get("threshold") is not None:
            print(f"        Threshold: {r['threshold']:.4f}")

        parseable = r.get("thinker_parseable")
        if parseable is False:
            print(f"        FORMAT:    pt_thinker CANNOT parse output!")
        elif parseable is True:
            print(f"        Format:    pt_thinker compatible")

        if r["status"] in ("DIVERGED", "FORMAT_ERROR", "NO_OUTPUT"):
            all_pass = False

    return all_pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    coin = args.coin.upper()
    source = args.source
    skip_baseline = args.skip_baseline

    if not skip_baseline and not BASELINE_TRAINER.exists():
        print(f"ERROR: baseline trainer not found at {BASELINE_TRAINER}")
        print("Run: git show main:pt_trainer.py > reconciliation/pt_trainer_baseline.py")
        return 1

    from pt_trainer import TIMEFRAMES
    display_tfs = TIMEFRAMES

    recon_dir = PROJECT_ROOT / "reconciliation" / coin
    if recon_dir.exists():
        shutil.rmtree(recon_dir)
    recon_dir.mkdir(parents=True)

    print(f"{'=' * 70}")
    print(f"RECONCILIATION: {coin} (source={source})")
    print(f"{'=' * 70}")
    print(f"Timeframes:  {', '.join(display_tfs)}")
    print(f"Output:      {recon_dir}")
    if skip_baseline:
        print(f"Mode:        refactored only (--skip-baseline)")
    else:
        print(f"Baseline:    {BASELINE_TRAINER}")
    print()

    all_pass = True
    overall_results = {}

    # --- Phase 1: From scratch ---
    print(f"{'═' * 70}")
    print("PHASE 1: FROM SCRATCH (empty state)")
    print(f"{'═' * 70}")

    from_scratch_dir = recon_dir / "from_scratch"
    timing_fs, results_fs = run_phase(
        "from_scratch", coin, source, from_scratch_dir,
        skip_baseline=skip_baseline,
    )
    phase1_pass = print_results("FROM SCRATCH", timing_fs, results_fs)
    all_pass = all_pass and phase1_pass
    overall_results["from_scratch"] = {"timing": timing_fs, "results": results_fs}

    # --- Phase 2: Retrain ---
    print(f"\n\n{'═' * 70}")
    print("PHASE 2: RETRAIN (training on top of from_scratch output)")
    print(f"{'═' * 70}")

    retrain_dir = recon_dir / "retrain"
    timing_rt, results_rt = run_phase(
        "retrain", coin, source, retrain_dir,
        seed_from_baseline=from_scratch_dir / "baseline",
        seed_from_refactored=from_scratch_dir / "refactored",
        skip_baseline=skip_baseline,
    )
    phase2_pass = print_results("RETRAIN", timing_rt, results_rt)
    all_pass = all_pass and phase2_pass
    overall_results["retrain"] = {"timing": timing_rt, "results": results_rt}

    # --- Summary ---
    print(f"\n\n{'═' * 70}")
    print("SUMMARY")
    print(f"{'═' * 70}")
    print(f"  From scratch: {'PASS' if phase1_pass else 'REVIEW NEEDED'}")
    print(f"  Retrain:      {'PASS' if phase2_pass else 'REVIEW NEEDED'}")

    for label, t in [("From scratch", timing_fs), ("Retrain", timing_rt)]:
        if t.get("baseline_seconds") and t.get("refactored_seconds"):
            print(f"\n  {label} timing:")
            print(f"    Baseline:   {t['baseline_seconds']:.1f}s")
            print(f"    Refactored: {t['refactored_seconds']:.1f}s")
            print(f"    Speedup:    {t.get('speedup', '?')}x")

    overall = "PASS" if all_pass else "REVIEW NEEDED"
    print(f"\n  OVERALL: {overall}")
    print(f"{'═' * 70}")

    results_path = recon_dir / "results.json"
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump({
            "coin": coin,
            "source": source,
            "timeframes": display_tfs,
            "overall": overall,
            **overall_results,
        }, f, indent=2, default=str)

    print(f"\n  Full results: {results_path}")

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
