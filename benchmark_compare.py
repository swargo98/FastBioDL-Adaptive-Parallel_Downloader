#!/usr/bin/env python3
"""
benchmark_compare.py — Compare fastbiodl vs sra-tools vs kingfisher.

Reads timing results from:
  • benchmark_fastbiodl_results_*.json   (produced by fastbiodl_upgrade.py)
  • benchmark_sratools_results_*.json    (produced by benchmark_sratools.py)
  • benchmark_kingfisher_results_*.json  (produced by benchmark_kingfisher.py)

Alternatively, accepts explicit file paths as positional arguments.

Can also parse a raw fastbiodl log file (--parse-log) to extract timings when
the JSON was not saved or when re-running is not possible.

Usage
-----
  # Auto-discover latest result files in current directory:
  python benchmark_compare.py

  # Pass files explicitly:
  python benchmark_compare.py \\
      --fastbiodl  benchmark_fastbiodl_results_20250101_120000.json \\
      --sratools   benchmark_sratools_results_20250101_130000.json \\
      --kingfisher benchmark_kingfisher_results_20250101_140000.json

  # Parse a raw fastbiodl log instead of a JSON:
  python benchmark_compare.py --parse-log logs/receiver.01_01_2025_12_00_00.log \\
      --sratools   benchmark_sratools_results.json \\
      --kingfisher benchmark_kingfisher_results.json
"""

import os
import re
import sys
import json
import glob
import argparse
import datetime
from typing import Optional


# ── Log parser for fastbiodl ─────────────────────────────────────────────────

# fastbiodl log format:  %(created)f -- %(levelname)s: %(message)s
# e.g.: 1735732800.123456 -- INFO: Download Tasks Completed! Success: 5, Failed: 0

_TS_RE = re.compile(r'^(\d+\.\d+)\s+--\s+\w+:\s+(.*)$')


def _parse_fastbiodl_log(log_path: str) -> dict:
    """
    Extract timing from a fastbiodl log file.

    Key lines:
      "Total files to download:"        → t_start  (first meaningful timestamp)
      "Download Tasks Completed!"        → t_download_end
      "[Converter #N] fasterq-dump done" → t_fasterq_end  (last occurrence)
      "[Converter #N] Completed ACC:"    → t_pigz_end     (last occurrence)
    """
    t_start        = None
    t_download_end = None
    t_fasterq_end  = None   # last seen
    t_pigz_end     = None   # last seen

    with open(log_path) as f:
        for line in f:
            m = _TS_RE.match(line.strip())
            if not m:
                continue
            ts, msg = float(m.group(1)), m.group(2)

            if t_start is None:
                t_start = ts   # first line = pipeline start

            if "Total files to download:" in msg and t_start is None:
                t_start = ts

            if "Download Tasks Completed!" in msg:
                t_download_end = ts

            if re.search(r'\[Converter #\d+\] fasterq-dump done', msg):
                if t_fasterq_end is None or ts > t_fasterq_end:
                    t_fasterq_end = ts

            if re.search(r'\[Converter #\d+\] Completed ', msg):
                if t_pigz_end is None or ts > t_pigz_end:
                    t_pigz_end = ts

    if t_start is None:
        raise ValueError(f"Could not find start timestamp in {log_path}")

    t_dl_end  = t_download_end or t_start
    t_fq_end  = t_fasterq_end  or t_dl_end
    t_pz_end  = t_pigz_end     or t_fq_end

    # The log does not record per-phase start timestamps directly, so for
    # sequential ordering we approximate: each phase starts where the previous
    # one ended.  download starts at t_start; conversion starts when the last
    # download completes; compression starts when the last fasterq-dump completes.
    # This is conservative (no overlap assumed) for log-parsed data.
    phases = {
        "download":    {"start": t_start,  "end": t_dl_end,
                        "duration_s": round(t_dl_end - t_start,  2)},
        "conversion":  {"start": t_dl_end, "end": t_fq_end,
                        "duration_s": round(max(0, t_fq_end - t_dl_end), 2)},
        "compression": {"start": t_fq_end, "end": t_pz_end,
                        "duration_s": round(max(0, t_pz_end - t_fq_end), 2)},
    }

    return {
        "tool":               "fastbiodl (log-parsed)",
        "phases":             phases,
        "download_time_s":    phases["download"]["duration_s"],
        "conversion_time_s":  phases["conversion"]["duration_s"],
        "compression_time_s": phases["compression"]["duration_s"],
        "total_time_s":       round(t_pz_end - t_start, 2),
    }


# ── File discovery ───────────────────────────────────────────────────────────

def _latest_json(pattern: str) -> Optional[str]:
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None


# ── Formatting ───────────────────────────────────────────────────────────────

def _bar(value: float, max_val: float, width: int = 30) -> str:
    if max_val == 0:
        return " " * width
    filled = int(round(value / max_val * width))
    return "█" * filled + "░" * (width - filled)


def _fmt(seconds: float) -> str:
    if seconds >= 3600:
        return f"{seconds/3600:.2f}h"
    if seconds >= 60:
        return f"{seconds/60:.1f}m"
    return f"{seconds:.1f}s"


def _overlap(s1: float, e1: float, s2: float, e2: float) -> float:
    """Overlap in seconds between [s1,e1] and [s2,e2]. Returns 0 if either interval is unset."""
    if s1 == 0.0 or s2 == 0.0:
        return 0.0
    return max(0.0, min(e1, e2) - max(s1, s2))


def _extract_phases(r: dict) -> dict:
    """
    Return a normalised phases dict with {download, conversion, compression}
    each containing {start, end, duration_s}.

    Accepts both the new `phases` key (fastbiodl) and the legacy flat keys
    (sra-tools / kingfisher sequential runs, where start = prev end).
    """
    if "phases" in r:
        return r["phases"]

    # Sequential tools: reconstruct windows from the stored epoch timestamps.
    # sra-tools / kingfisher store t_start, t_download_end, t_fasterq_end, t_pigz_end.
    t_s  = r.get("t_start",        0.0)
    t_dl = r.get("t_download_end",  t_s)
    t_fq = r.get("t_fasterq_end",   t_dl)
    t_pz = r.get("t_pigz_end",      t_fq)
    return {
        "download":    {"start": t_s,  "end": t_dl, "duration_s": round(t_dl - t_s,  2)},
        "conversion":  {"start": t_dl, "end": t_fq, "duration_s": round(t_fq - t_dl, 2)},
        "compression": {"start": t_fq, "end": t_pz, "duration_s": round(t_pz - t_fq, 2)},
    }


def _print_table(results: list[dict]):
    """Print phase durations, overlap matrix, bar chart, and speedup."""

    tools  = [r["tool"] for r in results]
    phases = [_extract_phases(r) for r in results]
    dl     = [p["download"]["duration_s"]    for p in phases]
    cv     = [p["conversion"]["duration_s"]  for p in phases]
    cp     = [p["compression"]["duration_s"] for p in phases]
    total  = [r["total_time_s"]              for r in results]

    max_total = max(total) if total else 1
    col_w = max(len(t) for t in tools) + 2

    # ── Duration table ────────────────────────────────────────────────────────
    header = f"{'Tool':<{col_w}}  {'Download':>10}  {'Conversion':>11}  {'Compression':>12}  {'Total':>8}"
    print()
    print("=" * len(header))
    print("  BENCHMARK COMPARISON — Phase Durations")
    print("=" * len(header))
    print(header)
    print("-" * len(header))
    for i, r in enumerate(results):
        print(
            f"{tools[i]:<{col_w}}  "
            f"{_fmt(dl[i]):>10}  "
            f"{_fmt(cv[i]):>11}  "
            f"{_fmt(cp[i]):>12}  "
            f"{_fmt(total[i]):>8}"
        )
    print("=" * len(header))

    # ── Overlap matrix ────────────────────────────────────────────────────────
    print()
    print("  OVERLAP BETWEEN PHASES (per tool)")
    print(f"  {'Tool':<{col_w}}  {'DL∩Conv':>9}  {'DL∩Comp':>9}  {'Conv∩Comp':>10}")
    print(f"  {'-'*col_w}  {'-'*9}  {'-'*9}  {'-'*10}")
    for i, p in enumerate(phases):
        dl_s, dl_e = p["download"]["start"],    p["download"]["end"]
        cv_s, cv_e = p["conversion"]["start"],  p["conversion"]["end"]
        cp_s, cp_e = p["compression"]["start"], p["compression"]["end"]
        ov_dl_cv = _overlap(dl_s, dl_e, cv_s, cv_e)
        ov_dl_cp = _overlap(dl_s, dl_e, cp_s, cp_e)
        ov_cv_cp = _overlap(cv_s, cv_e, cp_s, cp_e)
        print(
            f"  {tools[i]:<{col_w}}  "
            f"{_fmt(ov_dl_cv):>9}  "
            f"{_fmt(ov_dl_cp):>9}  "
            f"{_fmt(ov_cv_cp):>10}"
        )
    print()
    print("  Note: sequential tools (sra-tools, kingfisher) have 0s overlap by design.")
    print("        fastbiodl overlaps reflect genuine pipeline concurrency.")

    # ── Bar chart ─────────────────────────────────────────────────────────────
    print()
    print("  WALL-CLOCK BREAKDOWN  (bar width ∝ max total =", _fmt(max_total), ")")
    print()
    for i, r in enumerate(results):
        dl_bar = _bar(dl[i], max_total, 12)
        cv_bar = _bar(cv[i], max_total, 10)
        cp_bar = _bar(cp[i], max_total, 8)
        print(f"  {tools[i]:<{col_w}}")
        print(f"    {'Download':12} [{dl_bar}] {_fmt(dl[i])}")
        print(f"    {'Conversion':12} [{cv_bar}] {_fmt(cv[i])}")
        print(f"    {'Compression':12} [{cp_bar}] {_fmt(cp[i])}")
        print(f"    {'Total':12} {'─'*30}  {_fmt(total[i])}")
        print()

    # ── Speedup ───────────────────────────────────────────────────────────────
    if len(results) > 1:
        slowest = max(total)
        print("  SPEEDUP vs slowest:")
        for i, r in enumerate(results):
            speedup = slowest / total[i] if total[i] > 0 else float("inf")
            print(f"    {tools[i]:<{col_w}}  {speedup:.2f}x")
        print()

    # ── Per-stage winner ──────────────────────────────────────────────────────
    print("  FASTEST PER STAGE:")
    for values, label in [(dl, "download"), (cv, "conversion"),
                          (cp, "compression"), (total, "total")]:
        if any(v > 0 for v in values):
            wi = min(range(len(values)), key=lambda i: values[i])
            print(f"    {label:<14}  → {tools[wi]}  ({_fmt(values[wi])})")
    print()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compare fastbiodl / sra-tools / kingfisher benchmark results",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--fastbiodl",
                        help="fastbiodl result JSON (auto-discovered if omitted)")
    parser.add_argument("--sratools",
                        help="sra-tools result JSON (auto-discovered if omitted)")
    parser.add_argument("--kingfisher",
                        help="kingfisher result JSON (auto-discovered if omitted)")
    parser.add_argument("--parse-log",
                        help="Parse a raw fastbiodl .log file to extract timings "
                             "(use when JSON was not saved)")
    parser.add_argument("--output-json",
                        help="Save merged comparison to this JSON file")
    args = parser.parse_args()

    results: list[dict] = []

    # ── fastbiodl ─────────────────────────────────────────────────────────────
    if args.parse_log:
        try:
            r = _parse_fastbiodl_log(args.parse_log)
            results.append(r)
            print(f"[✓] Parsed fastbiodl timings from log: {args.parse_log}")
        except Exception as e:
            print(f"[✗] Failed to parse log {args.parse_log}: {e}")
    else:
        fb_file = args.fastbiodl or _latest_json("benchmark_fastbiodl_results_*.json")
        if fb_file and os.path.exists(fb_file):
            with open(fb_file) as f:
                results.append(json.load(f))
            print(f"[✓] Loaded fastbiodl results: {fb_file}")
        else:
            print("[–] No fastbiodl results found (run fastbiodl_upgrade.py first, "
                  "or use --parse-log)")

    # ── sra-tools ─────────────────────────────────────────────────────────────
    st_file = args.sratools or _latest_json("benchmark_sratools_results_*.json")
    if st_file and os.path.exists(st_file):
        with open(st_file) as f:
            results.append(json.load(f))
        print(f"[✓] Loaded sra-tools results:  {st_file}")
    else:
        print("[–] No sra-tools results found (run benchmark_sratools.py first)")

    # ── kingfisher ────────────────────────────────────────────────────────────
    kf_file = args.kingfisher or _latest_json("benchmark_kingfisher_results_*.json")
    if kf_file and os.path.exists(kf_file):
        with open(kf_file) as f:
            results.append(json.load(f))
        print(f"[✓] Loaded kingfisher results: {kf_file}")
    else:
        print("[–] No kingfisher results found (run benchmark_kingfisher.py first)")

    if not results:
        print("\nNo results loaded. Nothing to compare.")
        sys.exit(1)

    # ── Print comparison ──────────────────────────────────────────────────────
    _print_table(results)

    # ── Optional merged JSON ──────────────────────────────────────────────────
    if args.output_json:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out = args.output_json if args.output_json.endswith(".json") \
              else f"{args.output_json}_{ts}.json"
        with open(out, "w") as f:
            json.dump({"comparison": results}, f, indent=2)
        print(f"Merged results saved → {out}")


if __name__ == "__main__":
    main()