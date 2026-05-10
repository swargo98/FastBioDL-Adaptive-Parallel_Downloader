#!/usr/bin/env python3
"""
benchmark_compare.py

Log-driven benchmark comparison for FastBioDL vs other methods.

Default behavior:
  - Auto-discovers benchmark JSONs under logs/** and in repo root.
  - Groups repeated runs by accession set and method.
  - Prints paper-ready statistics, including required speedups:
      1) Total speedup
      2) Download-only speedup
      3) Post-download speedup: (total - download)

Supported methods (auto-normalized from tool names in JSON):
  fastbiodl, sratools, kingfisher, aria2c, pysradb

Usage examples:
  python benchmark_compare.py
  python benchmark_compare.py --logs-root logs --output-json compare_report.json
  python benchmark_compare.py --methods fastbiodl aria2c pysradb
  python benchmark_compare.py --accession-sets accessions_large_PRJNA251383
  python benchmark_compare.py --files path/to/result1.json path/to/result2.json
  python benchmark_compare.py --parse-log logs/fastbiodl/.../fastbiodl.xxx.log
"""

from __future__ import annotations

import argparse
import datetime
import glob
import json
import math
import os
import re
import statistics
import sys
from dataclasses import dataclass
from typing import Any, Optional


_TS_RE = re.compile(r"^(\d+\.\d+)\s+--\s+\w+:\s+(.*)$")
_ACC_SET_RE = re.compile(r"(accessions_[^/\\]+)")
_STAMP_RE = re.compile(r"(\d{8}_\d{6})")


def _fmt_s(seconds: float) -> str:
    if seconds >= 3600:
        return f"{seconds / 3600:.2f}h"
    if seconds >= 60:
        return f"{seconds / 60:.1f}m"
    return f"{seconds:.1f}s"


def _fmt_mu_sigma(mu: float, sd: float, n: int) -> str:
    if n <= 1:
        return f"{_fmt_s(mu):>8}"
    return f"{_fmt_s(mu):>8} +- {_fmt_s(sd):<8}"


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: list[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def _overlap(s1: float, e1: float, s2: float, e2: float) -> float:
    if s1 == 0.0 or s2 == 0.0:
        return 0.0
    return max(0.0, min(e1, e2) - max(s1, s2))


def _extract_accession_set(path: str, payload: dict[str, Any]) -> str:
    m = _ACC_SET_RE.search(path)
    if m:
        return m.group(1)
    acc = payload.get("accessions") or []
    if isinstance(acc, list):
        return f"accessions_n{len(acc)}"
    return "accessions_unknown"


def _normalize_method(tool_name: str) -> str:
    t = (tool_name or "unknown").strip().lower()
    if "fastbiodl" in t:
        return "fastbiodl"
    if t in {"sra-tools", "sratools", "sra_tools"}:
        return "sratools"
    if "kingfisher" in t:
        return "kingfisher"
    if "aria2c" in t:
        return "aria2c"
    if "pysradb" in t:
        return "pysradb"
    return re.sub(r"[^a-z0-9]+", "_", t).strip("_") or "unknown"


def _extract_phases(payload: dict[str, Any]) -> dict[str, dict[str, float]]:
    if "phases" in payload and isinstance(payload["phases"], dict):
        p = payload["phases"]
        out: dict[str, dict[str, float]] = {}
        for k in ("download", "conversion", "compression"):
            pk = p.get(k, {})
            s = float(pk.get("start", 0.0) or 0.0)
            e = float(pk.get("end", s) or s)
            d = float(pk.get("duration_s", max(0.0, e - s)) or 0.0)
            out[k] = {"start": s, "end": e, "duration_s": max(0.0, d)}
        return out

    t_s = float(payload.get("t_start", 0.0) or 0.0)
    t_dl = float(payload.get("t_download_end", t_s) or t_s)
    t_fq = float(payload.get("t_fasterq_end", t_dl) or t_dl)
    t_pz = float(payload.get("t_pigz_end", t_fq) or t_fq)
    return {
        "download": {"start": t_s, "end": t_dl, "duration_s": max(0.0, t_dl - t_s)},
        "conversion": {"start": t_dl, "end": t_fq, "duration_s": max(0.0, t_fq - t_dl)},
        "compression": {"start": t_fq, "end": t_pz, "duration_s": max(0.0, t_pz - t_fq)},
    }


def _parse_fastbiodl_log(log_path: str) -> dict[str, Any]:
    t_start = None
    t_download_end = None
    t_fasterq_end = None
    t_pigz_end = None

    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            m = _TS_RE.match(line.strip())
            if not m:
                continue
            ts, msg = float(m.group(1)), m.group(2)

            if t_start is None:
                t_start = ts
            if "Download Tasks Completed!" in msg:
                t_download_end = ts
            if re.search(r"\[Converter #\d+\] fasterq-dump done", msg):
                if t_fasterq_end is None or ts > t_fasterq_end:
                    t_fasterq_end = ts
            if re.search(r"\[Converter #\d+\] Completed ", msg):
                if t_pigz_end is None or ts > t_pigz_end:
                    t_pigz_end = ts

    if t_start is None:
        raise ValueError(f"Could not parse timestamps from {log_path}")

    t_dl = t_download_end or t_start
    t_fq = t_fasterq_end or t_dl
    t_pz = t_pigz_end or t_fq
    phases = {
        "download": {"start": t_start, "end": t_dl, "duration_s": round(max(0.0, t_dl - t_start), 2)},
        "conversion": {"start": t_dl, "end": t_fq, "duration_s": round(max(0.0, t_fq - t_dl), 2)},
        "compression": {"start": t_fq, "end": t_pz, "duration_s": round(max(0.0, t_pz - t_fq), 2)},
    }
    return {
        "tool": "fastbiodl",
        "phases": phases,
        "download_time_s": phases["download"]["duration_s"],
        "conversion_time_s": phases["conversion"]["duration_s"],
        "compression_time_s": phases["compression"]["duration_s"],
        "total_time_s": round(max(0.0, t_pz - t_start), 2),
        "source": "log-parsed",
    }


@dataclass
class RunRecord:
    method: str
    accession_set: str
    source_path: str
    run_stamp: str
    download_s: float
    conversion_s: float
    compression_s: float
    total_s: float
    post_download_s: float
    ov_dl_cv_s: float
    ov_dl_cp_s: float
    ov_cv_cp_s: float
    concurrency_ratio: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "accession_set": self.accession_set,
            "source_path": self.source_path,
            "run_stamp": self.run_stamp,
            "download_s": self.download_s,
            "conversion_s": self.conversion_s,
            "compression_s": self.compression_s,
            "total_s": self.total_s,
            "post_download_s": self.post_download_s,
            "ov_dl_cv_s": self.ov_dl_cv_s,
            "ov_dl_cp_s": self.ov_dl_cp_s,
            "ov_cv_cp_s": self.ov_cv_cp_s,
            "concurrency_ratio": self.concurrency_ratio,
        }


def _to_run_record(payload: dict[str, Any], source_path: str) -> RunRecord:
    method = _normalize_method(str(payload.get("tool", "unknown")))
    phases = _extract_phases(payload)
    dl = float(payload.get("download_time_s", phases["download"]["duration_s"]) or 0.0)
    cv = float(payload.get("conversion_time_s", phases["conversion"]["duration_s"]) or 0.0)
    cp = float(payload.get("compression_time_s", phases["compression"]["duration_s"]) or 0.0)
    total = float(payload.get("total_time_s", 0.0) or 0.0)
    if total <= 0.0:
        total = max(0.0, phases["compression"]["end"] - phases["download"]["start"])

    dl_s, dl_e = phases["download"]["start"], phases["download"]["end"]
    cv_s, cv_e = phases["conversion"]["start"], phases["conversion"]["end"]
    cp_s, cp_e = phases["compression"]["start"], phases["compression"]["end"]
    ov_dl_cv = _overlap(dl_s, dl_e, cv_s, cv_e)
    ov_dl_cp = _overlap(dl_s, dl_e, cp_s, cp_e)
    ov_cv_cp = _overlap(cv_s, cv_e, cp_s, cp_e)

    denom = dl + cv + cp
    concurrency_ratio = max(0.0, 1.0 - (total / denom)) if denom > 0 else 0.0
    m = _STAMP_RE.search(os.path.basename(source_path))
    run_stamp = m.group(1) if m else "unknown"

    return RunRecord(
        method=method,
        accession_set=_extract_accession_set(source_path, payload),
        source_path=source_path,
        run_stamp=run_stamp,
        download_s=round(max(0.0, dl), 4),
        conversion_s=round(max(0.0, cv), 4),
        compression_s=round(max(0.0, cp), 4),
        total_s=round(max(0.0, total), 4),
        post_download_s=round(max(0.0, total - dl), 4),
        ov_dl_cv_s=round(ov_dl_cv, 4),
        ov_dl_cp_s=round(ov_dl_cp, 4),
        ov_cv_cp_s=round(ov_cv_cp, 4),
        concurrency_ratio=round(concurrency_ratio, 6),
    )


def _discover_jsons(logs_root: str, include_globs: list[str], files: list[str]) -> list[str]:
    patterns = [
        os.path.join(logs_root, "**", "benchmark_*results_*.json"),
        os.path.join(logs_root, "**", "benchmark_*_results_*.json"),
        "benchmark_*results_*.json",
        "benchmark_*_results_*.json",
    ]
    patterns.extend(include_globs)

    found: set[str] = set()
    for pat in patterns:
        for p in glob.glob(pat, recursive=True):
            if os.path.isfile(p):
                found.add(os.path.normpath(p))
    for p in files:
        if os.path.isfile(p):
            found.add(os.path.normpath(p))

    return sorted(found)


def _aggregate(records: list[RunRecord]) -> dict[tuple[str, str], dict[str, Any]]:
    buckets: dict[tuple[str, str], list[RunRecord]] = {}
    for r in records:
        buckets.setdefault((r.accession_set, r.method), []).append(r)

    out: dict[tuple[str, str], dict[str, Any]] = {}
    for key, runs in sorted(buckets.items()):
        dl = [r.download_s for r in runs]
        cv = [r.conversion_s for r in runs]
        cp = [r.compression_s for r in runs]
        total = [r.total_s for r in runs]
        post = [r.post_download_s for r in runs]
        ov1 = [r.ov_dl_cv_s for r in runs]
        ov2 = [r.ov_dl_cp_s for r in runs]
        ov3 = [r.ov_cv_cp_s for r in runs]
        ccr = [r.concurrency_ratio for r in runs]

        out[key] = {
            "n": len(runs),
            "download": {"mean": _mean(dl), "std": _std(dl), "min": min(dl), "max": max(dl)},
            "conversion": {"mean": _mean(cv), "std": _std(cv), "min": min(cv), "max": max(cv)},
            "compression": {"mean": _mean(cp), "std": _std(cp), "min": min(cp), "max": max(cp)},
            "total": {"mean": _mean(total), "std": _std(total), "min": min(total), "max": max(total)},
            "post_download": {"mean": _mean(post), "std": _std(post), "min": min(post), "max": max(post)},
            "overlap": {
                "dl_x_conv": {"mean": _mean(ov1), "std": _std(ov1)},
                "dl_x_comp": {"mean": _mean(ov2), "std": _std(ov2)},
                "conv_x_comp": {"mean": _mean(ov3), "std": _std(ov3)},
            },
            "concurrency_ratio": {"mean": _mean(ccr), "std": _std(ccr)},
        }
    return out


def _safe_speedup(other: float, base: float) -> float:
    if base <= 0.0:
        return math.inf
    return other / base


def _compute_speedups(agg: dict[tuple[str, str], dict[str, Any]]) -> dict[str, Any]:
    accession_sets = sorted({k[0] for k in agg.keys()})
    methods = sorted({k[1] for k in agg.keys()})
    out: dict[str, Any] = {"per_accession_set": {}, "overall": {}}

    if "fastbiodl" not in methods:
        return out

    for acc_set in accession_sets:
        base = agg.get((acc_set, "fastbiodl"))
        if not base:
            continue
        out["per_accession_set"][acc_set] = {}
        for method in methods:
            if method == "fastbiodl":
                continue
            other = agg.get((acc_set, method))
            if not other:
                continue
            out["per_accession_set"][acc_set][method] = {
                "total_speedup_x": _safe_speedup(other["total"]["mean"], base["total"]["mean"]),
                "download_speedup_x": _safe_speedup(other["download"]["mean"], base["download"]["mean"]),
                "post_download_speedup_x": _safe_speedup(
                    other["post_download"]["mean"], base["post_download"]["mean"]
                ),
            }

    # Overall macro and pooled speedups across common accession sets.
    for method in methods:
        if method == "fastbiodl":
            continue
        common = [
            s for s in accession_sets
            if (s, "fastbiodl") in agg and (s, method) in agg
        ]
        if not common:
            continue

        macro_total = []
        macro_dl = []
        macro_post = []
        pooled_num_total = 0.0
        pooled_den_total = 0.0
        pooled_num_dl = 0.0
        pooled_den_dl = 0.0
        pooled_num_post = 0.0
        pooled_den_post = 0.0

        for s in common:
            b = agg[(s, "fastbiodl")]
            o = agg[(s, method)]
            macro_total.append(_safe_speedup(o["total"]["mean"], b["total"]["mean"]))
            macro_dl.append(_safe_speedup(o["download"]["mean"], b["download"]["mean"]))
            macro_post.append(_safe_speedup(o["post_download"]["mean"], b["post_download"]["mean"]))

            pooled_num_total += o["total"]["mean"]
            pooled_den_total += b["total"]["mean"]
            pooled_num_dl += o["download"]["mean"]
            pooled_den_dl += b["download"]["mean"]
            pooled_num_post += o["post_download"]["mean"]
            pooled_den_post += b["post_download"]["mean"]

        out["overall"][method] = {
            "common_accession_sets": common,
            "macro_avg": {
                "total_speedup_x": _mean(macro_total),
                "download_speedup_x": _mean(macro_dl),
                "post_download_speedup_x": _mean(macro_post),
            },
            "pooled": {
                "total_speedup_x": _safe_speedup(pooled_num_total, pooled_den_total),
                "download_speedup_x": _safe_speedup(pooled_num_dl, pooled_den_dl),
                "post_download_speedup_x": _safe_speedup(pooled_num_post, pooled_den_post),
            },
        }

    return out


def _print_by_accession_set(agg: dict[tuple[str, str], dict[str, Any]]) -> None:
    accession_sets = sorted({k[0] for k in agg.keys()})
    for acc_set in accession_sets:
        methods = sorted([k[1] for k in agg.keys() if k[0] == acc_set])
        if not methods:
            continue

        print()
        print("=" * 108)
        print(f"ACCESSION SET: {acc_set}")
        print("=" * 108)
        print(
            f"{'Method':<12} {'n':>3}  {'Download':>20}  {'Conversion':>20}  "
            f"{'Compression':>20}  {'Total':>20}"
        )
        print("-" * 108)

        for method in methods:
            a = agg[(acc_set, method)]
            print(
                f"{method:<12} {a['n']:>3}  "
                f"{_fmt_mu_sigma(a['download']['mean'], a['download']['std'], a['n']):>20}  "
                f"{_fmt_mu_sigma(a['conversion']['mean'], a['conversion']['std'], a['n']):>20}  "
                f"{_fmt_mu_sigma(a['compression']['mean'], a['compression']['std'], a['n']):>20}  "
                f"{_fmt_mu_sigma(a['total']['mean'], a['total']['std'], a['n']):>20}"
            )


def _print_metric_distributions(agg: dict[tuple[str, str], dict[str, Any]]) -> None:
    print()
    print("=" * 112)
    print("PER-METRIC DISTRIBUTIONS (MIN / MAX / MEAN / STD)")
    print("=" * 112)

    metric_order = ["download", "post_download", "conversion", "compression", "total"]
    metric_label = {
        "download": "download",
        "post_download": "post_download",
        "conversion": "conversion",
        "compression": "compression",
        "total": "total",
    }

    for acc_set in sorted({k[0] for k in agg.keys()}):
        print()
        print(f"[{acc_set}]")
        print(
            f"{'Method':<12} {'Metric':<14} {'n':>3}  {'Min(s)':>10}  {'Max(s)':>10}  "
            f"{'Mean(s)':>10}  {'Std(s)':>10}"
        )
        print("-" * 112)

        for method in sorted([k[1] for k in agg.keys() if k[0] == acc_set]):
            a = agg[(acc_set, method)]
            for m in metric_order:
                s = a[m]
                print(
                    f"{method:<12} {metric_label[m]:<14} {a['n']:>3}  "
                    f"{s['min']:>10.2f}  {s['max']:>10.2f}  {s['mean']:>10.2f}  {s['std']:>10.2f}"
                )


def _print_required_speedups(speedups: dict[str, Any]) -> None:
    print()
    print("=" * 94)
    print("REQUIRED RESULTS: FASTBIODL VS ALL OTHER METHODS")
    print("=" * 94)

    per_set = speedups.get("per_accession_set", {})
    if not per_set:
        print("No fastbiodl-vs-other overlap found, cannot compute speedups.")
        return

    for acc_set, methods in sorted(per_set.items()):
        print()
        print(f"[{acc_set}]")
        print(f"{'Method':<12} {'Total':>10}  {'Download':>10}  {'Total-Download':>16}")
        print(f"{'-' * 12} {'-' * 10}  {'-' * 10}  {'-' * 16}")
        for method, sp in sorted(methods.items()):
            print(
                f"{method:<12} "
                f"{sp['total_speedup_x']:>9.2f}x  "
                f"{sp['download_speedup_x']:>9.2f}x  "
                f"{sp['post_download_speedup_x']:>15.2f}x"
            )

    print()
    print("OVERALL (macro-average across accession sets and pooled-time ratio):")
    overall = speedups.get("overall", {})
    if not overall:
        print("No common accession sets across methods for overall speedups.")
        return

    print(
        f"{'Method':<12} {'Macro Total':>12}  {'Macro DL':>10}  {'Macro T-DL':>12}  "
        f"{'Pooled Total':>12}  {'Pooled DL':>10}  {'Pooled T-DL':>12}"
    )
    print("-" * 94)
    for method, row in sorted(overall.items()):
        ma = row["macro_avg"]
        po = row["pooled"]
        print(
            f"{method:<12} "
            f"{ma['total_speedup_x']:>11.2f}x  "
            f"{ma['download_speedup_x']:>9.2f}x  "
            f"{ma['post_download_speedup_x']:>11.2f}x  "
            f"{po['total_speedup_x']:>11.2f}x  "
            f"{po['download_speedup_x']:>9.2f}x  "
            f"{po['post_download_speedup_x']:>11.2f}x"
        )


def _print_extra_stats(agg: dict[tuple[str, str], dict[str, Any]]) -> None:
    print()
    print("=" * 94)
    print("PAPER-READY ADDITIONAL STATS")
    print("=" * 94)

    # 1) Concurrency/overlap evidence.
    print("Overlap and concurrency evidence (mean over runs):")
    print(
        f"{'Accession Set':<32} {'Method':<12} {'DLxConv':>10}  {'DLxComp':>10}  "
        f"{'ConvxComp':>10}  {'Concurrency':>12}"
    )
    print("-" * 94)
    for (acc_set, method), a in sorted(agg.items()):
        ov = a["overlap"]
        ccr = a["concurrency_ratio"]["mean"] * 100.0
        print(
            f"{acc_set:<32} {method:<12} "
            f"{_fmt_s(ov['dl_x_conv']['mean']):>10}  "
            f"{_fmt_s(ov['dl_x_comp']['mean']):>10}  "
            f"{_fmt_s(ov['conv_x_comp']['mean']):>10}  "
            f"{ccr:>11.1f}%"
        )

    # 2) Stability (coefficient of variation for total time).
    print()
    print("Run-to-run stability (Total time CV%):")
    print(f"{'Accession Set':<32} {'Method':<12} {'n':>3}  {'Mean':>10}  {'Std':>10}  {'CV%':>8}  {'Min..Max':>18}")
    print("-" * 94)
    for (acc_set, method), a in sorted(agg.items()):
        mu = a["total"]["mean"]
        sd = a["total"]["std"]
        cv = (sd / mu * 100.0) if mu > 0 else 0.0
        print(
            f"{acc_set:<32} {method:<12} {a['n']:>3}  "
            f"{_fmt_s(mu):>10}  {_fmt_s(sd):>10}  {cv:>7.2f}%  "
            f"{_fmt_s(a['total']['min'])}..{_fmt_s(a['total']['max'])}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare benchmark logs with FastBioDL-focused speedups",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--logs-root", default="logs", help="Root directory containing benchmark log folders")
    parser.add_argument("--include-glob", action="append", default=[], help="Extra glob(s) for JSON discovery")
    parser.add_argument("--files", nargs="*", default=[], help="Explicit benchmark result JSON files")
    parser.add_argument("--parse-log", nargs="*", default=[], help="Raw fastbiodl log(s) to parse")
    parser.add_argument("--methods", nargs="*", default=[], help="Keep only these normalized methods")
    parser.add_argument("--accession-sets", nargs="*", default=[], help="Keep only these accession set folder names")
    parser.add_argument("--output-json", help="Write full merged report to JSON")
    args = parser.parse_args()

    json_files = _discover_jsons(args.logs_root, args.include_glob, args.files)
    print(f"[i] Discovered {len(json_files)} benchmark JSON file(s)")

    payloads: list[tuple[dict[str, Any], str]] = []
    for p in json_files:
        try:
            with open(p, "r", encoding="utf-8") as f:
                payloads.append((json.load(f), p))
        except Exception as e:
            print(f"[warn] Skipping unreadable JSON: {p} ({e})")

    for lp in args.parse_log:
        try:
            payloads.append((_parse_fastbiodl_log(lp), lp))
            print(f"[i] Parsed fastbiodl log: {lp}")
        except Exception as e:
            print(f"[warn] Could not parse fastbiodl log: {lp} ({e})")

    records: list[RunRecord] = []
    for payload, src in payloads:
        try:
            records.append(_to_run_record(payload, src))
        except Exception as e:
            print(f"[warn] Skipping malformed payload from {src}: {e}")

    if args.methods:
        allowed_methods = set(m.strip().lower() for m in args.methods)
        records = [r for r in records if r.method in allowed_methods]
    if args.accession_sets:
        allowed_sets = set(args.accession_sets)
        records = [r for r in records if r.accession_set in allowed_sets]

    if not records:
        print("No benchmark records available after filtering.")
        sys.exit(1)

    print(f"[i] Loaded {len(records)} run record(s)")
    methods_seen = sorted({r.method for r in records})
    sets_seen = sorted({r.accession_set for r in records})
    print(f"[i] Methods: {', '.join(methods_seen)}")
    print(f"[i] Accession sets: {', '.join(sets_seen)}")

    agg = _aggregate(records)
    speedups = _compute_speedups(agg)

    _print_by_accession_set(agg)
    _print_metric_distributions(agg)
    _print_required_speedups(speedups)
    _print_extra_stats(agg)

    if args.output_json:
        output_path = args.output_json
        if not output_path.endswith(".json"):
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"{output_path}_{ts}.json"

        report = {
            "records": [r.as_dict() for r in records],
            "aggregate": {
                f"{k[0]}::{k[1]}": v for k, v in agg.items()
            },
            "speedups": speedups,
            "meta": {
                "logs_root": args.logs_root,
                "discovered_json_files": len(json_files),
                "loaded_records": len(records),
                "methods": methods_seen,
                "accession_sets": sets_seen,
            },
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"\n[i] Saved merged report: {output_path}")


if __name__ == "__main__":
    main()