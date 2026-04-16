#!/usr/bin/env python3
"""
benchmark_nvme_staging.py

Measures the performance benefit of NVMe-staged SRA-to-FASTQ conversion
versus converting directly on a parallel filesystem (Lustre).

Three conversion modes are compared for each accession:

  staged  -- fasterq-dump + pigz on NVMe, then copy .fastq.gz to Lustre
  direct  -- fasterq-dump + pigz with --outdir and --temp both on Lustre
  hybrid  -- fasterq-dump on NVMe, pigz compresses to Lustre via stdout

The SRA download is performed once per accession on NVMe and reused across
all modes and repetitions.  Page-cache flush is attempted between reps.

Output
------
One JSON file per accession containing per-rep timings, summary statistics
(mean, std, median, min, max, 95% CI), and pairwise speedup ratios suitable
for inclusion in a research paper.

Usage
-----
  python3 benchmark_nvme_staging.py SRR1234567 \\
      --nvme-dir /scratch/$USER/job_$SLURM_JOB_ID \\
      --lustre-dir /expanse/lustre/scratch/$USER/temp_project/results \\
      --threads 8 --reps 3
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np

# Two-tailed t critical values for 95% CI (df = n-1).
_T_CRIT_95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
               6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228}


# ── helpers ───────────────────────────────────────────────────────────────

def run_cmd(cmd: List[str], timeout_s: int = 0) -> Tuple[int, float, str, str]:
    """Run *cmd* and return (returncode, elapsed_s, stdout, stderr)."""
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, timeout=(timeout_s if timeout_s > 0 else None),
        )
        return proc.returncode, time.perf_counter() - t0, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        return 124, time.perf_counter() - t0, exc.stdout or "", (exc.stderr or "") + "\nTIMEOUT"


def resolve_tool(binary: str, script_dir: Path) -> str:
    found = shutil.which(binary)
    if found:
        return found
    bundled = script_dir / "sratoolkit.3.1.0-ubuntu64" / "bin" / binary
    if bundled.exists() and os.access(bundled, os.X_OK):
        return str(bundled)
    raise RuntimeError(f"Required tool not found: {binary}")


def drop_caches() -> bool:
    """Best-effort page-cache flush.  Returns True on success."""
    try:
        subprocess.run(["sync"], check=True, timeout=60)
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
        return True
    except (PermissionError, OSError, subprocess.SubprocessError):
        return False


def clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def summary_stats(values: List[float]) -> Dict:
    a = np.array(values, dtype=float)
    n = len(a)
    mean = float(np.mean(a))
    std = float(np.std(a, ddof=1)) if n > 1 else 0.0
    se = std / (n ** 0.5) if n > 1 else 0.0
    t_val = _T_CRIT_95.get(n - 1, 2.0)
    return {
        "n": n,
        "mean": round(mean, 4),
        "std": round(std, 4),
        "median": round(float(np.median(a)), 4),
        "min": round(float(np.min(a)), 4),
        "max": round(float(np.max(a)), 4),
        "ci_95_lo": round(mean - t_val * se, 4),
        "ci_95_hi": round(mean + t_val * se, 4),
    }


def speedup_stats(baseline_times: List[float],
                  treatment_times: List[float]) -> Dict:
    """Per-rep speedup: baseline / treatment (>1 means treatment is faster)."""
    ratios = [b / t if t > 0 else float("inf")
              for b, t in zip(baseline_times, treatment_times)]
    return summary_stats(ratios)


def _human(nbytes: int) -> str:
    v = float(max(0, nbytes))
    for u in ("B", "KB", "MB", "GB", "TB"):
        if v < 1024.0 or u == "TB":
            return f"{v:.2f} {u}"
        v /= 1024.0


# ── download ──────────────────────────────────────────────────────────────

def download_sra(accession: str, sra_dir: Path, script_dir: Path,
                 segment_size_mb: int = 512, max_segments: int = 8,
                 max_retries: int = 3) -> Tuple[Path, float, str]:
    """Download one accession with FastBioDL's segmented downloader."""
    try:
        import aiohttp
        import fastbiodl_upgrade as fb
    except ImportError as exc:
        raise RuntimeError(f"FastBioDL import failed: {exc}") from exc

    url_acc_pairs = fb.get_ncbi_urls(accession, field="sra_ftp")
    if not url_acc_pairs:
        raise RuntimeError(f"No SRA URL found for {accession}")

    url, _ = url_acc_pairs[0]
    filename = os.path.basename(url)
    local_path = sra_dir / accession / filename
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # Module-level globals the downloader reads
    fb.download_dir = str(sra_dir)
    fb.transfer_done = mp.Value("i", 0)
    fb.download_process_status = mp.Array("i", [1])

    counter = mp.Value("Q", 0)
    active  = mp.Value("i", 0)
    reserved = mp.Value("Q", 0)
    pending  = mp.Value("Q", 0)

    async def _dl():
        import aiohttp as _aio
        timeout = _aio.ClientTimeout(total=3600, connect=60, sock_read=300)
        connector = _aio.TCPConnector(
            limit=max_segments, limit_per_host=max_segments,
            ttl_dns_cache=300, enable_cleanup_closed=True,
        )
        async with _aio.ClientSession(
            connector=connector, timeout=timeout,
            headers={"User-Agent": "fastbiodl/3.0"},
        ) as session:
            dl = fb.SegmentedDownloader(
                session=session, url=url, local_path=str(local_path),
                segment_size=segment_size_mb * 1024 * 1024,
                max_segments=max_segments, process_id=0,
                process_counter=counter,
                active_connections=active,
                disk_reserved_bytes=reserved,
                min_pending_conversion_bytes=pending,
                disk_safety_margin_bytes=0,
                max_retries=max_retries,
            )
            return await dl.download_with_resume()

    t0 = time.perf_counter()
    success, paused, _ = asyncio.run(_dl())
    elapsed = time.perf_counter() - t0

    if not success:
        raise RuntimeError(f"Download {'paused' if paused else 'failed'} for {accession}")
    if not local_path.exists():
        raise RuntimeError(f"Success reported but file missing: {local_path}")
    return local_path, elapsed, url


def find_sra_file(sra_dir: Path, accession: str) -> Optional[Path]:
    """Locate the downloaded SRA file under various naming conventions."""
    for suffix in ["", ".sra", ".sralite.1", ".sralite.2", ".1", ".2"]:
        for base in [sra_dir, sra_dir / accession]:
            c = base / f"{accession}{suffix}"
            if c.exists() and c.is_file():
                return c
    for p in sorted(sra_dir.rglob(f"{accession}*")):
        if p.is_file() and p.stat().st_size > 0:
            return p
    return None


# ── conversion modes ─────────────────────────────────────────────────────

def _run_fasterq(sra_path: Path, out_dir: Path, tmp_dir: Path,
                 fasterq_cmd: str, threads: int) -> float:
    """Run fasterq-dump and return wall-clock seconds."""
    rc, elapsed, _, err = run_cmd([
        fasterq_cmd, "--threads", str(threads),
        "--temp", str(tmp_dir), "--outdir", str(out_dir),
        "--split-3", "--skip-technical", str(sra_path),
    ], timeout_s=7200)
    if rc != 0:
        raise RuntimeError(f"fasterq-dump rc={rc}: {err.strip()[-500:]}")
    return elapsed


def _run_pigz_inplace(out_dir: Path, pigz_cmd: str, threads: int) -> float:
    """Compress all .fastq in *out_dir* in-place; return wall-clock seconds."""
    fastq_files = sorted(out_dir.glob("*.fastq"))
    if not fastq_files:
        raise RuntimeError(f"No .fastq produced in {out_dir}")
    t0 = time.perf_counter()
    for fq in fastq_files:
        rc, _, _, err = run_cmd([pigz_cmd, "-1", "-p", str(threads), str(fq)])
        if rc != 0:
            raise RuntimeError(f"pigz rc={rc}: {err.strip()[-500:]}")
    return time.perf_counter() - t0


def _output_summary(gz_dir: Path) -> Tuple[int, int]:
    """Return (file_count, total_bytes) for .fastq.gz in gz_dir."""
    gz = list(gz_dir.glob("*.fastq.gz"))
    return len(gz), sum(f.stat().st_size for f in gz)


def mode_staged(sra_path: Path, nvme_work: Path, lustre_dest: Path,
                fasterq_cmd: str, pigz_cmd: str, threads: int) -> Dict:
    """fasterq + pigz on NVMe, then copy .fastq.gz to Lustre."""
    out_dir = nvme_work / "fastq"
    tmp_dir = nvme_work / "tmp"
    clean_dir(out_dir); clean_dir(tmp_dir); clean_dir(lustre_dest)

    t_wall = time.perf_counter()
    fasterq_s = _run_fasterq(sra_path, out_dir, tmp_dir, fasterq_cmd, threads)
    pigz_s = _run_pigz_inplace(out_dir, pigz_cmd, threads)

    t_copy = time.perf_counter()
    for gz in sorted(out_dir.glob("*.fastq.gz")):
        shutil.copy2(gz, lustre_dest / gz.name)
    copy_s = time.perf_counter() - t_copy
    total_s = time.perf_counter() - t_wall

    n_files, n_bytes = _output_summary(lustre_dest)
    return {"mode": "staged", "fasterq_s": round(fasterq_s, 3),
            "pigz_s": round(pigz_s, 3), "copy_s": round(copy_s, 3),
            "total_s": round(total_s, 3),
            "output_files": n_files, "output_bytes": n_bytes}


def mode_direct(sra_path: Path, lustre_work: Path,
                fasterq_cmd: str, pigz_cmd: str, threads: int) -> Dict:
    """fasterq + pigz with output and temp both on Lustre."""
    out_dir = lustre_work / "fastq"
    tmp_dir = lustre_work / "tmp"
    clean_dir(out_dir); clean_dir(tmp_dir)

    t_wall = time.perf_counter()
    fasterq_s = _run_fasterq(sra_path, out_dir, tmp_dir, fasterq_cmd, threads)
    pigz_s = _run_pigz_inplace(out_dir, pigz_cmd, threads)
    total_s = time.perf_counter() - t_wall

    n_files, n_bytes = _output_summary(out_dir)
    return {"mode": "direct", "fasterq_s": round(fasterq_s, 3),
            "pigz_s": round(pigz_s, 3), "copy_s": 0.0,
            "total_s": round(total_s, 3),
            "output_files": n_files, "output_bytes": n_bytes}


def mode_hybrid(sra_path: Path, nvme_work: Path, lustre_dest: Path,
                fasterq_cmd: str, pigz_cmd: str, threads: int) -> Dict:
    """fasterq on NVMe; pigz reads NVMe, streams compressed output to Lustre."""
    out_dir = nvme_work / "fastq"
    tmp_dir = nvme_work / "tmp"
    clean_dir(out_dir); clean_dir(tmp_dir); clean_dir(lustre_dest)

    t_wall = time.perf_counter()
    fasterq_s = _run_fasterq(sra_path, out_dir, tmp_dir, fasterq_cmd, threads)

    # pigz -c writes to stdout; we redirect each stream to a file on Lustre
    fastq_files = sorted(out_dir.glob("*.fastq"))
    if not fastq_files:
        raise RuntimeError(f"No .fastq produced in {out_dir}")
    t_pigz = time.perf_counter()
    for fq in fastq_files:
        gz_dest = lustre_dest / (fq.name + ".gz")
        with open(gz_dest, "wb") as out_f:
            proc = subprocess.run(
                [pigz_cmd, "-1", "-c", "-p", str(threads), str(fq)],
                stdout=out_f, stderr=subprocess.PIPE, timeout=7200,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"pigz failed (hybrid): {proc.stderr.decode(errors='replace')[-500:]}")
    pigz_s = time.perf_counter() - t_pigz
    total_s = time.perf_counter() - t_wall

    n_files, n_bytes = _output_summary(lustre_dest)
    return {"mode": "hybrid", "fasterq_s": round(fasterq_s, 3),
            "pigz_s": round(pigz_s, 3), "copy_s": 0.0,
            "total_s": round(total_s, 3),
            "output_files": n_files, "output_bytes": n_bytes}


# ── main ──────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Benchmark NVMe staging vs. direct-on-Lustre SRA conversion")
    ap.add_argument("accession", help="Single SRA accession (e.g. SRR390728)")
    ap.add_argument("--nvme-dir", required=True,
                    help="NVMe local scratch root")
    ap.add_argument("--lustre-dir", required=True,
                    help="Lustre output root")
    ap.add_argument("--threads", type=int, default=8,
                    help="Threads for fasterq-dump and pigz (default: 8)")
    ap.add_argument("--reps", type=int, default=3,
                    help="Repetitions per mode (default: 3)")
    ap.add_argument("--json-out", default="",
                    help="Explicit JSON output path (default: auto)")
    ap.add_argument("--download-segment-size-mb", type=int, default=512)
    ap.add_argument("--download-max-segments", type=int, default=8)
    ap.add_argument("--cleanup", action="store_true",
                    help="Remove work directories on completion")
    args = ap.parse_args()

    accession = args.accession.strip()
    script_dir = Path(__file__).resolve().parent
    nvme_root = Path(args.nvme_dir).resolve()
    lustre_root = Path(args.lustre_dir).resolve()

    # Resolve external tools
    cmds = {}
    for tool in ("fasterq-dump", "pigz"):
        try:
            cmds[tool] = resolve_tool(tool, script_dir)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    print(f"Tools: {cmds}")

    # Directories
    work_nvme   = nvme_root / f"bench_staging_{accession}"
    work_lustre = lustre_root / f"bench_staging_{accession}"
    sra_dir     = work_nvme / "sra"
    sra_dir.mkdir(parents=True, exist_ok=True)
    work_lustre.mkdir(parents=True, exist_ok=True)

    # ── Download ──────────────────────────────────────────────────────────
    print(f"\n[1/{args.reps * 3 + 1}] Downloading {accession} to NVMe ...")
    try:
        sra_path, dl_s, source_url = download_sra(
            accession, sra_dir, script_dir,
            segment_size_mb=args.download_segment_size_mb,
            max_segments=args.download_max_segments,
        )
    except Exception as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        return 1

    if sra_path is None or not sra_path.exists():
        sra_path = find_sra_file(sra_dir, accession)
    if sra_path is None:
        print(f"SRA file not found under {sra_dir}", file=sys.stderr)
        return 1

    sra_size = sra_path.stat().st_size
    print(f"  {sra_path}  ({_human(sra_size)})  in {dl_s:.1f}s")

    # ── Benchmark loop ────────────────────────────────────────────────────
    mode_fns = {
        "staged": lambda rep: mode_staged(
            sra_path,
            work_nvme / f"staged_r{rep}",
            work_lustre / f"staged_r{rep}",
            cmds["fasterq-dump"], cmds["pigz"], args.threads,
        ),
        "direct": lambda rep: mode_direct(
            sra_path,
            work_lustre / f"direct_r{rep}",
            cmds["fasterq-dump"], cmds["pigz"], args.threads,
        ),
        "hybrid": lambda rep: mode_hybrid(
            sra_path,
            work_nvme / f"hybrid_r{rep}",
            work_lustre / f"hybrid_r{rep}",
            cmds["fasterq-dump"], cmds["pigz"], args.threads,
        ),
    }

    results: Dict[str, List[Dict]] = {m: [] for m in mode_fns}
    ever_dropped = True
    step = 1

    print(f"\n[2] Running {args.reps} reps x {len(mode_fns)} modes")
    for rep in range(1, args.reps + 1):
        for mode_name, fn in mode_fns.items():
            step += 1
            if not drop_caches():
                ever_dropped = False
            print(f"  [{step}] rep={rep}/{args.reps}  mode={mode_name} ... ",
                  end="", flush=True)
            try:
                r = fn(rep)
                r["rep"] = rep
                results[mode_name].append(r)
                print(f"total={r['total_s']:.1f}s  "
                      f"(fasterq={r['fasterq_s']:.1f}  pigz={r['pigz_s']:.1f}  "
                      f"copy={r['copy_s']:.1f})")
            except RuntimeError as exc:
                print(f"FAILED: {exc}")
                results[mode_name].append(
                    {"mode": mode_name, "rep": rep, "error": str(exc)})

            # Remove work dirs for this rep to reclaim NVMe space
            for p in (work_nvme / f"{mode_name}_r{rep}",
                      work_lustre / f"{mode_name}_r{rep}"):
                shutil.rmtree(p, ignore_errors=True)

    # ── Summary statistics ────────────────────────────────────────────────
    summaries = {}
    for mode_name, reps_list in results.items():
        ok = [r for r in reps_list if "error" not in r]
        if not ok:
            continue
        summaries[mode_name] = {}
        for field in ("total_s", "fasterq_s", "pigz_s", "copy_s"):
            vals = [r[field] for r in ok if field in r]
            if vals:
                summaries[mode_name][field] = summary_stats(vals)

    # Pairwise speedup: direct / alt  (>1 means alt is faster)
    speedups = {}
    direct_ok = [r for r in results["direct"] if "error" not in r]
    for alt in ("staged", "hybrid"):
        alt_ok = [r for r in results[alt] if "error" not in r]
        n = min(len(direct_ok), len(alt_ok))
        if n > 0:
            speedups[f"direct_vs_{alt}"] = speedup_stats(
                [direct_ok[i]["total_s"] for i in range(n)],
                [alt_ok[i]["total_s"] for i in range(n)],
            )

    # ── Report ────────────────────────────────────────────────────────────
    report = {
        "benchmark": "nvme_staging",
        "accession": accession,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "system": {
            "hostname": platform.node(),
            "cpu_count": mp.cpu_count(),
            "threads_used": args.threads,
            "reps": args.reps,
            "caches_dropped": ever_dropped,
            "fasterq_dump": cmds["fasterq-dump"],
            "pigz": cmds["pigz"],
        },
        "download": {
            "elapsed_s": round(dl_s, 3),
            "source_url": source_url,
            "sra_size_bytes": sra_size,
        },
        "per_rep": results,
        "summary": summaries,
        "speedups": speedups,
    }

    out_json = (Path(args.json_out) if args.json_out
                else lustre_root / f"benchmark_nvme_staging_{accession}.json")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    with out_json.open("w") as f:
        json.dump(report, f, indent=2)

    # Console summary
    print(f"\n{'=' * 65}")
    print(f"  NVMe STAGING BENCHMARK  --  {accession}")
    print(f"{'=' * 65}")
    for m in ("staged", "direct", "hybrid"):
        if m in summaries and "total_s" in summaries[m]:
            s = summaries[m]["total_s"]
            print(f"  {m:8s}  mean={s['mean']:8.1f}s   "
                  f"std={s['std']:6.1f}   "
                  f"95%CI=[{s['ci_95_lo']:.1f}, {s['ci_95_hi']:.1f}]")
    for name, sp in speedups.items():
        print(f"  speedup {name}: {sp['mean']:.2f}x  "
              f"95%CI=[{sp['ci_95_lo']:.2f}, {sp['ci_95_hi']:.2f}]")
    print(f"  JSON -> {out_json}")
    print(f"{'=' * 65}")

    if args.cleanup:
        shutil.rmtree(work_nvme, ignore_errors=True)
        shutil.rmtree(work_lustre, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
