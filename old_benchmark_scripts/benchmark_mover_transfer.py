#!/usr/bin/env python3
"""
benchmark_mover_transfer.py

Compares FastBioDL's FileMover system against naive file-transfer methods
for moving .fastq.gz files from NVMe local scratch to Lustre.

Methods
-------
  filemover       -- Full mover.py system (threaded I/O pool + optimizer)
  shutil_seq      -- Sequential shutil.copy2, one file at a time
  shutil_parallel -- concurrent.futures ThreadPoolExecutor + shutil.copy2
  cp              -- Single GNU ``cp`` subprocess

The input .fastq.gz files are prepared once on NVMe (download + fasterq-dump
+ pigz) and copied to a fresh staging directory before each rep so that the
FileMover's source-file cleanup does not affect subsequent reps.

Output
------
One JSON file per accession with per-rep timings, throughput (MB/s), summary
statistics, and pairwise speedup ratios.

Usage
-----
  python3 benchmark_mover_transfer.py SRR1234567 \\
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
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

_T_CRIT_95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
               6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228}


# ── helpers (same as benchmark_nvme_staging.py) ──────────────────────────

def run_cmd(cmd: List[str], timeout_s: int = 0) -> Tuple[int, float, str, str]:
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
    ratios = [b / t if t > 0 else float("inf")
              for b, t in zip(baseline_times, treatment_times)]
    return summary_stats(ratios)


def _human(nbytes: int) -> str:
    v = float(max(0, nbytes))
    for u in ("B", "KB", "MB", "GB", "TB"):
        if v < 1024.0 or u == "TB":
            return f"{v:.2f} {u}"
        v /= 1024.0


# ── download + conversion (one-time preparation) ─────────────────────────

def download_sra(accession: str, sra_dir: Path, script_dir: Path,
                 segment_size_mb: int = 512, max_segments: int = 8,
                 max_retries: int = 3) -> Tuple[Path, float, str]:
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

    fb.download_dir = str(sra_dir)
    fb.transfer_done = mp.Value("i", 0)
    fb.download_process_status = mp.Array("i", [1])

    counter  = mp.Value("Q", 0)
    active   = mp.Value("i", 0)
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
    for suffix in ["", ".sra", ".sralite.1", ".sralite.2", ".1", ".2"]:
        for base in [sra_dir, sra_dir / accession]:
            c = base / f"{accession}{suffix}"
            if c.exists() and c.is_file():
                return c
    for p in sorted(sra_dir.rglob(f"{accession}*")):
        if p.is_file() and p.stat().st_size > 0:
            return p
    return None


def prepare_fastq_gz(sra_path: Path, golden_dir: Path,
                     fasterq_cmd: str, pigz_cmd: str,
                     threads: int) -> Tuple[float, float, int]:
    """Convert SRA to .fastq.gz on NVMe.  Returns (fasterq_s, pigz_s, total_bytes)."""
    tmp_dir = golden_dir.parent / "prep_tmp"
    clean_dir(golden_dir)
    clean_dir(tmp_dir)

    rc, fasterq_s, _, err = run_cmd([
        fasterq_cmd, "--threads", str(threads),
        "--temp", str(tmp_dir), "--outdir", str(golden_dir),
        "--split-3", "--skip-technical", str(sra_path),
    ], timeout_s=7200)
    if rc != 0:
        raise RuntimeError(f"fasterq-dump failed: {err.strip()[-500:]}")
    shutil.rmtree(tmp_dir, ignore_errors=True)

    fastq_files = sorted(golden_dir.glob("*.fastq"))
    if not fastq_files:
        raise RuntimeError("No .fastq produced during preparation")
    t0 = time.perf_counter()
    for fq in fastq_files:
        rc, _, _, err = run_cmd([pigz_cmd, "-1", "-p", str(threads), str(fq)])
        if rc != 0:
            raise RuntimeError(f"pigz failed: {err.strip()[-500:]}")
    pigz_s = time.perf_counter() - t0

    total_bytes = sum(f.stat().st_size for f in golden_dir.glob("*.fastq.gz"))
    return fasterq_s, pigz_s, total_bytes


def stage_golden_to(golden_dir: Path, staging_dir: Path) -> int:
    """Copy golden .fastq.gz to a fresh staging dir (untimed setup)."""
    clean_dir(staging_dir)
    total = 0
    for gz in sorted(golden_dir.glob("*.fastq.gz")):
        shutil.copy2(gz, staging_dir / gz.name)
        total += gz.stat().st_size
    return total


# ── transfer methods ─────────────────────────────────────────────────────

def xfer_shutil_seq(src: Path, dst: Path, _threads: int) -> float:
    """Sequential shutil.copy2."""
    clean_dir(dst)
    t0 = time.perf_counter()
    for f in sorted(src.rglob("*.fastq.gz")):
        rel = f.relative_to(src)
        dest = dst / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, dest)
    return time.perf_counter() - t0


def xfer_shutil_parallel(src: Path, dst: Path, threads: int) -> float:
    """Threaded shutil.copy2 using ThreadPoolExecutor."""
    clean_dir(dst)
    files = sorted(src.rglob("*.fastq.gz"))

    def _copy_one(f: Path) -> None:
        rel = f.relative_to(src)
        dest = dst / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, dest)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        futs = [pool.submit(_copy_one, f) for f in files]
        for fut in as_completed(futs):
            fut.result()
    return time.perf_counter() - t0


def xfer_cp(src: Path, dst: Path, _threads: int) -> float:
    """GNU cp subprocess."""
    clean_dir(dst)
    files = sorted(str(f) for f in src.rglob("*.fastq.gz"))
    if not files:
        return 0.0
    t0 = time.perf_counter()
    proc = subprocess.run(
        ["cp", "--"] + files + [str(dst) + "/"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=7200,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"cp failed: {proc.stderr.decode(errors='replace')[-500:]}")
    return time.perf_counter() - t0


def xfer_filemover(src: Path, dst: Path, threads: int) -> float:
    """FastBioDL FileMover with fixed-probe I/O optimization."""
    # Configure the module-level dict that mover.py workers read at runtime.
    from config_fastbiodl import configurations
    configurations["method"] = "probe"
    configurations["fixed_probing"] = {"thread": threads}
    configurations.setdefault("K", 1.01)

    from mover import FileMover

    clean_dir(dst)
    move_queue = mp.Queue()
    config = {
        "thread_limit": threads,
        "probing_sec": 5,
        "io_limit": -1,
    }

    mover = FileMover(
        move_queue=move_queue,
        tmpfs_dir=str(src),
        root_dir=str(dst),
        config=config,
    )

    t0 = time.perf_counter()
    mover.begin()
    for gz in sorted(src.rglob("*.fastq.gz")):
        move_queue.put(str(gz))
    move_queue.put(None)
    mover.stop(timeout=3600)
    return time.perf_counter() - t0


# ── main ──────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Benchmark FileMover vs. naive NVMe-to-Lustre transfers")
    ap.add_argument("accession", help="Single SRA accession (e.g. SRR390728)")
    ap.add_argument("--nvme-dir", required=True,
                    help="NVMe local scratch root")
    ap.add_argument("--lustre-dir", required=True,
                    help="Lustre output root")
    ap.add_argument("--threads", type=int, default=8,
                    help="Thread count for parallel methods (default: 8)")
    ap.add_argument("--reps", type=int, default=3,
                    help="Repetitions per method (default: 3)")
    ap.add_argument("--json-out", default="",
                    help="Explicit JSON output path (default: auto)")
    ap.add_argument("--download-segment-size-mb", type=int, default=512)
    ap.add_argument("--download-max-segments", type=int, default=8)
    ap.add_argument("--cleanup", action="store_true",
                    help="Remove work directories on completion")
    args = ap.parse_args()

    accession = args.accession.strip()
    script_dir = Path(__file__).resolve().parent
    nvme_root  = Path(args.nvme_dir).resolve()
    lustre_root = Path(args.lustre_dir).resolve()

    # Resolve tools
    cmds = {}
    for tool in ("fasterq-dump", "pigz"):
        try:
            cmds[tool] = resolve_tool(tool, script_dir)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2

    # Directories
    work_nvme   = nvme_root / f"bench_mover_{accession}"
    work_lustre = lustre_root / f"bench_mover_{accession}"
    sra_dir     = work_nvme / "sra"
    golden_dir  = work_nvme / "golden"
    sra_dir.mkdir(parents=True, exist_ok=True)
    work_lustre.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Download ──────────────────────────────────────────────────
    print(f"\n[1] Downloading {accession} to NVMe ...")
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

    # ── Step 2: Prepare golden .fastq.gz on NVMe ─────────────────────────
    print(f"\n[2] Converting {accession} to .fastq.gz (golden copy on NVMe) ...")
    fq_s, pz_s, golden_bytes = prepare_fastq_gz(
        sra_path, golden_dir,
        cmds["fasterq-dump"], cmds["pigz"], args.threads,
    )
    golden_files = sorted(golden_dir.glob("*.fastq.gz"))
    print(f"  {len(golden_files)} file(s), {_human(golden_bytes)}  "
          f"(fasterq={fq_s:.1f}s, pigz={pz_s:.1f}s)")

    # ── Step 3: Benchmark transfers ──────────────────────────────────────
    methods = {
        "shutil_seq":      xfer_shutil_seq,
        "shutil_parallel": xfer_shutil_parallel,
        "cp":              xfer_cp,
        "filemover":       xfer_filemover,
    }

    results: Dict[str, List[Dict]] = {m: [] for m in methods}
    ever_dropped = True

    print(f"\n[3] Running {args.reps} reps x {len(methods)} methods")
    for rep in range(1, args.reps + 1):
        for method_name, fn in methods.items():
            # FileMover deletes source files, so stage a fresh copy each time.
            # For the other methods, stage as well for parity (same copy overhead).
            staging = work_nvme / f"staging_{method_name}_r{rep}"
            staged_bytes = stage_golden_to(golden_dir, staging)

            if not drop_caches():
                ever_dropped = False

            dest = work_lustre / f"{method_name}_r{rep}"
            print(f"  rep={rep}/{args.reps}  method={method_name:16s} ... ",
                  end="", flush=True)
            try:
                elapsed = fn(staging, dest, args.threads)

                # Verify output matches golden
                dest_bytes = sum(
                    f.stat().st_size for f in dest.rglob("*.fastq.gz"))
                throughput_mbs = (staged_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0

                rec = {
                    "method": method_name,
                    "rep": rep,
                    "elapsed_s": round(elapsed, 3),
                    "bytes": staged_bytes,
                    "throughput_mbs": round(throughput_mbs, 2),
                    "output_bytes": dest_bytes,
                    "byte_match": dest_bytes == staged_bytes,
                }
                results[method_name].append(rec)
                print(f"{elapsed:.1f}s  {throughput_mbs:.1f} MB/s  "
                      f"match={dest_bytes == staged_bytes}")

            except Exception as exc:
                print(f"FAILED: {exc}")
                results[method_name].append(
                    {"method": method_name, "rep": rep, "error": str(exc)})

            # Cleanup to save NVMe/Lustre space between reps
            shutil.rmtree(staging, ignore_errors=True)
            shutil.rmtree(dest, ignore_errors=True)

    # ── Summary ──────────────────────────────────────────────────────────
    summaries = {}
    for method_name, reps_list in results.items():
        ok = [r for r in reps_list if "error" not in r]
        if ok:
            summaries[method_name] = {
                "elapsed_s":      summary_stats([r["elapsed_s"] for r in ok]),
                "throughput_mbs": summary_stats([r["throughput_mbs"] for r in ok]),
            }

    # Pairwise speedups: method / shutil_seq  (>1 means method is faster)
    speedups = {}
    baseline_ok = [r for r in results["shutil_seq"] if "error" not in r]
    for alt in ("shutil_parallel", "cp", "filemover"):
        alt_ok = [r for r in results[alt] if "error" not in r]
        n = min(len(baseline_ok), len(alt_ok))
        if n > 0:
            speedups[f"{alt}_vs_shutil_seq"] = speedup_stats(
                [baseline_ok[i]["elapsed_s"] for i in range(n)],
                [alt_ok[i]["elapsed_s"] for i in range(n)],
            )

    report = {
        "benchmark": "mover_transfer",
        "accession": accession,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "system": {
            "hostname": platform.node(),
            "cpu_count": mp.cpu_count(),
            "threads_used": args.threads,
            "reps": args.reps,
            "caches_dropped": ever_dropped,
        },
        "download": {
            "elapsed_s": round(dl_s, 3),
            "source_url": source_url,
            "sra_size_bytes": sra_size,
        },
        "golden": {
            "files": len(golden_files),
            "total_bytes": golden_bytes,
            "fasterq_s": round(fq_s, 3),
            "pigz_s": round(pz_s, 3),
        },
        "per_rep": results,
        "summary": summaries,
        "speedups": speedups,
    }

    out_json = (Path(args.json_out) if args.json_out
                else lustre_root / f"benchmark_mover_transfer_{accession}.json")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    with out_json.open("w") as f:
        json.dump(report, f, indent=2)

    # Console summary
    print(f"\n{'=' * 65}")
    print(f"  MOVER TRANSFER BENCHMARK  --  {accession}")
    print(f"  Payload: {len(golden_files)} file(s), {_human(golden_bytes)}")
    print(f"{'=' * 65}")
    for m in ("shutil_seq", "shutil_parallel", "cp", "filemover"):
        if m in summaries:
            s = summaries[m]["throughput_mbs"]
            t = summaries[m]["elapsed_s"]
            print(f"  {m:16s}  "
                  f"time={t['mean']:7.1f}s  "
                  f"throughput={s['mean']:7.1f} MB/s  "
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
