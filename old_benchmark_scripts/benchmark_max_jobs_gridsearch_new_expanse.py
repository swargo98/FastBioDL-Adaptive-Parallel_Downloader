#!/usr/bin/env python3
"""
benchmark_max_jobs_gridsearch.py

Multi-accession, multi-repeat benchmark for the FastBioDL conversion pipeline.

For each accession:

  1. Download the SRA file once with the FastBioDL segmented downloader and
     keep it for the entire benchmark of that accession (no re-download per run).
  2. Run a strict sequential baseline (no fasterq/pigz overlap) ``--repeats``
     times over a 3-copy dataset.
  3. Run the overlapped pipeline for every (max_conversion_jobs, max_pigz_jobs)
     pair in the 3x3 grid {1,2,3} x {1,2,3}, ``--repeats`` times each, again
     over a 3-copy dataset.
  4. After each individual repeat is recorded, remove that repeat's staged SRA
     copies, intermediate FASTQ files, and final .fastq.gz files so disk usage
     stays bounded.

Storage tiers are independent inputs:

  --sra-out-dir    storage tier for the master SRA download and the per-run
                   input copies fed to fasterq-dump.
  --fastq-out-dir  storage tier for intermediate FASTQ output written by
                   fasterq-dump.
  --pigz-out-dir   storage tier for final .fastq.gz output. pigz writes
                   directly here via ``pigz -c`` redirection; no post-run move.

The overlapped pipeline is implemented locally with two thread pools so that
``max_conversion_jobs`` bounds concurrent fasterq-dump processes,
``max_pigz_jobs`` bounds concurrent pigz processes, and the two stages overlap
naturally as fasterq-dump completions feed pigz tasks.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import shutil
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from storage_config import nvme_path


GRID_PAIRS: List[Tuple[int, int]] = [(c, p) for c in (4, 5, 6) for p in (4, 5, 6)]


# ---------------------------------------------------------------------------
# Tool discovery and small filesystem helpers
# ---------------------------------------------------------------------------

def resolve_tool(binary: str, script_dir: Path) -> str:
    """Resolve tool from PATH, then fall back to the bundled sratoolkit in the repo."""
    in_path = shutil.which(binary)
    if in_path:
        return in_path

    bundled = script_dir / "sratoolkit.3.1.0-ubuntu64" / "bin" / binary
    if bundled.exists() and os.access(bundled, os.X_OK):
        return str(bundled)

    raise RuntimeError(
        f"Required command not found: {binary}. Tried PATH and {bundled}"
    )


def prepare_tool_path(script_dir: Path) -> None:
    """Ensure the bundled fasterq-dump is reachable by child worker processes."""
    bundled_bin = script_dir / "sratoolkit.3.1.0-ubuntu64" / "bin"
    if bundled_bin.exists() and shutil.which("fasterq-dump") is None:
        os.environ["PATH"] = f"{bundled_bin}:{os.environ.get('PATH', '')}"


def clean_dir(path: Path) -> None:
    """Remove ``path`` if it exists, then recreate it as an empty directory."""
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def remove_dirs(*dirs: Path) -> None:
    """Best-effort recursive removal; suppress errors so cleanup never aborts a run."""
    for d in dirs:
        if d is None:
            continue
        try:
            shutil.rmtree(d, ignore_errors=True)
        except Exception:
            pass


def default_json_out(script_dir: Path) -> Path:
    """Choose a combined JSON report location for both local and Slurm runs."""
    for env_name in ("GRIDSEARCH_RESULTS_DIR", "RESULTS_ROOT"):
        configured = os.environ.get(env_name, "").strip()
        if configured:
            return Path(configured).expanduser() / "benchmark_max_jobs_combined.json"

    submit_dir = os.environ.get("SLURM_SUBMIT_DIR", "").strip()
    if submit_dir:
        return Path(submit_dir).expanduser() / "benchmark_results" / "benchmark_max_jobs_combined.json"

    return script_dir / "benchmark" / "benchmark_max_jobs_combined.json"


# ---------------------------------------------------------------------------
# Download (run once per accession)
# ---------------------------------------------------------------------------

def download_with_fastbiodl(
    accession: str,
    sra_dir: Path,
    segment_size_mb: int,
    max_segments: int,
    max_retries: int,
) -> Tuple[Path, float, str]:
    """Download one accession using the FastBioDL segmented downloader."""
    try:
        import aiohttp
        import fastbiodl_upgrade as fb
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"FastBioDL downloader import failed: {exc}") from exc

    url_acc_pairs = fb.get_ncbi_urls(accession, field="sra_ftp")
    if not url_acc_pairs:
        raise RuntimeError(f"No SRA download URL found for {accession}")

    url, source_acc = url_acc_pairs[0]
    filename = os.path.basename(url)
    local_path = sra_dir / source_acc / filename
    local_path.parent.mkdir(parents=True, exist_ok=True)

    fb.download_dir = str(sra_dir)
    fb.transfer_done = mp.Value("i", 0)
    fb.download_process_status = mp.Array("i", [1])

    process_counter = mp.Value("Q", 0)
    active_connections = mp.Value("i", 0)
    disk_reserved_bytes = mp.Value("Q", 0)
    min_pending_conversion_bytes = mp.Value("Q", 0)

    async def _download_once() -> Tuple[bool, bool, int]:
        timeout = aiohttp.ClientTimeout(total=3600, connect=60, sock_read=300)
        connector = aiohttp.TCPConnector(
            limit=max_segments,
            limit_per_host=max_segments,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": "fastbiodl/3.0"},
        ) as session:
            downloader = fb.SegmentedDownloader(
                session=session,
                url=url,
                local_path=str(local_path),
                segment_size=max(1, int(segment_size_mb)) * 1024 * 1024,
                max_segments=max(1, int(max_segments)),
                process_id=0,
                process_counter=process_counter,
                active_connections=active_connections,
                disk_reserved_bytes=disk_reserved_bytes,
                min_pending_conversion_bytes=min_pending_conversion_bytes,
                disk_safety_margin_bytes=0,
                max_retries=max(0, int(max_retries)),
            )
            return await downloader.download_with_resume()

    t0 = time.perf_counter()
    success, was_paused, _connections = asyncio.run(_download_once())
    elapsed = time.perf_counter() - t0

    if not success:
        state = "paused" if was_paused else "failed"
        raise RuntimeError(f"FastBioDL downloader {state} for {accession} ({url})")

    if not local_path.exists():
        raise RuntimeError(f"FastBioDL reported success but file missing: {local_path}")

    return local_path, elapsed, url


def find_sra_file(sra_dir: Path, accession: str) -> Optional[Path]:
    """Find the downloaded SRA file under common naming variants."""
    candidates = [
        sra_dir / accession,
        sra_dir / f"{accession}.sra",
        sra_dir / f"{accession}.sralite.1",
        sra_dir / f"{accession}.sralite.2",
    ]
    for c in candidates:
        if c.exists() and c.is_file():
            return c

    for p in sorted(sra_dir.rglob(f"{accession}*")):
        if p.is_file() and (p.suffix == ".sra" or p.name.startswith(accession)):
            return p
    return None


# ---------------------------------------------------------------------------
# Input staging
# ---------------------------------------------------------------------------

def prepare_inputs(master_sra: Path, target_dir: Path, accession: str, n_copies: int = 3) -> List[Path]:
    """Materialize ``n_copies`` independent SRA inputs in ``target_dir``."""
    clean_dir(target_dir)
    suffix = "".join(master_sra.suffixes) or ".sra"
    copies: List[Path] = []
    for i in range(1, int(n_copies) + 1):
        dst = target_dir / f"{accession}_copy{i}{suffix}"
        shutil.copy2(master_sra, dst)
        copies.append(dst)
    return copies


def run_cmd(cmd: List[str], timeout_s: int = 0) -> Tuple[int, float, str, str]:
    """Run a command and return (rc, elapsed_s, stdout, stderr)."""
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=(timeout_s if timeout_s > 0 else None),
        )
        return proc.returncode, time.perf_counter() - t0, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        elapsed = time.perf_counter() - t0
        return 124, elapsed, exc.stdout or "", (exc.stderr or "") + "\nTIMEOUT"


def run_pigz_to_dst(pigz_cmd: str, fq_path: Path, gz_dst: Path, threads: int) -> Tuple[int, float, float, str]:
    """
    Compress one FASTQ with pigz, redirecting stdout directly to ``gz_dst``.

    Returns (rc, t_start, t_end, stderr_tail). The source FASTQ is removed on
    success to mirror in-place pigz semantics.
    """
    gz_dst.parent.mkdir(parents=True, exist_ok=True)
    t_start = time.perf_counter()
    try:
        with open(gz_dst, "wb") as fh_out:
            proc = subprocess.run(
                [pigz_cmd, "-1", "-c", "-p", str(max(1, int(threads))), str(fq_path)],
                stdout=fh_out,
                stderr=subprocess.PIPE,
                timeout=3600,
            )
        rc = proc.returncode
        err = (proc.stderr or b"").decode("utf-8", errors="replace")
        if rc == 0:
            try:
                fq_path.unlink()
            except FileNotFoundError:
                pass
        return rc, t_start, time.perf_counter(), err
    except subprocess.TimeoutExpired as exc:
        err = exc.stderr.decode("utf-8", errors="replace") if exc.stderr else "TIMEOUT"
        return 124, t_start, time.perf_counter(), err


# ---------------------------------------------------------------------------
# Strict sequential baseline (no fasterq / pigz overlap)
# ---------------------------------------------------------------------------

def run_strict_sequential_baseline(
    inputs: List[Path],
    fastq_dir: Path,
    pigz_dir: Path,
    fasterq_cmd: str,
    pigz_cmd: str,
    threads: int,
    pigz_threads: int,
) -> Dict[str, object]:
    """
    For each SRA input, run fasterq-dump to completion (FASTQ written to
    ``fastq_dir``), then pigz every produced FASTQ with output streamed directly
    into ``pigz_dir``. Inputs are processed strictly one after another.
    """
    clean_dir(fastq_dir)
    clean_dir(pigz_dir)
    tmp_root = fastq_dir / "_tmp"
    tmp_root.mkdir(parents=True, exist_ok=True)

    converted = 0
    failed = 0
    gz_outputs: List[str] = []
    per_input: List[Dict[str, object]] = []

    t0 = time.perf_counter()
    for idx, sra_path in enumerate(inputs, start=1):
        out_dir = fastq_dir / f"job_{idx}"
        temp_dir = tmp_root / f"job_{idx}"
        out_dir.mkdir(parents=True, exist_ok=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        gz_dst_dir = pigz_dir / f"job_{idx}"
        gz_dst_dir.mkdir(parents=True, exist_ok=True)

        rc_fq, fq_s, _out, err_fq = run_cmd(
            [
                fasterq_cmd,
                "--threads", str(max(1, int(threads))),
                "--temp", str(temp_dir),
                "--outdir", str(out_dir),
                "--split-3",
                "--skip-technical",
                str(sra_path),
            ],
            timeout_s=7200,
        )
        if rc_fq != 0:
            failed += 1
            per_input.append({
                "input": str(sra_path),
                "status": "failed_fasterq",
                "fasterq_s": round(fq_s, 3),
                "error_tail": err_fq.strip()[-500:],
            })
            continue

        fastq_files = sorted(out_dir.glob("*.fastq"))
        if not fastq_files:
            failed += 1
            per_input.append({
                "input": str(sra_path),
                "status": "failed_no_fastq",
                "fasterq_s": round(fq_s, 3),
                "error_tail": "No FASTQ files produced",
            })
            continue

        pigz_s_total = 0.0
        pigz_ok = True
        pigz_err = ""
        produced: List[str] = []
        for fq in fastq_files:
            gz_dst = gz_dst_dir / (fq.name + ".gz")
            rc_pz, t_pz_start, t_pz_end, err_pz = run_pigz_to_dst(
                pigz_cmd=pigz_cmd, fq_path=fq, gz_dst=gz_dst, threads=pigz_threads,
            )
            pigz_s_total += (t_pz_end - t_pz_start)
            if rc_pz != 0:
                pigz_ok = False
                pigz_err = err_pz.strip()[-500:]
                break
            produced.append(str(gz_dst))

        if not pigz_ok:
            failed += 1
            per_input.append({
                "input": str(sra_path),
                "status": "failed_pigz",
                "fasterq_s": round(fq_s, 3),
                "pigz_s": round(pigz_s_total, 3),
                "error_tail": pigz_err,
            })
            continue

        gz_outputs.extend(produced)
        converted += 1
        per_input.append({
            "input": str(sra_path),
            "status": "ok",
            "fasterq_s": round(fq_s, 3),
            "pigz_s": round(pigz_s_total, 3),
            "gz_outputs": sorted(produced),
        })

    total_s = time.perf_counter() - t0
    return {
        "mode": "strict_sequential_no_overlap",
        "status": "ok" if failed == 0 else "partial",
        "total_s": round(total_s, 3),
        "converted_count": int(converted),
        "failed_count": int(failed),
        "moved_outputs_count": len(gz_outputs),
        "moved_outputs": sorted(gz_outputs),
        "fasterq_span_s": 0.0,
        "pigz_span_s": 0.0,
        "overlap_s": 0.0,
        "per_input": per_input,
    }


# ---------------------------------------------------------------------------
# Overlapped pipeline (replaces SRAConverter for measurement purposes)
# ---------------------------------------------------------------------------

def run_overlapped_pipeline(
    inputs: List[Path],
    fastq_dir: Path,
    pigz_dir: Path,
    fasterq_cmd: str,
    pigz_cmd: str,
    threads: int,
    max_conversion_jobs: int,
    max_pigz_jobs: int,
) -> Dict[str, object]:
    """
    Run fasterq-dump and pigz with explicit concurrency caps.

    Architecture:
      - A fasterq pool with ``max_conversion_jobs`` worker threads. Each worker
        calls one fasterq-dump process and waits for it. fasterq-dump output
        FASTQs are written to ``fastq_dir``.
      - A pigz pool with ``max_pigz_jobs`` worker threads. As soon as a
        fasterq-dump completes, one pigz task is submitted per produced FASTQ.
        pigz writes its compressed output directly to ``pigz_dir`` via
        ``pigz -c`` and stdout redirection. No post-run move.
      - The two pools are independent, so fasterq and pigz can overlap freely
        subject only to their per-pool caps.

    Returns a dict with total wall time, fasterq/pigz spans and overlap, plus
    per-input status records.
    """
    clean_dir(fastq_dir)
    clean_dir(pigz_dir)
    tmp_root = fastq_dir / "_tmp"
    tmp_root.mkdir(parents=True, exist_ok=True)

    state_lock = threading.Lock()
    fasterq_starts: List[float] = []
    fasterq_ends: List[float] = []
    pigz_starts: List[float] = []
    pigz_ends: List[float] = []
    gz_outputs: List[str] = []
    per_input: Dict[int, Dict[str, object]] = {}

    def do_fasterq(sra_path: Path, idx: int) -> Tuple[List[Path], float, float, int, str]:
        out_dir = fastq_dir / f"job_{idx}"
        tmp_dir = tmp_root / f"job_{idx}"
        out_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        t_start = time.perf_counter()
        with state_lock:
            fasterq_starts.append(t_start)
        rc, _elapsed, _out, err = run_cmd(
            [
                fasterq_cmd,
                "--threads", str(max(1, int(threads))),
                "--temp", str(tmp_dir),
                "--outdir", str(out_dir),
                "--split-3",
                "--skip-technical",
                str(sra_path),
            ],
            timeout_s=7200,
        )
        t_end = time.perf_counter()
        with state_lock:
            fasterq_ends.append(t_end)
        if rc != 0:
            return [], t_start, t_end, rc, err
        return sorted(out_dir.glob("*.fastq")), t_start, t_end, rc, err

    def do_pigz(fq: Path, gz_dst: Path) -> Tuple[bool, float, float, str]:
        rc, t_start, t_end, err = run_pigz_to_dst(
            pigz_cmd=pigz_cmd, fq_path=fq, gz_dst=gz_dst, threads=threads,
        )
        with state_lock:
            pigz_starts.append(t_start)
            pigz_ends.append(t_end)
        return rc == 0, t_start, t_end, err

    n_conv = max(1, int(max_conversion_jobs))
    n_pigz = max(1, int(max_pigz_jobs))

    fasterq_pool = ThreadPoolExecutor(max_workers=n_conv, thread_name_prefix="fasterq")
    pigz_pool = ThreadPoolExecutor(max_workers=n_pigz, thread_name_prefix="pigz")

    t0 = time.perf_counter()
    try:
        fasterq_meta: Dict[Future, Tuple[Path, int]] = {}
        for idx, sra in enumerate(inputs, start=1):
            f = fasterq_pool.submit(do_fasterq, sra, idx)
            fasterq_meta[f] = (sra, idx)

        pigz_meta: Dict[Future, Tuple[Path, Path, int]] = {}

        for f in as_completed(list(fasterq_meta.keys())):
            sra, idx = fasterq_meta[f]
            try:
                fastq_files, t_fq_s, t_fq_e, rc, err = f.result()
            except Exception as exc:
                per_input[idx] = {
                    "input": str(sra),
                    "status": "fasterq_exception",
                    "error_tail": str(exc)[-500:],
                }
                continue

            if rc != 0:
                per_input[idx] = {
                    "input": str(sra),
                    "status": "failed_fasterq",
                    "fasterq_s": round(t_fq_e - t_fq_s, 3),
                    "error_tail": err.strip()[-500:],
                }
                continue
            if not fastq_files:
                per_input[idx] = {
                    "input": str(sra),
                    "status": "failed_no_fastq",
                    "fasterq_s": round(t_fq_e - t_fq_s, 3),
                    "error_tail": "No FASTQ files produced",
                }
                continue

            gz_dst_dir = pigz_dir / f"job_{idx}"
            gz_dst_dir.mkdir(parents=True, exist_ok=True)
            per_input[idx] = {
                "input": str(sra),
                "status": "ok",
                "fasterq_s": round(t_fq_e - t_fq_s, 3),
                "fastq_files": [str(p) for p in fastq_files],
                "gz_outputs": [],
                "pigz_files_failed": 0,
            }
            for fq in fastq_files:
                gz_dst = gz_dst_dir / (fq.name + ".gz")
                pf = pigz_pool.submit(do_pigz, fq, gz_dst)
                pigz_meta[pf] = (fq, gz_dst, idx)

        for pf in as_completed(list(pigz_meta.keys())):
            fq, gz_dst, idx = pigz_meta[pf]
            try:
                ok, t_pz_s, t_pz_e, err = pf.result()
            except Exception as exc:
                ok = False
                err = str(exc)
            entry = per_input.setdefault(idx, {"input": "<unknown>", "status": "ok"})
            if ok:
                gz_outputs.append(str(gz_dst))
                entry.setdefault("gz_outputs", []).append(str(gz_dst))
            else:
                entry["pigz_files_failed"] = int(entry.get("pigz_files_failed", 0)) + 1
                entry["pigz_error_tail"] = (err or "").strip()[-500:]
                if entry.get("status") == "ok":
                    entry["status"] = "failed_pigz"
    finally:
        fasterq_pool.shutdown(wait=True)
        pigz_pool.shutdown(wait=True)

    total_s = time.perf_counter() - t0

    converted = sum(
        1 for r in per_input.values()
        if r.get("status") == "ok" and int(r.get("pigz_files_failed", 0)) == 0
    )
    failed = len(inputs) - converted

    fq_start = min(fasterq_starts) if fasterq_starts else 0.0
    fq_end = max(fasterq_ends) if fasterq_ends else 0.0
    pz_start = min(pigz_starts) if pigz_starts else 0.0
    pz_end = max(pigz_ends) if pigz_ends else 0.0

    fasterq_span_s = max(0.0, fq_end - fq_start) if (fasterq_starts and fasterq_ends) else 0.0
    pigz_span_s = max(0.0, pz_end - pz_start) if (pigz_starts and pigz_ends) else 0.0
    overlap_s = 0.0
    if fasterq_starts and fasterq_ends and pigz_starts and pigz_ends:
        overlap_s = max(0.0, min(fq_end, pz_end) - max(fq_start, pz_start))

    return {
        "mode": f"overlap_c{n_conv}_p{n_pigz}",
        "status": "ok" if failed == 0 else "partial",
        "total_s": round(total_s, 3),
        "converted_count": int(converted),
        "failed_count": int(failed),
        "moved_outputs_count": len(gz_outputs),
        "moved_outputs": sorted(gz_outputs),
        "fasterq_span_s": round(fasterq_span_s, 3),
        "pigz_span_s": round(pigz_span_s, 3),
        "overlap_s": round(overlap_s, 3),
        "per_input": [per_input[idx] for idx in sorted(per_input.keys())],
    }


# ---------------------------------------------------------------------------
# Repeat aggregation and reporting
# ---------------------------------------------------------------------------

def is_successful(result: Dict[str, object]) -> bool:
    return (
        str(result.get("status", "")) == "ok"
        and int(result.get("failed_count", 1)) == 0
    )


def aggregate_repeats(repeats: List[Dict[str, object]]) -> Dict[str, object]:
    """Compute simple summary statistics across repeated runs."""
    successful = [r for r in repeats if is_successful(r)]
    totals = [float(r["total_s"]) for r in successful if r.get("total_s") not in (None, float("inf"))]
    overlaps = [float(r.get("overlap_s", 0.0)) for r in successful]
    summary: Dict[str, object] = {
        "n_repeats": len(repeats),
        "n_successful": len(successful),
        "n_failed": len(repeats) - len(successful),
    }
    if totals:
        summary.update({
            "min_total_s": round(min(totals), 3),
            "max_total_s": round(max(totals), 3),
            "mean_total_s": round(statistics.mean(totals), 3),
            "median_total_s": round(statistics.median(totals), 3),
            "stdev_total_s": round(statistics.pstdev(totals), 3) if len(totals) > 1 else 0.0,
        })
    if overlaps:
        summary["mean_overlap_s"] = round(statistics.mean(overlaps), 3)
        summary["median_overlap_s"] = round(statistics.median(overlaps), 3)
    return summary


def median_total_s(summary: Dict[str, object]) -> float:
    val = summary.get("median_total_s")
    if val is None:
        return float("inf")
    return float(val)


def print_grid_summary(rows: List[Dict[str, object]]) -> None:
    print("\nGrid results (lower median_total_s is better):")
    print(f"{'conv':>4} {'pigz':>4} {'n_ok':>5} {'min_s':>9} {'median_s':>10} {'mean_s':>9} {'max_s':>9} {'stdev_s':>9}")
    for row in rows:
        s = row.get("summary", {})
        if s.get("n_successful", 0) == 0:
            print(
                f"{row['max_conversion_jobs']:>4} {row['max_pigz_jobs']:>4} "
                f"{s.get('n_successful', 0):>5} {'-':>9} {'-':>10} {'-':>9} {'-':>9} {'-':>9}"
            )
            continue
        print(
            f"{row['max_conversion_jobs']:>4} {row['max_pigz_jobs']:>4} "
            f"{s.get('n_successful', 0):>5} "
            f"{float(s.get('min_total_s', 0.0)):>9.3f} "
            f"{float(s.get('median_total_s', 0.0)):>10.3f} "
            f"{float(s.get('mean_total_s', 0.0)):>9.3f} "
            f"{float(s.get('max_total_s', 0.0)):>9.3f} "
            f"{float(s.get('stdev_total_s', 0.0)):>9.3f}"
        )


# ---------------------------------------------------------------------------
# Per-accession driver
# ---------------------------------------------------------------------------

def benchmark_one_accession(
    accession: str,
    args: argparse.Namespace,
    fasterq_cmd: str,
    pigz_cmd: str,
    sra_root: Path,
    fastq_root: Path,
    pigz_root: Path,
) -> Dict[str, object]:
    """Run the full benchmark suite for a single accession and return its report."""
    print(f"\n========== Accession: {accession} ==========")

    sra_dir_acc = sra_root / accession
    fastq_dir_acc = fastq_root / accession
    pigz_dir_acc = pigz_root / accession
    sra_dir_acc.mkdir(parents=True, exist_ok=True)
    fastq_dir_acc.mkdir(parents=True, exist_ok=True)
    pigz_dir_acc.mkdir(parents=True, exist_ok=True)

    download_dir = sra_dir_acc / "download"
    download_dir.mkdir(parents=True, exist_ok=True)

    # --- Download once -----------------------------------------------------
    print(f"[{accession}][download] FastBioDL segmented download")
    try:
        downloaded_sra, dl_s, source_url = download_with_fastbiodl(
            accession=accession,
            sra_dir=download_dir,
            segment_size_mb=args.download_segment_size_mb,
            max_segments=args.download_max_segments,
            max_retries=args.download_max_retries,
        )
    except Exception as exc:
        print(f"[{accession}] Download failed: {exc}", file=sys.stderr)
        return {"accession": accession, "status": "download_failed", "error": str(exc)}

    if not downloaded_sra.exists():
        found = find_sra_file(download_dir, accession)
        if found is None:
            msg = f"Could not locate downloaded SRA under {download_dir}"
            print(f"[{accession}] {msg}", file=sys.stderr)
            return {"accession": accession, "status": "download_missing", "error": msg}
        downloaded_sra = found

    sra_size = downloaded_sra.stat().st_size
    print(
        f"[{accession}][download] Done: {downloaded_sra} "
        f"({sra_size / (1024 ** 3):.3f} GB) in {dl_s:.2f}s"
    )

    runs_sra_root = sra_dir_acc / "runs"
    runs_fastq_root = fastq_dir_acc / "runs"
    runs_pigz_root = pigz_dir_acc / "runs"
    runs_sra_root.mkdir(parents=True, exist_ok=True)
    runs_fastq_root.mkdir(parents=True, exist_ok=True)
    runs_pigz_root.mkdir(parents=True, exist_ok=True)

    repeats = max(1, int(args.repeats))

    # --- Strict sequential baseline ---------------------------------------
    print(f"[{accession}][seq] Strict sequential baseline x{repeats}")
    seq_repeats: List[Dict[str, object]] = []
    for r in range(1, repeats + 1):
        run_id = f"seq_r{r}"
        sra_dir = runs_sra_root / run_id
        fastq_dir = runs_fastq_root / run_id
        pigz_dir = runs_pigz_root / run_id
        try:
            seq_inputs = prepare_inputs(downloaded_sra, sra_dir, accession, n_copies=3)
            res = run_strict_sequential_baseline(
                inputs=seq_inputs,
                fastq_dir=fastq_dir,
                pigz_dir=pigz_dir,
                fasterq_cmd=fasterq_cmd,
                pigz_cmd=pigz_cmd,
                threads=args.threads,
                pigz_threads=args.threads,
            )
        except Exception as exc:
            res = {
                "status": "failed", "error": str(exc),
                "total_s": float("inf"),
                "converted_count": 0, "failed_count": 3,
                "moved_outputs_count": 0, "moved_outputs": [],
                "fasterq_span_s": 0.0, "pigz_span_s": 0.0, "overlap_s": 0.0,
            }
        res["repeat"] = r
        seq_repeats.append(res)
        print(
            f"[{accession}][seq][r{r}] total={float(res.get('total_s', float('inf'))):.3f}s "
            f"converted={res.get('converted_count', 0)} failed={res.get('failed_count', 0)}"
        )
        # Free disk before the next repeat / configuration.
        remove_dirs(sra_dir, fastq_dir, pigz_dir)
    sequential_block = {"repeats": seq_repeats, "summary": aggregate_repeats(seq_repeats)}

    # --- 3x3 grid search --------------------------------------------------
    print(f"[{accession}][grid] 3x3 grid search x{repeats}")
    grid_rows: List[Dict[str, object]] = []
    for max_c, max_p in GRID_PAIRS:
        cell_repeats: List[Dict[str, object]] = []
        for r in range(1, repeats + 1):
            run_id = f"grid_c{max_c}_p{max_p}_r{r}"
            sra_dir = runs_sra_root / run_id
            fastq_dir = runs_fastq_root / run_id
            pigz_dir = runs_pigz_root / run_id
            try:
                run_inputs = prepare_inputs(downloaded_sra, sra_dir, accession, n_copies=3)
                run_result = run_overlapped_pipeline(
                    inputs=run_inputs,
                    fastq_dir=fastq_dir,
                    pigz_dir=pigz_dir,
                    fasterq_cmd=fasterq_cmd,
                    pigz_cmd=pigz_cmd,
                    threads=args.threads,
                    max_conversion_jobs=max_c,
                    max_pigz_jobs=max_p,
                )
                res: Dict[str, object] = {
                    **run_result,
                    "max_conversion_jobs": max_c,
                    "max_pigz_jobs": max_p,
                }
            except Exception as exc:
                res = {
                    "status": "failed", "error": str(exc),
                    "max_conversion_jobs": max_c, "max_pigz_jobs": max_p,
                    "total_s": float("inf"),
                    "converted_count": 0, "failed_count": 3,
                    "moved_outputs_count": 0, "moved_outputs": [],
                    "fasterq_span_s": 0.0, "pigz_span_s": 0.0, "overlap_s": 0.0,
                }
            res["repeat"] = r
            cell_repeats.append(res)
            print(
                f"[{accession}][grid][c{max_c}_p{max_p}][r{r}] "
                f"total={float(res.get('total_s', float('inf'))):.3f}s "
                f"converted={res.get('converted_count', 0)} failed={res.get('failed_count', 0)}"
            )
            # Free disk before the next repeat / grid point.
            remove_dirs(sra_dir, fastq_dir, pigz_dir)
        grid_rows.append({
            "max_conversion_jobs": max_c,
            "max_pigz_jobs": max_p,
            "repeats": cell_repeats,
            "summary": aggregate_repeats(cell_repeats),
        })

    grid_with_data = [g for g in grid_rows if g["summary"].get("n_successful", 0) > 0]
    if not grid_with_data:
        print(f"[{accession}] All grid configurations failed.", file=sys.stderr)
        return {
            "accession": accession,
            "status": "grid_all_failed",
            "download": {
                "elapsed_s": round(dl_s, 3),
                "source_url": source_url,
                "sra_path": str(downloaded_sra),
                "sra_size_bytes": sra_size,
            },
            "sequential_baseline": sequential_block,
            "grid": grid_rows,
        }

    best_grid = min(grid_with_data, key=lambda g: median_total_s(g["summary"]))
    print_grid_summary(grid_rows)
    print(
        f"\n[{accession}] Best grid (by median): "
        f"max_conversion_jobs={best_grid['max_conversion_jobs']}, "
        f"max_pigz_jobs={best_grid['max_pigz_jobs']} "
        f"(median={median_total_s(best_grid['summary']):.3f}s)"
    )

    accession_report: Dict[str, object] = {
        "accession": accession,
        "status": "ok",
        "download": {
            "elapsed_s": round(dl_s, 3),
            "source_url": source_url,
            "sra_path": str(downloaded_sra),
            "sra_size_bytes": sra_size,
        },
        "sequential_baseline": sequential_block,
        "grid": grid_rows,
        "best_grid": {
            "max_conversion_jobs": best_grid["max_conversion_jobs"],
            "max_pigz_jobs": best_grid["max_pigz_jobs"],
            "summary": best_grid["summary"],
        },
    }

    # Per-accession JSON next to the combined JSON.
    if args.json_out:
        per_acc_dir = Path(args.json_out).expanduser().resolve().parent
    else:
        per_acc_dir = default_json_out(Path(__file__).resolve().parent).parent
    per_acc_dir.mkdir(parents=True, exist_ok=True)
    per_acc_json = per_acc_dir / f"benchmark_max_jobs_{accession}.json"
    with per_acc_json.open("w", encoding="utf-8") as fh:
        json.dump(accession_report, fh, indent=2)
    print(f"[{accession}] Per-accession JSON: {per_acc_json}")

    if args.cleanup_work_root:
        remove_dirs(runs_sra_root, runs_fastq_root, runs_pigz_root)

    return accession_report


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Multi-accession, multi-repeat benchmark over the strict sequential "
            "baseline and the 3x3 grid (max_conversion_jobs, max_pigz_jobs)."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "accessions",
        nargs="+",
        help="One or more accessions (e.g., SRR390728 SRR1234567 SRR7654321)",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Number of independent end-to-end runs per (accession, configuration)",
    )
    parser.add_argument(
        "--work-root",
        default=nvme_path("fastbiodl_gridsearch"),
        help="Local scratch working directory (transient; used only when --sra/--fastq/--pigz-out-dir not given)",
    )
    parser.add_argument(
        "--sra-out-dir",
        default="",
        help="Storage tier for the master SRA download and per-run input copies. Defaults to <work_root>/sra",
    )
    parser.add_argument(
        "--fastq-out-dir",
        default="",
        help="Storage tier for intermediate FASTQ output. Defaults to <work_root>/fastq",
    )
    parser.add_argument(
        "--pigz-out-dir",
        default="",
        help="Storage tier for final .fastq.gz output. Defaults to <work_root>/pigz",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="Optional explicit combined JSON report output path",
    )
    parser.add_argument("--threads", type=int, default=8, help="threads passed to fasterq-dump and pigz")
    parser.add_argument("--download-segment-size-mb", type=int, default=512, help="FastBioDL segment size MB")
    parser.add_argument("--download-max-segments", type=int, default=8, help="FastBioDL max segments")
    parser.add_argument("--download-max-retries", type=int, default=3, help="FastBioDL max retries")
    parser.add_argument(
        "--cleanup-work-root",
        action="store_true",
        help="Also remove the per-accession runs subdirectories at the end",
    )
    args = parser.parse_args()

    accessions = [a.strip() for a in args.accessions if a.strip()]
    if not accessions:
        print("At least one accession is required", file=sys.stderr)
        return 2

    script_dir = Path(__file__).resolve().parent
    prepare_tool_path(script_dir)

    try:
        fasterq_cmd = resolve_tool("fasterq-dump", script_dir)
        pigz_cmd = resolve_tool("pigz", script_dir)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    work_root = Path(args.work_root).resolve()
    sra_root = Path(args.sra_out_dir).expanduser().resolve() if args.sra_out_dir else (work_root / "sra")
    fastq_root = Path(args.fastq_out_dir).expanduser().resolve() if args.fastq_out_dir else (work_root / "fastq")
    pigz_root = Path(args.pigz_out_dir).expanduser().resolve() if args.pigz_out_dir else (work_root / "pigz")

    out_json = (
        Path(args.json_out).expanduser().resolve()
        if args.json_out
        else default_json_out(script_dir)
    )

    work_root.mkdir(parents=True, exist_ok=True)
    sra_root.mkdir(parents=True, exist_ok=True)
    fastq_root.mkdir(parents=True, exist_ok=True)
    pigz_root.mkdir(parents=True, exist_ok=True)

    print(f"Accessions:     {accessions}")
    print(f"Repeats:        {args.repeats}")
    print(f"Work root:      {work_root}")
    print(f"SRA   out dir:  {sra_root}")
    print(f"FASTQ out dir:  {fastq_root}")
    print(f"PIGZ  out dir:  {pigz_root}")
    print(f"JSON report:    {out_json}")

    accession_reports: List[Dict[str, object]] = []
    for accession in accessions:
        try:
            report = benchmark_one_accession(
                accession=accession,
                args=args,
                fasterq_cmd=fasterq_cmd,
                pigz_cmd=pigz_cmd,
                sra_root=sra_root,
                fastq_root=fastq_root,
                pigz_root=pigz_root,
            )
        except Exception as exc:
            report = {"accession": accession, "status": "exception", "error": str(exc)}
        accession_reports.append(report)

    combined = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "accessions": accessions,
        "repeats": int(args.repeats),
        "search_space": {
            "max_conversion_jobs": [1, 2, 3],
            "max_pigz_jobs": [1, 2, 3],
            "copies_per_run": 3,
        },
        "storage": {
            "work_root": str(work_root),
            "sra_out_dir": str(sra_root),
            "fastq_out_dir": str(fastq_root),
            "pigz_out_dir": str(pigz_root),
        },
        "tools": {"fasterq_dump": fasterq_cmd, "pigz": pigz_cmd},
        "results": accession_reports,
    }

    out_json.parent.mkdir(parents=True, exist_ok=True)
    with out_json.open("w", encoding="utf-8") as fh:
        json.dump(combined, fh, indent=2)
    print(f"\nCombined JSON report: {out_json}")

    if args.cleanup_work_root:
        print(f"Cleaning work root: {work_root}")
        remove_dirs(work_root)

    any_failed = any(r.get("status") not in ("ok",) for r in accession_reports)
    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
