#!/usr/bin/env python3
"""
benchmark_fasterq_pigz_overlap.py

Simple single-accession experiment to test whether overlapping compression (`pigz`)
with conversion (`fasterq-dump`) yields speedup vs. a strictly sequential flow.

Workflow
--------
1. Download one accession with FastBioDL's segmented downloader.
2. Run sequential mode:
   - fasterq-dump completes first
   - pigz compresses produced .fastq files
3. Run overlapped mode in a separate directory:
   - fasterq-dump starts
   - monitor output dir; when a .fastq file becomes stable, start pigz on it
4. Print timing + computed speedup.

Notes
-----
- This script does not prove causality across all datasets; it is a quick sanity test.
- If fasterq-dump only exposes final files near completion, overlap benefit may be small.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


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


def resolve_tool(binary: str, script_dir: Path) -> str:
    """Resolve tool from PATH, then fallback to bundled sratoolkit in repo."""
    in_path = shutil.which(binary)
    if in_path:
        return in_path

    bundled = script_dir / "sratoolkit.3.1.0-ubuntu64" / "bin" / binary
    if bundled.exists() and os.access(bundled, os.X_OK):
        return str(bundled)

    raise RuntimeError(
        f"Required command not found: {binary}. Tried PATH and {bundled}"
    )


def download_with_fastbiodl(
    accession: str,
    sra_dir: Path,
    segment_size_mb: int,
    max_segments: int,
    max_retries: int,
) -> Tuple[Path, float, str]:
    """
    Download one accession using FastBioDL's segmented downloader class.

    Returns: (local_sra_path, elapsed_seconds, source_url)
    """
    try:
        import aiohttp
        import fastbiodl_upgrade as fb
    except Exception as exc:
        raise RuntimeError(f"FastBioDL downloader import failed: {exc}") from exc

    url_acc_pairs = fb.get_ncbi_urls(accession, field="sra_ftp")
    if not url_acc_pairs:
        raise RuntimeError(f"No SRA download URL found via FastBioDL lookup for {accession}")

    url, _source_acc = url_acc_pairs[0]
    filename = os.path.basename(url)
    relative_path = os.path.join(accession, filename)
    local_path = sra_dir / relative_path
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # SegmentedDownloader relies on module-level globals from fastbiodl_upgrade.
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
        raise RuntimeError(f"FastBioDL reported success but file is missing: {local_path}")

    return local_path, elapsed, url


def find_sra_file(sra_dir: Path, accession: str) -> Optional[Path]:
    """Find the downloaded SRA file with common naming variants."""
    candidates = [
        sra_dir / accession,
        sra_dir / f"{accession}.sra",
        sra_dir / f"{accession}.sralite.1",
        sra_dir / f"{accession}.sralite.2",
        sra_dir / accession / accession,
        sra_dir / accession / f"{accession}.sra",
        sra_dir / accession / f"{accession}.sralite.1",
        sra_dir / accession / f"{accession}.sralite.2",
        sra_dir / f"{accession}.1",
        sra_dir / f"{accession}.2",
        sra_dir / accession / f"{accession}.1",
        sra_dir / accession / f"{accession}.2",
    ]
    for c in candidates:
        if c.exists() and c.is_file():
            return c

    pattern = re.compile(
        rf"(?:^|/){re.escape(accession)}(?:\\.sra|\\.lite\\.\\d+|\\.sralite\\.\\d+|\\.\\d+)?$",
        re.IGNORECASE,
    )
    for p in sorted(sra_dir.rglob(f"{accession}*")):
        if p.is_file() and pattern.search(str(p)):
            return p
    return None


def clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def collect_fastq_files(out_dir: Path) -> List[Path]:
    return sorted(out_dir.glob("*.fastq"))


def compress_all_fastq_sequential(
    fastq_files: List[Path], pigz_cmd: str, pigz_threads: int
) -> Tuple[float, List[Dict[str, object]]]:
    """Compress .fastq files one by one after conversion completes."""
    t0 = time.perf_counter()
    file_results: List[Dict[str, object]] = []

    for fq in fastq_files:
        rc, elapsed, _, err = run_cmd([pigz_cmd, "-1", "-p", str(pigz_threads), str(fq)])
        file_results.append(
            {
                "file": str(fq),
                "rc": rc,
                "elapsed_s": round(elapsed, 3),
                "stderr_tail": err.strip()[-500:],
            }
        )
        if rc != 0:
            raise RuntimeError(f"pigz failed for {fq}: {err.strip()[-500:]}")

    return time.perf_counter() - t0, file_results


def run_sequential_mode(
    sra_path: Path,
    seq_dir: Path,
    fasterq_cmd: str,
    threads: int,
    pigz_cmd: str,
    pigz_threads: int,
) -> Dict[str, object]:
    """Run fasterq-dump then pigz in strict sequence."""
    fastq_dir = seq_dir / "fastq"
    tmp_dir = seq_dir / "tmp"
    clean_dir(fastq_dir)
    clean_dir(tmp_dir)

    rc, convert_s, out, err = run_cmd(
        [
            fasterq_cmd,
            "--threads",
            str(threads),
            "--outdir",
            str(fastq_dir),
            "--temp",
            str(tmp_dir),
            str(sra_path),
        ]
    )
    if rc != 0:
        raise RuntimeError(f"fasterq-dump (sequential) failed: {err.strip()[-500:]}")

    fastq_files = collect_fastq_files(fastq_dir)
    if not fastq_files:
        raise RuntimeError("No .fastq files were produced in sequential mode")

    compress_s, file_results = compress_all_fastq_sequential(
        fastq_files, pigz_cmd, pigz_threads
    )
    total_s = convert_s + compress_s

    return {
        "mode": "sequential",
        "convert_s": round(convert_s, 3),
        "compress_s": round(compress_s, 3),
        "total_s": round(total_s, 3),
        "fasterq_stdout_tail": out.strip()[-500:],
        "fasterq_stderr_tail": err.strip()[-500:],
        "compressed_files": file_results,
    }


def discover_stable_fastq(
    fastq_dir: Path,
    size_snapshot: Dict[str, int],
    stable_counts: Dict[str, int],
    min_stable_cycles: int,
) -> Set[Path]:
    """Return files whose size stopped changing for enough polling cycles."""
    ready: Set[Path] = set()
    for fq in sorted(fastq_dir.glob("*.fastq")):
        key = str(fq)
        try:
            size = fq.stat().st_size
        except FileNotFoundError:
            continue

        prev_size = size_snapshot.get(key)
        if prev_size is None or prev_size != size:
            size_snapshot[key] = size
            stable_counts[key] = 0
            continue

        stable_counts[key] = stable_counts.get(key, 0) + 1
        if stable_counts[key] >= min_stable_cycles and size > 0:
            ready.add(fq)

    return ready


def run_overlapped_mode(
    sra_path: Path,
    par_dir: Path,
    fasterq_cmd: str,
    threads: int,
    pigz_cmd: str,
    pigz_threads: int,
    poll_interval_s: float,
    min_stable_cycles: int,
) -> Dict[str, object]:
    """
    Start fasterq-dump and pigz overlapping.

    pigz starts for each .fastq once file size appears stable for a few polls.
    """
    fastq_dir = par_dir / "fastq"
    tmp_dir = par_dir / "tmp"
    clean_dir(fastq_dir)
    clean_dir(tmp_dir)

    t0 = time.perf_counter()
    fasterq = subprocess.Popen(
        [
            fasterq_cmd,
            "--threads",
            str(threads),
            "--outdir",
            str(fastq_dir),
            "--temp",
            str(tmp_dir),
            str(sra_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    size_snapshot: Dict[str, int] = {}
    stable_counts: Dict[str, int] = {}
    compressing: Dict[str, subprocess.Popen] = {}
    completed: List[Dict[str, object]] = []

    while True:
        ready = discover_stable_fastq(
            fastq_dir=fastq_dir,
            size_snapshot=size_snapshot,
            stable_counts=stable_counts,
            min_stable_cycles=min_stable_cycles,
        )

        for fq in ready:
            key = str(fq)
            if key in compressing:
                continue
            if not fq.exists() or Path(str(fq) + ".gz").exists():
                continue
            compressing[key] = subprocess.Popen(
                [pigz_cmd, "-1", "-p", str(pigz_threads), str(fq)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

        finished_keys: List[str] = []
        for key, proc in compressing.items():
            rc = proc.poll()
            if rc is None:
                continue
            _, err = proc.communicate()
            completed.append(
                {
                    "file": key,
                    "rc": rc,
                    "stderr_tail": err.strip()[-500:],
                }
            )
            if rc != 0:
                raise RuntimeError(f"pigz failed in overlapped mode for {key}: {err.strip()[-500:]}")
            finished_keys.append(key)

        for key in finished_keys:
            compressing.pop(key, None)

        fasterq_done = fasterq.poll() is not None
        if fasterq_done:
            remaining_fastq = [p for p in fastq_dir.glob("*.fastq") if p.exists()]
            if not remaining_fastq and not compressing:
                break

            for fq in remaining_fastq:
                key = str(fq)
                if key in compressing:
                    continue
                if Path(str(fq) + ".gz").exists():
                    continue
                compressing[key] = subprocess.Popen(
                    [pigz_cmd, "-1", "-p", str(pigz_threads), str(fq)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )

        time.sleep(poll_interval_s)

    f_stdout, f_stderr = fasterq.communicate()
    f_rc = fasterq.returncode
    if f_rc != 0:
        raise RuntimeError(f"fasterq-dump (overlapped) failed: {f_stderr.strip()[-500:]}")

    total_s = time.perf_counter() - t0

    gz_files = sorted(str(p) for p in fastq_dir.glob("*.fastq.gz"))
    if not gz_files:
        raise RuntimeError("No .fastq.gz files were produced in overlapped mode")

    return {
        "mode": "overlapped",
        "total_s": round(total_s, 3),
        "fasterq_stdout_tail": f_stdout.strip()[-500:],
        "fasterq_stderr_tail": f_stderr.strip()[-500:],
        "compressed_files": completed,
        "gz_outputs": gz_files,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Single-accession benchmark: sequential fasterq+pigz vs overlapped mode"
    )
    parser.add_argument("accession", help="Single accession (e.g., SRR390728)")
    parser.add_argument("--work-root", default="benchmark/single_accession_overlap", help="Root output directory")
    parser.add_argument("--threads", type=int, default=8, help="Threads for fasterq-dump")
    parser.add_argument("--pigz-threads", type=int, default=8, help="Threads for pigz")
    parser.add_argument("--download-segment-size-mb", type=int, default=512,
                        help="FastBioDL download segment size in MB")
    parser.add_argument("--download-max-segments", type=int, default=8,
                        help="FastBioDL max segments for one file")
    parser.add_argument("--download-max-retries", type=int, default=3,
                        help="FastBioDL max retries for one file")
    parser.add_argument("--poll-interval", type=float, default=0.5, help="Polling interval for overlapped mode")
    parser.add_argument("--stable-cycles", type=int, default=3, help="Stable polls before pigz starts on a .fastq")
    parser.add_argument("--keep-existing", action="store_true", help="Reuse directories if they already exist")
    parser.add_argument("--json-out", default="", help="Optional explicit JSON output path")
    args = parser.parse_args()

    accession = args.accession.strip()
    if not accession:
        print("Accession is required", file=sys.stderr)
        return 2

    script_dir = Path(__file__).resolve().parent
    commands: Dict[str, str] = {}
    for cmd in ["fasterq-dump", "pigz"]:
        try:
            commands[cmd] = resolve_tool(cmd, script_dir)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2

    root = Path(args.work_root).resolve()
    sra_dir = root / "sra"
    seq_dir = root / "sequential"
    par_dir = root / "overlapped"

    if not args.keep_existing:
        clean_dir(root)
    else:
        root.mkdir(parents=True, exist_ok=True)

    sra_dir.mkdir(parents=True, exist_ok=True)
    seq_dir.mkdir(parents=True, exist_ok=True)
    par_dir.mkdir(parents=True, exist_ok=True)

    print(f"[1/4] Downloading accession with FastBioDL downloader: {accession}")
    try:
        sra_path, dl_s, source_url = download_with_fastbiodl(
            accession=accession,
            sra_dir=sra_dir,
            segment_size_mb=args.download_segment_size_mb,
            max_segments=args.download_max_segments,
            max_retries=args.download_max_retries,
        )
    except Exception as exc:
        print(f"FastBioDL download failed: {exc}", file=sys.stderr)
        return 1

    if sra_path is None or not sra_path.exists():
        sra_path = find_sra_file(sra_dir, accession)
    if sra_path is None:
        print(f"Could not find downloaded SRA file under: {sra_dir}", file=sys.stderr)
        return 1

    sra_size = sra_path.stat().st_size
    print(
        f"Downloaded: {sra_path} ({sra_size / (1024**3):.3f} GB) in {dl_s:.2f}s\n"
        f"Source URL: {source_url}"
    )

    print("[2/4] Running sequential mode (fasterq-dump, then pigz)")
    seq_result = run_sequential_mode(
        sra_path=sra_path,
        seq_dir=seq_dir,
        fasterq_cmd=commands["fasterq-dump"],
        threads=args.threads,
        pigz_cmd=commands["pigz"],
        pigz_threads=args.pigz_threads,
    )

    print("[3/4] Running overlapped mode (fasterq-dump and pigz concurrently)")
    par_result = run_overlapped_mode(
        sra_path=sra_path,
        par_dir=par_dir,
        fasterq_cmd=commands["fasterq-dump"],
        threads=args.threads,
        pigz_cmd=commands["pigz"],
        pigz_threads=args.pigz_threads,
        poll_interval_s=args.poll_interval,
        min_stable_cycles=args.stable_cycles,
    )

    seq_total = float(seq_result["total_s"])
    par_total = float(par_result["total_s"])

    speedup = (seq_total / par_total) if par_total > 0 else 0.0
    pct_gain = (1.0 - (par_total / seq_total)) * 100.0 if seq_total > 0 else 0.0

    report = {
        "accession": accession,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "download": {
            "method": "fastbiodl_segmented",
            "elapsed_s": round(dl_s, 3),
            "source_url": source_url,
            "sra_path": str(sra_path),
            "sra_size_bytes": sra_size,
        },
        "sequential": seq_result,
        "overlapped": par_result,
        "comparison": {
            "speedup_seq_over_overlapped": round(speedup, 4),
            "overlapped_time_reduction_pct": round(pct_gain, 2),
            "is_overlapped_faster": par_total < seq_total,
        },
    }

    out_json = Path(args.json_out).resolve() if args.json_out else root / f"benchmark_overlap_{accession}.json"
    with out_json.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("[4/4] Results")
    print(f"Sequential total: {seq_total:.3f}s")
    print(f"Overlapped total: {par_total:.3f}s")
    print(f"Speedup (seq/overlapped): {speedup:.4f}x")
    print(f"Time reduction with overlap: {pct_gain:.2f}%")
    print(f"JSON report: {out_json}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
