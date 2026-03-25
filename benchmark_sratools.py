#!/usr/bin/env python3
"""
benchmark_sratools.py — Benchmark the classic sra-tools pipeline.

Pipeline
--------
  prefetch      → save .sra files to DISK (--sra-dir)
  fasterq-dump  → save .fastq to NVMe (--fastq-dir) [recommended for temp I/O]
  pigz          → compress .fastq → .fastq.gz, final output to DISK (--out-dir)

Timing model (matches fastbiodl benchmark)
------------------------------------------
  download_time   = wall time until ALL prefetch calls complete
  conversion_time = wall time for ALL fasterq-dump calls (after download done)
  compression_time= wall time for ALL pigz calls (after conversion done)

Usage
-----
  python benchmark_sratools.py -i accessions.txt \\
      --sra-dir  /mnt/disk/benchmark/sratools/sra \\
      --fastq-dir /mnt/raid0/benchmark/sratools/fastq \\
      --out-dir  /mnt/disk/benchmark/sratools/output \\
      --threads 8

Output
------
  benchmark_sratools_results_<timestamp>.json  — machine-readable timing
  benchmark_sratools_<timestamp>.log           — full command log
"""

import os
import sys
import time
import json
import shutil
import logging
import argparse
import datetime
import subprocess
import re
from pathlib import Path
from typing import Dict, List, Tuple


# ── Helpers ──────────────────────────────────────────────────────────────────

def _run(cmd: List[str], log: logging.Logger) -> Tuple[float, bool, str]:
    """Run a command, return (elapsed_seconds, success, stderr_tail)."""
    log.info(f"$ {' '.join(cmd)}")
    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=7200,
        )
        elapsed = time.time() - t0
        stderr_tail = result.stderr.decode(errors="replace").strip()[-500:]
        ok = result.returncode == 0
        if not ok:
            log.error(f"  FAILED (rc={result.returncode}): {stderr_tail}")
        return elapsed, ok, stderr_tail
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        log.error(f"  TIMED OUT after {elapsed:.0f}s")
        return elapsed, False, "timeout"
    except FileNotFoundError as e:
        elapsed = time.time() - t0
        log.error(f"  COMMAND NOT FOUND: {e}")
        return elapsed, False, str(e)


def _find_sra(sra_dir: str, acc: str) -> str | None:
    """
    Locate SRA-format file for an accession.
    Follows converter.py naming semantics: bare accession, .sra, .lite.N, .N.
    Also accepts .sralite.N observed from current NCBI URLs.
    """
    candidates = [
        os.path.join(sra_dir, acc),
        os.path.join(sra_dir, acc, acc),
        os.path.join(sra_dir, acc, f"{acc}.sra"),
        os.path.join(sra_dir, f"{acc}.sra"),
        os.path.join(sra_dir, f"{acc}.sralite.1"),
        os.path.join(sra_dir, acc, f"{acc}.sralite.1"),
        os.path.join(sra_dir, f"{acc}.sralite.2"),
        os.path.join(sra_dir, acc, f"{acc}.sralite.2"),
        os.path.join(sra_dir, f"{acc}.1"),
        os.path.join(sra_dir, acc, f"{acc}.1"),
        os.path.join(sra_dir, f"{acc}.2"),
        os.path.join(sra_dir, acc, f"{acc}.2"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c

    sra_file_re = re.compile(
        rf"(?:^|/){re.escape(acc)}(?:\.sra|\.lite\.\d+|\.\d+|\.sralite\.\d+)?$",
        re.IGNORECASE,
    )
    matches = []
    for path in Path(sra_dir).glob(f"**/{acc}*"):
        if path.is_file() and sra_file_re.search(str(path)):
            matches.append(path)
    if matches:
        return str(sorted(matches)[0])
    return None


def _human_bytes(num_bytes: int) -> str:
    """Format byte counts for readable logs."""
    value = float(max(0, num_bytes))
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if value < 1024.0 or unit == "PB":
            return f"{value:.2f}{unit}"
        value /= 1024.0
    return "0.00B"


def _compress_and_move_fastq(
    fq: Path,
    out_dir: str,
    threads: int,
    log: logging.Logger,
    comp_details: Dict[str, Dict[str, object]],
) -> Tuple[float, bool]:
    """Compress one FASTQ, move to out_dir, and clean up NVMe artifacts."""
    rel_key = str(fq)
    out_gz = os.path.join(out_dir, fq.name + ".gz")
    elapsed, ok, stderr_tail = _run(["pigz", "-1", "-p", str(threads), str(fq)], log)
    gz_src = str(fq) + ".gz"

    if ok and os.path.exists(gz_src):
        shutil.move(gz_src, out_gz)
        if os.path.exists(gz_src):
            os.remove(gz_src)

    if ok and fq.exists():
        try:
            fq.unlink()
        except OSError as e:
            log.warning(f"  Compressed but could not remove FASTQ {fq}: {e}")

    comp_details[rel_key] = {
        "ok": ok,
        "elapsed_s": round(elapsed, 2),
        "stderr_tail": stderr_tail,
        "out_file": out_gz,
    }
    log.info(f"  pigz {fq.name}: {'OK' if ok else 'FAILED'} in {elapsed:.1f}s")
    return elapsed, ok


def _flush_fastq_backlog(
    fastq_dir: str,
    out_dir: str,
    threads: int,
    log: logging.Logger,
    comp_details: Dict[str, Dict[str, object]],
) -> Tuple[int, float]:
    """Compress/move every pending .fastq one-by-one to free NVMe space."""
    pending = sorted(Path(fastq_dir).rglob("*.fastq"))
    if not pending:
        return 0, 0.0

    log.info(f"  Flushing {len(pending)} pending .fastq file(s) from NVMe")
    total_elapsed = 0.0
    for fq in pending:
        elapsed, _ = _compress_and_move_fastq(fq, out_dir, threads, log, comp_details)
        total_elapsed += elapsed
    return len(pending), total_elapsed


def _wait_for_nvme_headroom(
    work_dir: str,
    required_bytes: int,
    reserved_bytes: int,
    probing_sec: float,
    log: logging.Logger,
    fastq_dir: str,
    out_dir: str,
    threads: int,
    comp_details: Dict[str, Dict[str, object]],
) -> float:
    """
    Block until (free - reserved) >= required.
    While blocked, flush completed .fastq files one-by-one to reclaim NVMe space.
    Returns time spent in compression while waiting.
    """
    wait_cycles = 0
    compressed_elapsed = 0.0
    while True:
        free_bytes = shutil.disk_usage(work_dir).free
        effective_free = max(0, free_bytes - reserved_bytes)
        if effective_free >= required_bytes:
            if wait_cycles > 0:
                log.info(
                    f"  Disk admission granted (free-reserved {effective_free / (1024 ** 3):.2f}GB >= "
                    f"required {required_bytes / (1024 ** 3):.2f}GB)"
                )
            return compressed_elapsed

        wait_cycles += 1
        log.info(
            f"  Waiting for NVMe headroom (free-reserved {effective_free / (1024 ** 3):.2f}GB < "
            f"required {required_bytes / (1024 ** 3):.2f}GB; reserved {reserved_bytes / (1024 ** 3):.2f}GB)"
        )
        flushed, elapsed = _flush_fastq_backlog(
            fastq_dir=fastq_dir,
            out_dir=out_dir,
            threads=threads,
            log=log,
            comp_details=comp_details,
        )
        compressed_elapsed += elapsed
        if flushed == 0:
            time.sleep(probing_sec)


def _accession_group_name(input_path: str) -> str:
    """Derive a safe folder name from the accession input filename."""
    raw_name = Path(input_path).stem or "accessions"
    safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in raw_name)
    safe_name = safe_name.strip("._-")
    return safe_name or "accessions"


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark sra-tools: prefetch → fasterq-dump → pigz",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-i", "--input", required=True,
                        help="Text file with one SRA accession per line")
    parser.add_argument("--sra-dir",
                        default="sratools/sra",
                        help="Destination for prefetch output (DISK)")
    parser.add_argument("--fastq-dir",
                        default="/mnt/raid0/benchmark/sratools/fastq",
                        help="Destination for fasterq-dump output (NVMe recommended for temp I/O)")
    parser.add_argument("--out-dir",
                        default="sratools/output",
                        help="Final destination for .fastq.gz (DISK)")
    parser.add_argument("--threads", type=int, default=8,
                        help="Threads for fasterq-dump and pigz")
    parser.add_argument("--prefetch-max-size", default="100G",
                        help="--max-size passed to prefetch")
    parser.add_argument("--required-size-factor", type=float, default=10.0,
                        help="Required free-space factor before fasterq-dump")
    parser.add_argument("--reserve-size-factor", type=float, default=8.0,
                        help="Reserved-space factor used in free-reserved admission")
    parser.add_argument("--disk-safety-margin-gb", type=float, default=0.0,
                        help="Extra safety margin added to required bytes")
    parser.add_argument("--probing-sec", type=float, default=1.0,
                        help="Polling interval while waiting for NVMe headroom")
    parser.add_argument("--output-json",
                        help="Path for JSON results (default: auto-named with timestamp)")
    args = parser.parse_args()

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    accession_group = _accession_group_name(args.input)
    run_log_dir = os.path.join("logs", "sratools", accession_group)
    log_file = os.path.join(run_log_dir, f"benchmark_sratools_{ts}.log")
    json_file = args.output_json or os.path.join(run_log_dir, f"benchmark_sratools_results_{ts}.json")

    for d in (args.sra_dir, args.fastq_dir, args.out_dir, run_log_dir):
        os.makedirs(d, exist_ok=True)
    if args.output_json:
        os.makedirs(os.path.dirname(json_file) or ".", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    log = logging.getLogger()

    with open(args.input) as f:
        accessions = [l.strip() for l in f if l.strip()]

    log.info(f"sra-tools benchmark: {len(accessions)} accession(s) — {accessions}")
    log.info(f"  sra-dir  : {args.sra_dir}  (DISK)")
    log.info(f"  fastq-dir: {args.fastq_dir}  (NVMe)")
    log.info(f"  out-dir  : {args.out_dir}  (DISK)")
    log.info(f"  threads  : {args.threads}")
    log.info(f"  logs dir : {run_log_dir}")

    t_global_start = time.time()

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 1 — Download (prefetch → DISK)
    # ══════════════════════════════════════════════════════════════════════════
    log.info("=" * 60)
    log.info("PHASE 1: Download (prefetch)")
    log.info("=" * 60)

    t_dl_start = time.time()
    dl_details: dict = {}

    for acc in accessions:
        elapsed, ok, _ = _run([
            "prefetch",
            "--output-directory", args.sra_dir,
            "--max-size", args.prefetch_max_size,
            acc,
        ], log)
        dl_details[acc] = {"ok": ok, "elapsed_s": round(elapsed, 2)}
        log.info(f"  prefetch {acc}: {'OK' if ok else 'FAILED'} in {elapsed:.1f}s")

    t_dl_end = time.time()
    download_time = t_dl_end - t_dl_start
    log.info(f"Phase 1 complete: {download_time:.1f}s total")

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 2/3 — Serial conversion + compression per accession
    # ══════════════════════════════════════════════════════════════════════════
    log.info("=" * 60)
    log.info("PHASE 2/3: Serial fasterq-dump + pigz (no overlap)")
    log.info("=" * 60)

    required_size_factor = max(1.0, float(args.required_size_factor))
    reserve_size_factor = max(0.0, float(args.reserve_size_factor))
    disk_safety_margin_bytes = max(0, int(float(args.disk_safety_margin_gb) * (1024 ** 3)))

    t_conv_start = time.time()
    conv_details: dict = {}
    comp_details: Dict[str, Dict[str, object]] = {}
    conversion_time = 0.0
    compression_time = 0.0

    for acc in accessions:
        sra_path = _find_sra(args.sra_dir, acc)
        if sra_path is None:
            log.warning(f"  SRA file not found for {acc} — skipping conversion")
            conv_details[acc] = {"ok": False, "elapsed_s": 0, "reason": "sra_not_found"}
            continue

        try:
            sra_size = os.path.getsize(sra_path)
        except OSError as e:
            log.warning(f"  Could not stat {sra_path}; using size=0 for admission: {e}")
            sra_size = 0

        required_bytes = int(sra_size * required_size_factor) + disk_safety_margin_bytes
        reserved_bytes = int(sra_size * reserve_size_factor)

        log.info(
            f"  Admission check for {acc}: sra={_human_bytes(sra_size)}, "
            f"required={_human_bytes(required_bytes)}, reserved={_human_bytes(reserved_bytes)}"
        )
        compression_time += _wait_for_nvme_headroom(
            work_dir=args.fastq_dir,
            required_bytes=required_bytes,
            reserved_bytes=reserved_bytes,
            probing_sec=max(0.2, float(args.probing_sec)),
            log=log,
            fastq_dir=args.fastq_dir,
            out_dir=args.out_dir,
            threads=args.threads,
            comp_details=comp_details,
        )

        acc_fastq_dir = os.path.join(args.fastq_dir, acc)
        os.makedirs(acc_fastq_dir, exist_ok=True)

        elapsed, ok, _ = _run([
            "fasterq-dump",
            "--outdir",   acc_fastq_dir,
            "--temp",     acc_fastq_dir,   # keep temp I/O on NVMe
            "--threads",  str(args.threads),
            "--split-3",
            "--skip-technical",
            sra_path,
        ], log)
        conversion_time += elapsed
        conv_details[acc] = {"ok": ok, "elapsed_s": round(elapsed, 2)}
        log.info(f"  fasterq-dump {acc}: {'OK' if ok else 'FAILED'} in {elapsed:.1f}s")

        if ok:
            # Stage cleanup: remove source SRA after successful fasterq-dump.
            try:
                if os.path.exists(sra_path):
                    os.remove(sra_path)
            except OSError as e:
                log.warning(f"  Converted but could not remove SRA for {acc}: {e}")

            # Compress this accession immediately (one-by-one) so conversion and
            # compression never overlap across multiple accessions.
            acc_fastq_files = sorted(Path(acc_fastq_dir).glob("*.fastq"))
            for fq in acc_fastq_files:
                elapsed_c, _ = _compress_and_move_fastq(
                    fq=fq,
                    out_dir=args.out_dir,
                    threads=args.threads,
                    log=log,
                    comp_details=comp_details,
                )
                compression_time += elapsed_c

        # Also flush any leftover backlog one-by-one before next accession.
        flushed, elapsed_flush = _flush_fastq_backlog(
            fastq_dir=args.fastq_dir,
            out_dir=args.out_dir,
            threads=args.threads,
            log=log,
            comp_details=comp_details,
        )
        compression_time += elapsed_flush
        if flushed > 0:
            log.info(f"  Backlog flush complete after {acc}: {flushed} file(s)")

    serial_end = time.time()
    t_conv_end = serial_end
    t_comp_start = serial_end

    # Final defensive flush to leave NVMe clean.
    flushed_final, elapsed_final = _flush_fastq_backlog(
        fastq_dir=args.fastq_dir,
        out_dir=args.out_dir,
        threads=args.threads,
        log=log,
        comp_details=comp_details,
    )
    compression_time += elapsed_final
    if flushed_final > 0:
        log.info(f"  Final backlog flush complete: {flushed_final} file(s)")

    t_comp_end = serial_end
    log.info(f"Serial conversion time: {conversion_time:.1f}s")
    log.info(f"Serial compression time: {compression_time:.1f}s")

    # ══════════════════════════════════════════════════════════════════════════
    # Summary
    # ══════════════════════════════════════════════════════════════════════════
    total_time = time.time() - t_global_start

    result = {
        "tool":               "sra-tools",
        "accessions":         accessions,
        "t_start":            t_global_start,
        "t_download_end":     t_dl_end,
        "t_fasterq_end":      t_conv_end,
        "t_pigz_end":         t_comp_end,
        "t_end":              t_comp_end,
        "download_time_s":    round(download_time,    2),
        "conversion_time_s":  round(conversion_time,  2),
        "compression_time_s": round(compression_time, 2),
        "total_time_s":       round(total_time,        2),
        "details": {
            "prefetch":    dl_details,
            "fasterq_dump": conv_details,
            "pigz":        comp_details,
        },
    }

    with open(json_file, "w") as f:
        json.dump(result, f, indent=2)

    log.info(
        f"\n{'=' * 55}\n"
        f"  BENCHMARK SUMMARY (sra-tools)\n"
        f"{'=' * 55}\n"
        f"  Download   (prefetch → disk):        {download_time:>8.1f} s\n"
        f"  Conversion (fasterq-dump → NVMe):    {conversion_time:>8.1f} s\n"
        f"  Compression(pigz → disk):            {compression_time:>8.1f} s\n"
        f"  Total:                               {total_time:>8.1f} s\n"
        f"{'=' * 55}\n"
        f"  Results → {json_file}\n"
        f"  Log     → {log_file}\n"
        f"{'=' * 55}"
    )


if __name__ == "__main__":
    main()