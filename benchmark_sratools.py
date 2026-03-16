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
      --fastq-dir /mnt/nvme0n1/benchmark/sratools/fastq \\
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
from pathlib import Path
from typing import List, Tuple


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
    Locate the .sra file that prefetch wrote.
    prefetch saves to  <sra_dir>/<acc>/<acc>.sra   (default NCBI layout).
    Falls back to a flat  <sra_dir>/<acc>.sra.
    """
    candidates = [
        os.path.join(sra_dir, acc, f"{acc}.sra"),
        os.path.join(sra_dir, f"{acc}.sra"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return None


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
                        default="/mnt/nvme0n1/benchmark/sratools/fastq",
                        help="Destination for fasterq-dump output (NVMe recommended for temp I/O)")
    parser.add_argument("--out-dir",
                        default="sratools/output",
                        help="Final destination for .fastq.gz (DISK)")
    parser.add_argument("--threads", type=int, default=8,
                        help="Threads for fasterq-dump and pigz")
    parser.add_argument("--prefetch-max-size", default="100G",
                        help="--max-size passed to prefetch")
    parser.add_argument("--output-json",
                        help="Path for JSON results (default: auto-named with timestamp)")
    args = parser.parse_args()

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"benchmark_sratools_{ts}.log"
    json_file = args.output_json or f"benchmark_sratools_results_{ts}.json"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    log = logging.getLogger()

    for d in (args.sra_dir, args.fastq_dir, args.out_dir):
        os.makedirs(d, exist_ok=True)

    with open(args.input) as f:
        accessions = [l.strip() for l in f if l.strip()]

    log.info(f"sra-tools benchmark: {len(accessions)} accession(s) — {accessions}")
    log.info(f"  sra-dir  : {args.sra_dir}  (DISK)")
    log.info(f"  fastq-dir: {args.fastq_dir}  (NVMe)")
    log.info(f"  out-dir  : {args.out_dir}  (DISK)")
    log.info(f"  threads  : {args.threads}")

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
    # PHASE 2 — Conversion (fasterq-dump → NVMe)
    # ══════════════════════════════════════════════════════════════════════════
    log.info("=" * 60)
    log.info("PHASE 2: Conversion (fasterq-dump)")
    log.info("=" * 60)

    t_conv_start = time.time()
    conv_details: dict = {}

    for acc in accessions:
        sra_path = _find_sra(args.sra_dir, acc)
        if sra_path is None:
            log.warning(f"  SRA file not found for {acc} — skipping conversion")
            conv_details[acc] = {"ok": False, "elapsed_s": 0, "reason": "sra_not_found"}
            continue

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
        conv_details[acc] = {"ok": ok, "elapsed_s": round(elapsed, 2)}
        log.info(f"  fasterq-dump {acc}: {'OK' if ok else 'FAILED'} in {elapsed:.1f}s")

    t_conv_end = time.time()
    conversion_time = t_conv_end - t_conv_start
    log.info(f"Phase 2 complete: {conversion_time:.1f}s total")

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 3 — Compression (pigz → DISK)
    # ══════════════════════════════════════════════════════════════════════════
    log.info("=" * 60)
    log.info("PHASE 3: Compression (pigz)")
    log.info("=" * 60)

    # Collect all .fastq files produced by fasterq-dump
    fastq_files = list(Path(args.fastq_dir).rglob("*.fastq"))
    log.info(f"  Found {len(fastq_files)} .fastq file(s) to compress")

    t_comp_start = time.time()
    comp_details: dict = {}

    for fq in fastq_files:
        out_gz = os.path.join(args.out_dir, fq.name + ".gz")
        # Compress in-place, then move .gz to out-dir.
        elapsed, ok, _ = _run([
            "pigz", "-p", str(args.threads), str(fq)
        ], log)
        gz_src = str(fq) + ".gz"
        if ok and os.path.exists(gz_src):
            # shutil.move handles cross-filesystem moves (NVMe -> DISK).
            shutil.move(gz_src, out_gz)

        comp_details[fq.name] = {"ok": ok, "elapsed_s": round(elapsed, 2)}
        log.info(f"  pigz {fq.name}: {'OK' if ok else 'FAILED'} in {elapsed:.1f}s")

    t_comp_end = time.time()
    compression_time = t_comp_end - t_comp_start
    log.info(f"Phase 3 complete: {compression_time:.1f}s total")

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