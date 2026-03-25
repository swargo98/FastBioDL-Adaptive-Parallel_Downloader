#!/usr/bin/env python3
"""
benchmark_kingfisher.py — Benchmark the Kingfisher pipeline.

Pipeline
--------
  kingfisher get   → download .sra to DISK  (--sra-dir)
  kingfisher convert → .fastq to NVMe       (--fastq-dir, kingfisher's default)
  pigz             → .fastq.gz to DISK      (--out-dir)

Timing model (matches fastbiodl benchmark)
------------------------------------------
    download_time    = wall time for URL fetch + ALL kingfisher get calls
  conversion_time  = wall time for ALL kingfisher convert calls
  compression_time = wall time for ALL pigz calls

Kingfisher reference
--------------------
  https://github.com/wwood/kingfisher-download

  Required kingfisher version: ≥ 0.3 (supports `convert` subcommand)

  Install:
      conda install -c bioconda kingfisher   # or
      pip install kingfisher

  This script uses two subcommands:
      kingfisher get -r <ACC> \\
          --download-threads <N> \\
          -m prefetch \\
          --output-format sra \\
          --output-directory <sra-dir>

      kingfisher convert \\
          --sra-file <FILE.sra> \\
          --output-format fastq \\
          --output-directory <fastq-dir> \\
          --threads <N>

  NOTE: If kingfisher's `convert` subcommand CLI differs on your installed
  version, set --use-fasterq to fall back to calling fasterq-dump directly.

Usage
-----
  python benchmark_kingfisher.py -i accessions.txt \\
      --sra-dir   kingfisher/sra \\
      --fastq-dir /mnt/raid0/benchmark/kingfisher/fastq \\
      --out-dir   kingfisher/output \\
      --threads 8

Output
------
  benchmark_kingfisher_results_<timestamp>.json
  benchmark_kingfisher_<timestamp>.log
"""

import os
import sys
import time
import json
import logging
import argparse
import datetime
import subprocess
from pathlib import Path
from typing import List, Tuple

from ncbi_lookup import get_ncbi_urls as shared_get_ncbi_urls


# ── Helpers ──────────────────────────────────────────────────────────────────

def _run(cmd: List[str], log: logging.Logger, timeout: int = 7200) -> Tuple[float, bool, str]:
    """Run a command, return (elapsed_seconds, success, stderr_tail)."""
    log.info(f"$ {' '.join(cmd)}")
    t0 = time.time()
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
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
        log.error(f"  COMMAND NOT FOUND: {e}  — is kingfisher / fasterq-dump / pigz in PATH?")
        return elapsed, False, str(e)


def _find_sra(sra_dir: str, acc: str) -> str | None:
    """
    Locate the .sra written by kingfisher get.
    Kingfisher typically writes <acc>.sra in the output directory directly.
    Falls back to NCBI subdirectory layout.
    """
    candidates = [
        os.path.join(sra_dir, f"{acc}.sra"),
        os.path.join(sra_dir, acc, f"{acc}.sra"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    # Broader glob for .sra files matching the accession
    matches = list(Path(sra_dir).glob(f"**/{acc}*.sra"))
    return str(matches[0]) if matches else None


def _convert_with_fasterq(sra_path: str, fastq_dir: str, threads: int,
                           log: logging.Logger) -> Tuple[float, bool]:
    """Fallback: run fasterq-dump directly if kingfisher convert is unavailable."""
    acc = Path(sra_path).stem
    acc_dir = os.path.join(fastq_dir, acc)
    os.makedirs(acc_dir, exist_ok=True)
    elapsed, ok, _ = _run([
        "fasterq-dump",
        "--outdir",  acc_dir,
        "--temp",    acc_dir,
        "--threads", str(threads),
        "--split-3",
        "--skip-technical",
        sra_path,
    ], log)
    return elapsed, ok


def get_ncbi_urls(acc: str, field: str = "sra_ftp") -> List[Tuple[str, str]]:
    """Compatibility wrapper around shared NCBI lookup implementation."""
    return shared_get_ncbi_urls(
        acc,
        field=field,
        max_attempts=5,
        backoff_base=1.0,
        timeout=30,
        max_rps=2.0,
        user_agent="benchmark-kingfisher/1.0 (+https://github.com/)",
        tool_name="benchmark_kingfisher",
        email=os.environ.get("NCBI_EMAIL", ""),
        api_key=os.environ.get("NCBI_API_KEY", ""),
        logger=logging,
    )


def _accession_group_name(input_path: str) -> str:
    """Derive a safe folder name from the accession input filename."""
    raw_name = Path(input_path).stem or "accessions"
    safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in raw_name)
    safe_name = safe_name.strip("._-")
    return safe_name or "accessions"


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Kingfisher: get (sra) → convert → pigz",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-i", "--input", required=True,
                        help="Text file with one SRA accession per line")
    parser.add_argument("--sra-dir",
                        default="kingfisher/sra",
                        help="Destination for kingfisher get output (DISK)")
    parser.add_argument("--fastq-dir",
                        default="/mnt/raid0/benchmark/kingfisher/fastq",
                        help="Destination for converted .fastq files (NVMe recommended)")
    parser.add_argument("--out-dir",
                        default="kingfisher/output",
                        help="Final destination for .fastq.gz (DISK)")
    parser.add_argument("--threads", type=int, default=8,
                        help="Download, conversion, and pigz thread count")
    parser.add_argument("--download-method", default="prefetch",
                        choices=["prefetch", "aws-http", "ena-ftp"],
                        help="Kingfisher download method (-m flag)")
    parser.add_argument("--use-fasterq", action="store_true",
                        help="Use fasterq-dump directly for conversion instead of kingfisher convert")
    parser.add_argument("--output-json",
                        help="Path for JSON results (default: auto-named with timestamp)")
    args = parser.parse_args()

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    accession_group = _accession_group_name(args.input)
    run_log_dir = os.path.join("logs", "kingfisher", accession_group)
    log_file = os.path.join(run_log_dir, f"benchmark_kingfisher_{ts}.log")
    json_file = args.output_json or os.path.join(run_log_dir, f"benchmark_kingfisher_results_{ts}.json")

    os.makedirs(run_log_dir, exist_ok=True)
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

    for d in (args.sra_dir, args.fastq_dir, args.out_dir):
        os.makedirs(d, exist_ok=True)

    with open(args.input) as f:
        accessions = [l.strip() for l in f if l.strip()]

    log.info(f"Kingfisher benchmark: {len(accessions)} accession(s) — {accessions}")
    log.info(f"  sra-dir  : {args.sra_dir}   (DISK)")
    log.info(f"  fastq-dir: {args.fastq_dir}  (NVMe)")
    log.info(f"  out-dir  : {args.out_dir}    (DISK)")
    log.info(f"  threads  : {args.threads}")
    log.info(f"  dl method: {args.download_method}")
    log.info(f"  converter: {'fasterq-dump (fallback)' if args.use_fasterq else 'kingfisher convert'}")
    log.info(f"  logs dir : {run_log_dir}")

    t_global_start = time.time()

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 1 — URL fetch + Download SRA (kingfisher get → DISK)
    # ══════════════════════════════════════════════════════════════════════════
    log.info("=" * 60)
    log.info("PHASE 1: URL fetch + Download SRA (kingfisher get --output-format sra)")
    log.info("=" * 60)

    t_dl_start = time.time()
    dl_details: dict = {}

    for acc in accessions:
        # Include NCBI URL lookup wall time in download phase for parity with other benchmarks.
        t_url_start = time.time()
        url_pairs: List[Tuple[str, str]] = []
        url_fetch_ok = True
        url_fetch_error = ""
        try:
            url_pairs = get_ncbi_urls(acc, "sra_ftp")
        except Exception as exc:
            url_fetch_ok = False
            url_fetch_error = str(exc)
            log.warning(f"  URL fetch failed for {acc}: {exc}")
        url_fetch_elapsed = time.time() - t_url_start

        # kingfisher get saves .sra to the output-directory
        elapsed, ok, _ = _run([
            "kingfisher", "get",
            "-r", acc,
            # "--download-threads", str(args.threads),
            "-m", "aws-http" , "aws-cp"
            # "--output-format", "sra",
            # "--output-directory", args.sra_dir,
            # "--force",   # overwrite if exists (idempotent re-runs)
        ], log)
        dl_details[acc] = {
            "ok": ok,
            "elapsed_s": round(elapsed, 2),
            "url_fetch_ok": url_fetch_ok,
            "url_fetch_elapsed_s": round(url_fetch_elapsed, 2),
            "url_count": len(url_pairs),
        }
        if url_fetch_error:
            dl_details[acc]["url_fetch_error"] = url_fetch_error
        log.info(f"  kingfisher get {acc}: {'OK' if ok else 'FAILED'} in {elapsed:.1f}s")

    t_dl_end = time.time()
    download_time = t_dl_end - t_dl_start
    log.info(f"Phase 1 complete: {download_time:.1f}s total")

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 2 — Convert to FASTQ (kingfisher convert or fasterq-dump → NVMe)
    # ══════════════════════════════════════════════════════════════════════════
    log.info("=" * 60)
    log.info("PHASE 2: Convert SRA → FASTQ")
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

        if args.use_fasterq:
            elapsed, ok = _convert_with_fasterq(sra_path, args.fastq_dir, args.threads, log)
        else:
            # kingfisher convert subcommand
            # NOTE: --output-directory support depends on kingfisher version.
            # If this fails, re-run with --use-fasterq.
            elapsed, ok, stderr = _run([
                "kingfisher", "convert",
                "--sra-file", sra_path,
                "--output-format", "fastq",
                "--output-directory", acc_fastq_dir,
                "--threads", str(args.threads),
            ], log)
            if not ok and "unrecognized" in stderr.lower():
                log.warning(
                    "  kingfisher convert --output-directory not supported on this version; "
                    "falling back to fasterq-dump. Use --use-fasterq to suppress this warning."
                )
                elapsed, ok = _convert_with_fasterq(sra_path, args.fastq_dir, args.threads, log)

        conv_details[acc] = {"ok": ok, "elapsed_s": round(elapsed, 2)}
        log.info(f"  convert {acc}: {'OK' if ok else 'FAILED'} in {elapsed:.1f}s")

    t_conv_end = time.time()
    conversion_time = t_conv_end - t_conv_start
    log.info(f"Phase 2 complete: {conversion_time:.1f}s total")

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 3 — Compression (pigz → DISK)
    # ══════════════════════════════════════════════════════════════════════════
    log.info("=" * 60)
    log.info("PHASE 3: Compression (pigz)")
    log.info("=" * 60)

    fastq_files = list(Path(args.fastq_dir).rglob("*.fastq"))
    log.info(f"  Found {len(fastq_files)} .fastq file(s) to compress")

    t_comp_start = time.time()
    comp_details: dict = {}

    for fq in fastq_files:
        out_gz = os.path.join(args.out_dir, fq.name + ".gz")
        elapsed, ok, _ = _run([
            "pigz", "-p", str(args.threads), str(fq)
        ], log)
        gz_src = str(fq) + ".gz"
        if ok and os.path.exists(gz_src):
            os.replace(gz_src, out_gz)
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
        "tool":               "kingfisher",
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
            "kingfisher_get":     dl_details,
            "kingfisher_convert": conv_details,
            "pigz":               comp_details,
        },
    }

    with open(json_file, "w") as f:
        json.dump(result, f, indent=2)

    log.info(
        f"\n{'=' * 55}\n"
        f"  BENCHMARK SUMMARY (kingfisher)\n"
        f"{'=' * 55}\n"
        f"  Download   (kingfisher get → disk):  {download_time:>8.1f} s\n"
        f"  Conversion (kingfisher convert/fasterq-dump): {conversion_time:>8.1f} s\n"
        f"  Compression(pigz → disk):            {compression_time:>8.1f} s\n"
        f"  Total:                               {total_time:>8.1f} s\n"
        f"{'=' * 55}\n"
        f"  Results → {json_file}\n"
        f"  Log     → {log_file}\n"
        f"{'=' * 55}"
    )


if __name__ == "__main__":
    main()