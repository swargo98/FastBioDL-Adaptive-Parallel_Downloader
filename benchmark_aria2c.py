#!/usr/bin/env python3
"""
benchmark_aria2c.py — Benchmark the aria2c download pipeline.

Pipeline
--------
  1. Fetch SRA download URLs via NCBI efetch (same logic as fastbiodl_upgrade.py)
  2. Download .sra files with aria2c          → DISK  (--sra-dir)
  3. Convert .sra → .fastq with fasterq-dump  → NVMe  (--fastq-dir)
  4. Compress .fastq → .fastq.gz with pigz    → DISK  (--out-dir)

Timing model (matches fastbiodl benchmark)
------------------------------------------
  download_time    = wall time until ALL aria2c calls complete
  conversion_time  = wall time for ALL fasterq-dump calls
  compression_time = wall time for ALL pigz calls

Requirements
------------
  pip install requests
  apt install aria2 pigz   (or equivalent)
  SRA toolkit (fasterq-dump) in PATH

Usage
-----
  python benchmark_aria2c.py -i accessions.txt \\
      --sra-dir   aria2c/sra \\
      --fastq-dir /mnt/nvme0n1/benchmark/aria2c/fastq \\
      --out-dir   aria2c/output \\
      --threads 8

Output
------
  benchmark_aria2c_results_<timestamp>.json
  benchmark_aria2c_<timestamp>.log
"""

import os
import sys
import time
import json
import logging
import argparse
import datetime
import shutil
import subprocess
import re
from pathlib import Path
from typing import List, Tuple, Optional

from ncbi_lookup import get_ncbi_urls as shared_get_ncbi_urls


# ── URL fetching (shared with fastbiodl_upgrade.py) ──────────────────────────

def get_ncbi_urls(acc: str, field: str = "sra_ftp") -> List[Tuple[str, str]]:
    """Compatibility wrapper around shared NCBI lookup implementation."""
    return shared_get_ncbi_urls(
        acc,
        field=field,
        max_attempts=5,
        backoff_base=1.0,
        timeout=30,
        max_rps=2.0,
        user_agent="benchmark-aria2c/1.0 (+https://github.com/)",
        tool_name="benchmark_aria2c",
        email=os.environ.get("NCBI_EMAIL", ""),
        api_key=os.environ.get("NCBI_API_KEY", ""),
        logger=logging,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run(cmd: List[str], log: logging.Logger, timeout: int = 7200) -> Tuple[float, bool, str]:
    """Run a subprocess, return (elapsed_seconds, success, stderr_tail)."""
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
        log.error(f"  COMMAND NOT FOUND: {e}  — is aria2c / fasterq-dump / pigz in PATH?")
        return elapsed, False, str(e)


def _find_sra(sra_dir: str, acc: str) -> Optional[str]:
    """
    Locate the SRA-format file written by aria2c for the given accession.
    Covers the same naming variants accepted by converter.py:
    .sra, .lite.N, .N, and bare accession filenames.
    """
    candidates = [
        os.path.join(sra_dir, acc),
        os.path.join(sra_dir, acc, acc),
        os.path.join(sra_dir, f"{acc}.sra"),
        os.path.join(sra_dir, acc, f"{acc}.sra"),
        os.path.join(sra_dir, f"{acc}.1"),
        os.path.join(sra_dir, acc, f"{acc}.1"),
        os.path.join(sra_dir, f"{acc}.2"),
        os.path.join(sra_dir, acc, f"{acc}.2"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c

    sra_file_re = re.compile(
        rf"(?:^|/){re.escape(acc)}(?:\.sra|\.lite\.\d+|\.\d+)?$",
        re.IGNORECASE,
    )
    matches = []
    for path in Path(sra_dir).glob(f"**/{acc}*"):
        if path.is_file() and sra_file_re.search(str(path)):
            matches.append(path)
    if matches:
        return str(sorted(matches)[0])
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark aria2c: fetch URL → download (aria2c) → fasterq-dump → pigz",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-i", "--input", required=True,
                        help="Text file with one SRA accession per line")
    parser.add_argument("--sra-dir", default="aria2c/sra",
                        help="Destination for aria2c .sra downloads (DISK)")
    parser.add_argument("--fastq-dir", default="/mnt/nvme0n1/benchmark/aria2c/fastq",
                        help="Destination for fasterq-dump output (NVMe recommended)")
    parser.add_argument("--out-dir", default="aria2c/output",
                        help="Final destination for .fastq.gz files (DISK)")
    parser.add_argument("--threads", type=int, default=8,
                        help="Thread / connection count for aria2c, fasterq-dump, and pigz")
    parser.add_argument("--fastq", action="store_true",
                        help="Fetch fastq_ftp URLs instead of sra_ftp (downloads .fastq.gz directly)")
    parser.add_argument("--output-json",
                        help="Path for JSON results (default: auto-named with timestamp)")
    args = parser.parse_args()

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/aria2c/benchmark_aria2c_{ts}.log"
    json_file = args.output_json or f"logs/aria2c/benchmark_aria2c_results_{ts}.json"

    for d in (args.sra_dir, args.fastq_dir, args.out_dir, "logs/aria2c/"):
        os.makedirs(d, exist_ok=True)

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

    field = "fastq_ftp" if args.fastq else "sra_ftp"
    log.info(f"aria2c benchmark: {len(accessions)} accession(s) — {accessions}")
    log.info(f"  sra-dir  : {args.sra_dir}   (DISK)")
    log.info(f"  fastq-dir: {args.fastq_dir}  (NVMe)")
    log.info(f"  out-dir  : {args.out_dir}    (DISK)")
    log.info(f"  threads  : {args.threads}")
    log.info(f"  url field: {field}")

    t_global_start = time.time()

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 1 — Fetch URLs then download with aria2c (→ DISK)
    # ══════════════════════════════════════════════════════════════════════════
    log.info("=" * 60)
    log.info("PHASE 1: Fetch URLs + Download with aria2c")
    log.info("=" * 60)

    t_dl_start = time.time()
    dl_details: dict = {}

    for acc in accessions:
        # --- fetch URL(s) from NCBI ---
        try:
            url_pairs = get_ncbi_urls(acc, field)
        except Exception as e:
            log.error(f"  URL fetch failed for {acc}: {e}")
            dl_details[acc] = {"ok": False, "elapsed_s": 0.0, "reason": str(e)}
            continue

        if not url_pairs:
            log.warning(f"  No URLs found for {acc} — skipping")
            dl_details[acc] = {"ok": False, "elapsed_s": 0.0, "reason": "no_urls"}
            continue

        log.info(f"  {acc}: {len(url_pairs)} URL(s) found")
        acc_ok = True
        acc_elapsed = 0.0

        for url, _ in url_pairs:
            log.info(f"  Downloading: {url}")
            elapsed, ok, _ = _run([
                "aria2c",
                "--continue=true",
                f"--split={args.threads}",
                f"--max-connection-per-server={args.threads}",
                "--min-split-size=5M",
                f"--max-tries=3",
                "--retry-wait=5",
                f"--dir={args.sra_dir}",
                url,
            ], log)
            acc_elapsed += elapsed
            if not ok:
                acc_ok = False
                log.error(f"  aria2c failed for {url}")

        dl_details[acc] = {"ok": acc_ok, "elapsed_s": round(acc_elapsed, 2)}
        log.info(f"  Download {acc}: {'OK' if acc_ok else 'FAILED'} in {acc_elapsed:.1f}s")

    t_dl_end = time.time()
    download_time = t_dl_end - t_dl_start
    log.info(f"Phase 1 complete: {download_time:.1f}s total")

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 2 — Convert SRA → FASTQ with fasterq-dump (→ NVMe)
    # ══════════════════════════════════════════════════════════════════════════
    log.info("=" * 60)
    log.info("PHASE 2: Convert SRA → FASTQ (fasterq-dump)")
    log.info("=" * 60)

    t_conv_start = time.time()
    conv_details: dict = {}

    for acc in accessions:
        sra_path = _find_sra(args.sra_dir, acc)
        if sra_path is None:
            log.warning(f"  SRA file not found for {acc} — skipping conversion")
            conv_details[acc] = {"ok": False, "elapsed_s": 0.0, "reason": "sra_not_found"}
            continue

        acc_fastq_dir = os.path.join(args.fastq_dir, acc)
        os.makedirs(acc_fastq_dir, exist_ok=True)

        elapsed, ok, _ = _run([
            "fasterq-dump",
            "--outdir",  acc_fastq_dir,
            "--temp",    acc_fastq_dir,
            "--threads", str(args.threads),
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
    # PHASE 3 — Compression: pigz .fastq → .fastq.gz (→ DISK)
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
            "pigz", "-p", str(args.threads), str(fq),
        ], log)
        gz_src = str(fq) + ".gz"
        if ok and os.path.exists(gz_src):
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
        "tool":               "aria2c",
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
            "aria2c":       dl_details,
            "fasterq_dump": conv_details,
            "pigz":         comp_details,
        },
    }

    with open(json_file, "w") as f:
        json.dump(result, f, indent=2)

    log.info(
        f"\n{'=' * 55}\n"
        f"  BENCHMARK SUMMARY (aria2c)\n"
        f"{'=' * 55}\n"
        f"  Download   (aria2c → disk):          {download_time:>8.1f} s\n"
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
