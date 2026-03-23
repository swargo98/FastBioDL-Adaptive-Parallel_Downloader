#!/usr/bin/env python3
"""
benchmark_aria2c_kingfisher.py — Benchmark aria2c with per-accession pipeline.

Pipeline
--------
  For each accession (in order):
    1) Fetch SRA download URL(s) via NCBI efetch
    2) Download with aria2c                 -> DISK  (--sra-dir)
    3) Convert .sra -> .fastq with fasterq  -> NVMe  (--fastq-dir)
    4) Compress .fastq -> .fastq.gz with pigz -> DISK (--out-dir)

This differs from benchmark_aria2c.py, which is phase-batched as:
  download all -> convert all -> compress all.

Timing model
------------
    download_time_s    = sum of per-accession URL-fetch + download wall time
  conversion_time_s  = sum of per-accession conversion wall time
  compression_time_s = sum of per-accession compression wall time
  total_time_s       = end-to-end benchmark wall time

Requirements
------------
  pip install requests
  apt install aria2 pigz   (or equivalent)
  SRA toolkit (fasterq-dump) in PATH

Usage
-----
  python benchmark_aria2c_kingfisher.py -i accessions.txt \
      --sra-dir   aria2c/sra \
      --fastq-dir /mnt/nvme0n1/benchmark/aria2c/fastq \
      --out-dir   aria2c/output \
      --threads 8

Output
------
  logs/kingfisher/benchmark_aria2c_kingfisher_results_<timestamp>.json
  logs/kingfisher/benchmark_aria2c_kingfisher_<timestamp>.log
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


# -- URL fetching (shared with fastbiodl_upgrade.py) ---------------------------

def get_ncbi_urls(acc: str, field: str = "sra_ftp") -> List[Tuple[str, str]]:
    """Compatibility wrapper around shared NCBI lookup implementation."""
    return shared_get_ncbi_urls(
        acc,
        field=field,
        max_attempts=5,
        backoff_base=1.0,
        timeout=30,
        max_rps=2.0,
        user_agent="benchmark-aria2c-iterative/1.0 (+https://github.com/)",
        tool_name="benchmark_aria2c_kingfisher",
        email=os.environ.get("NCBI_EMAIL", ""),
        api_key=os.environ.get("NCBI_API_KEY", ""),
        logger=logging,
    )


# -- Helpers -------------------------------------------------------------------

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
        log.error(f"  COMMAND NOT FOUND: {e}  -- is aria2c / fasterq-dump / pigz in PATH?")
        return elapsed, False, str(e)


def _find_sra(sra_dir: str, acc: str) -> Optional[str]:
    """
    Locate the SRA-format file written by aria2c for the given accession.
    Follows converter.py naming semantics: bare accession, .sra, .lite.N, .N.
    Also accepts .sralite.N observed from current NCBI URLs.
    """
    candidates = [
        os.path.join(sra_dir, acc),
        os.path.join(sra_dir, acc, acc),
        os.path.join(sra_dir, f"{acc}.sra"),
        os.path.join(sra_dir, acc, f"{acc}.sra"),
        os.path.join(sra_dir, f"{acc}.sralite.1"),
        os.path.join(sra_dir, acc, f"{acc}.sralite.1"),
        os.path.join(sra_dir, f"{acc}.sralite.2"),
        os.path.join(sra_dir, acc, f"{acc}.sralite.2"),
        os.path.join(sra_dir, f"{acc}.1"),
        os.path.join(sra_dir, acc, f"{acc}.1"),
        os.path.join(sra_dir, f"{acc}.2"),
        os.path.join(sra_dir, acc, f"{acc}.2"),
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate

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


def _compress_accession_fastqs(
    acc_fastq_dir: str,
    out_dir: str,
    threads: int,
    log: logging.Logger,
) -> Tuple[float, bool, dict]:
    """Compress all .fastq files produced for one accession."""
    fastq_files = sorted(Path(acc_fastq_dir).glob("*.fastq"))
    if not fastq_files:
        return 0.0, False, {"reason": "no_fastq_files", "files": {}}

    total_elapsed = 0.0
    all_ok = True
    file_details = {}

    for fq in fastq_files:
        out_gz = os.path.join(out_dir, fq.name + ".gz")
        elapsed, ok, stderr_tail = _run([
            "pigz", "-1", "-p", str(threads), str(fq),
        ], log)
        total_elapsed += elapsed

        gz_src = str(fq) + ".gz"
        if ok and os.path.exists(gz_src):
            shutil.move(gz_src, out_gz)
            # Ensure source .gz does not linger after move completion.
            if os.path.exists(gz_src):
                os.remove(gz_src)
        else:
            all_ok = False

        # pigz usually removes input .fastq on success; enforce cleanup if it remains.
        if ok and fq.exists():
            try:
                fq.unlink()
            except OSError as e:
                all_ok = False
                log.warning(f"  Could not remove FASTQ {fq}: {e}")

        file_details[fq.name] = {
            "ok": ok and os.path.exists(out_gz),
            "elapsed_s": round(elapsed, 2),
            "stderr_tail": stderr_tail,
            "out_file": out_gz,
        }

    return total_elapsed, all_ok, {"files": file_details}


# -- Main ----------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark aria2c with iterative pipeline: "
            "download one -> convert one -> compress one"
        ),
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
                        help="Fetch fastq_ftp URLs instead of sra_ftp")
    parser.add_argument("--output-json",
                        help="Path for JSON results (default: auto-named with timestamp)")
    args = parser.parse_args()

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/kingfisher/benchmark_aria2c_kingfisher_{ts}.log"
    json_file = args.output_json or f"logs/kingfisher/benchmark_aria2c_kingfisher_results_{ts}.json"

    for directory in (args.sra_dir, args.fastq_dir, args.out_dir, "logs/kingfisher/"):
        os.makedirs(directory, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    log = logging.getLogger()

    with open(args.input, encoding="utf-8") as f:
        accessions = [line.strip() for line in f if line.strip()]

    field = "fastq_ftp" if args.fastq else "sra_ftp"
    log.info(f"aria2c iterative benchmark: {len(accessions)} accession(s) -- {accessions}")
    log.info(f"  sra-dir  : {args.sra_dir}   (DISK)")
    log.info(f"  fastq-dir: {args.fastq_dir}  (NVMe)")
    log.info(f"  out-dir  : {args.out_dir}    (DISK)")
    log.info(f"  threads  : {args.threads}")
    log.info(f"  url field: {field}")

    t_global_start = time.time()
    phase_totals = {
        "download": 0.0,
        "conversion": 0.0,
        "compression": 0.0,
    }
    details = {}

    for idx, acc in enumerate(accessions, start=1):
        log.info("=" * 70)
        log.info(f"ACCESSION {idx}/{len(accessions)}: {acc}")
        log.info("=" * 70)

        acc_record = {
            "download": {"ok": False, "elapsed_s": 0.0},
            "conversion": {"ok": False, "elapsed_s": 0.0},
            "compression": {"ok": False, "elapsed_s": 0.0},
            "ok": False,
        }

        acc_sra_dir = os.path.join(args.sra_dir, acc)
        acc_fastq_dir = os.path.join(args.fastq_dir, acc)

        shutil.rmtree(acc_sra_dir, ignore_errors=True)
        shutil.rmtree(acc_fastq_dir, ignore_errors=True)
        os.makedirs(acc_sra_dir, exist_ok=True)
        os.makedirs(acc_fastq_dir, exist_ok=True)

        # Step 1: NCBI URL fetch + aria2c download
        t_url_start = time.time()
        try:
            url_pairs = get_ncbi_urls(acc, field)
        except Exception as e:
            url_fetch_elapsed = time.time() - t_url_start
            log.error(f"  URL fetch failed for {acc}: {e}")
            acc_record["download"] = {
                "ok": False,
                "elapsed_s": round(url_fetch_elapsed, 2),
                "url_fetch_elapsed_s": round(url_fetch_elapsed, 2),
                "reason": str(e),
            }
            phase_totals["download"] += url_fetch_elapsed
            details[acc] = acc_record
            continue

        url_fetch_elapsed = time.time() - t_url_start

        if not url_pairs:
            log.warning(f"  No URLs found for {acc} -- skipping accession")
            acc_record["download"] = {
                "ok": False,
                "elapsed_s": round(url_fetch_elapsed, 2),
                "url_fetch_elapsed_s": round(url_fetch_elapsed, 2),
                "reason": "no_urls",
            }
            phase_totals["download"] += url_fetch_elapsed
            details[acc] = acc_record
            continue

        dl_elapsed_total = url_fetch_elapsed
        dl_ok = True
        for url, _ in url_pairs:
            log.info(f"  Downloading: {url}")
            elapsed, ok, stderr_tail = _run([
                "aria2c",
                "--continue=true",
                f"--split={args.threads}",
                f"--max-connection-per-server={args.threads}",
                "--min-split-size=5M",
                "--max-tries=3",
                "--retry-wait=5",
                f"--dir={acc_sra_dir}",
                url,
            ], log)
            dl_elapsed_total += elapsed
            if not ok:
                dl_ok = False
                log.error(f"  aria2c failed for {url}: {stderr_tail}")

        acc_record["download"] = {
            "ok": dl_ok,
            "elapsed_s": round(dl_elapsed_total, 2),
            "url_fetch_elapsed_s": round(url_fetch_elapsed, 2),
        }
        phase_totals["download"] += dl_elapsed_total
        if not dl_ok:
            details[acc] = acc_record
            continue

        # Step 2: fasterq-dump conversion for this accession
        sra_path = _find_sra(acc_sra_dir, acc)
        if sra_path is None:
            log.warning(f"  SRA file not found for {acc} -- skipping conversion/compression")
            acc_record["conversion"] = {
                "ok": False,
                "elapsed_s": 0.0,
                "reason": "sra_not_found",
            }
            details[acc] = acc_record
            continue

        conv_elapsed, conv_ok, conv_err = _run([
            "fasterq-dump",
            "--outdir", acc_fastq_dir,
            "--temp", acc_fastq_dir,
            "--threads", str(args.threads),
            "--split-3",
            "--skip-technical",
            sra_path,
        ], log)

        acc_record["conversion"] = {
            "ok": conv_ok,
            "elapsed_s": round(conv_elapsed, 2),
            "stderr_tail": conv_err,
        }
        phase_totals["conversion"] += conv_elapsed
        if not conv_ok:
            details[acc] = acc_record
            continue

        # Stage cleanup: remove source SRA after successful fasterq-dump.
        try:
            if os.path.exists(sra_path):
                os.remove(sra_path)
        except OSError as e:
            log.warning(f"  Converted but could not remove SRA for {acc}: {e}")

        # Step 3: pigz compression for this accession's FASTQ files
        comp_elapsed, comp_ok, comp_extra = _compress_accession_fastqs(
            acc_fastq_dir=acc_fastq_dir,
            out_dir=args.out_dir,
            threads=args.threads,
            log=log,
        )
        comp_record = {
            "ok": comp_ok,
            "elapsed_s": round(comp_elapsed, 2),
        }
        comp_record.update(comp_extra)
        acc_record["compression"] = comp_record
        phase_totals["compression"] += comp_elapsed

        acc_record["ok"] = dl_ok and conv_ok and comp_ok
        details[acc] = acc_record

        shutil.rmtree(acc_fastq_dir, ignore_errors=True)
        shutil.rmtree(acc_sra_dir, ignore_errors=True)

        log.info(
            f"  {acc} summary -- "
            f"download: {dl_elapsed_total:.1f}s, "
            f"convert: {conv_elapsed:.1f}s, "
            f"compress: {comp_elapsed:.1f}s, "
            f"status: {'OK' if acc_record['ok'] else 'FAILED'}"
        )

    t_end = time.time()
    total_time = t_end - t_global_start

    result = {
        "tool": "aria2c",
        "pipeline": "iterative_per_accession",
        "accessions": accessions,
        "t_start": t_global_start,
        "t_end": t_end,
        "download_time_s": round(phase_totals["download"], 2),
        "conversion_time_s": round(phase_totals["conversion"], 2),
        "compression_time_s": round(phase_totals["compression"], 2),
        "total_time_s": round(total_time, 2),
        "details": details,
    }

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    log.info(
        f"\n{'=' * 55}\n"
        f"  BENCHMARK SUMMARY (aria2c iterative)\n"
        f"{'=' * 55}\n"
        f"  Download   (sum per accession):      {phase_totals['download']:>8.1f} s\n"
        f"  Conversion (sum per accession):      {phase_totals['conversion']:>8.1f} s\n"
        f"  Compression(sum per accession):      {phase_totals['compression']:>8.1f} s\n"
        f"  Total wall time:                     {total_time:>8.1f} s\n"
        f"{'=' * 55}\n"
        f"  Results -> {json_file}\n"
        f"  Log     -> {log_file}\n"
        f"{'=' * 55}"
    )


if __name__ == "__main__":
    main()
