#!/usr/bin/env python3
"""
benchmark_pysradb.py -- Benchmark a pysradb-like requests downloader.

Pipeline
--------
  1. Fetch SRA download URLs via NCBI efetch (shared ncbi_lookup)
  2. Download files with requests.get using t worker threads   -> DISK (--sra-dir)
  3. Convert .sra -> .fastq with fasterq-dump                  -> NVMe (--fastq-dir)
  4. Compress .fastq -> .fastq.gz with pigz                    -> DISK (--out-dir)

Timing model
------------
  download_time    = wall time until URL lookup + ALL request downloads complete
  conversion_time  = wall time for ALL fasterq-dump calls
  compression_time = wall time for ALL pigz calls

Usage
-----
  python benchmark_pysradb.py -i accessions.txt \
      --sra-dir   pysradb/sra \
      --fastq-dir /mnt/nvme0n1/benchmark/pysradb/fastq \
      --out-dir   pysradb/output \
      --t 8 --threads 8
"""

import argparse
import datetime
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from ncbi_lookup import get_ncbi_urls as shared_get_ncbi_urls


def get_ncbi_urls(acc: str, field: str = "sra_ftp") -> List[Tuple[str, str]]:
    """Compatibility wrapper around shared NCBI lookup implementation."""
    return shared_get_ncbi_urls(
        acc,
        field=field,
        max_attempts=5,
        backoff_base=1.0,
        timeout=30,
        max_rps=2.0,
        user_agent="benchmark-pysradb/1.0 (+https://github.com/)",
        tool_name="benchmark_pysradb",
        email=os.environ.get("NCBI_EMAIL", ""),
        api_key=os.environ.get("NCBI_API_KEY", ""),
        logger=logging,
    )


def _run(cmd: List[str], log: logging.Logger, timeout: int = 7200) -> Tuple[float, bool, str]:
    """Run subprocess command and return (elapsed_seconds, success, stderr_tail)."""
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
        log.error(f"  COMMAND NOT FOUND: {e} -- is fasterq-dump / pigz in PATH?")
        return elapsed, False, str(e)


def _find_sra(sra_dir: str, acc: str) -> Optional[str]:
    """
    Locate SRA-format file for an accession.
    Supports .sra, .lite.N, .N and bare accession filenames.
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
        if os.path.isfile(c):
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


def _download_one(
    acc: str,
    url: str,
    sra_dir: str,
    timeout: int,
    chunk_size: int,
) -> Dict[str, object]:
    """Download one URL with requests.get and stream to disk."""
    t0 = time.time()
    filename = os.path.basename(url.split("?", 1)[0]) or acc
    out_dir = os.path.join(sra_dir, acc)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, filename)

    bytes_written = 0
    try:
        with requests.get(url, stream=True, timeout=timeout) as resp:
            resp.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    bytes_written += len(chunk)
        elapsed = time.time() - t0
        return {
            "acc": acc,
            "url": url,
            "path": out_path,
            "ok": True,
            "elapsed_s": round(elapsed, 2),
            "bytes": bytes_written,
            "reason": "",
        }
    except Exception as e:
        elapsed = time.time() - t0
        return {
            "acc": acc,
            "url": url,
            "path": out_path,
            "ok": False,
            "elapsed_s": round(elapsed, 2),
            "bytes": bytes_written,
            "reason": str(e),
        }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark pysradb-like downloader: URL lookup -> requests.get download "
            "(threaded) -> fasterq-dump -> pigz"
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-i", "--input", required=True,
                        help="Text file with one SRA accession per line")
    parser.add_argument("--sra-dir", default="pysradb/sra",
                        help="Destination for downloaded SRA files (DISK)")
    parser.add_argument("--fastq-dir", default="/mnt/nvme0n1/benchmark/pysradb/fastq",
                        help="Destination for fasterq-dump output (NVMe recommended)")
    parser.add_argument("--out-dir", default="pysradb/output",
                        help="Final destination for .fastq.gz files (DISK)")
    parser.add_argument("--t", type=int, default=8,
                        help="Concurrent download threads for requests.get")
    parser.add_argument("--threads", type=int, default=8,
                        help="Thread count for fasterq-dump and pigz")
    parser.add_argument("--lookup-workers", type=int, default=3,
                        help="Worker count for parallel accession URL lookups")
    parser.add_argument("--request-timeout", type=int, default=1200,
                        help="Per-request timeout in seconds")
    parser.add_argument("--chunk-size", type=int, default=4 * 1024 * 1024,
                        help="Streaming chunk size for requests.get (bytes)")
    parser.add_argument("--fastq", action="store_true",
                        help="Fetch fastq_ftp URLs instead of sra_ftp")
    parser.add_argument("--output-json",
                        help="Path for JSON results (default: auto-named with timestamp)")
    args = parser.parse_args()

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/pysradb/benchmark_pysradb_{ts}.log"
    json_file = args.output_json or f"logs/pysradb/benchmark_pysradb_results_{ts}.json"

    for d in (args.sra_dir, args.fastq_dir, args.out_dir, "logs/pysradb/"):
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

    with open(args.input, encoding="utf-8") as f:
        accessions = [line.strip() for line in f if line.strip()]

    field = "fastq_ftp" if args.fastq else "sra_ftp"
    lookup_workers = max(1, min(len(accessions), args.lookup_workers))
    download_workers = max(1, args.t)

    log.info(f"pysradb benchmark: {len(accessions)} accession(s) -- {accessions}")
    log.info(f"  sra-dir        : {args.sra_dir}   (DISK)")
    log.info(f"  fastq-dir      : {args.fastq_dir}  (NVMe)")
    log.info(f"  out-dir        : {args.out_dir}    (DISK)")
    log.info(f"  lookup-workers : {lookup_workers}")
    log.info(f"  download t     : {download_workers}")
    log.info(f"  conv/pigz thr  : {args.threads}")
    log.info(f"  url field      : {field}")

    t_global_start = time.time()

    # Phase 1: parallel URL lookup + requests downloads.
    log.info("=" * 60)
    log.info("PHASE 1: Fetch URLs + Download with requests.get")
    log.info("=" * 60)

    t_dl_start = time.time()
    dl_details: Dict[str, Dict[str, object]] = {
        acc: {"ok": False, "elapsed_s": 0.0, "files": [], "urls_found": 0}
        for acc in accessions
    }

    def _fetch(acc: str) -> Tuple[str, List[Tuple[str, str]], str]:
        try:
            return acc, get_ncbi_urls(acc, field), ""
        except Exception as e:
            return acc, [], str(e)

    all_download_jobs: List[Tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=lookup_workers) as pool:
        futures = {pool.submit(_fetch, acc): acc for acc in accessions}
        for fut in as_completed(futures):
            acc, url_pairs, err = fut.result()
            if err:
                log.error(f"  URL fetch failed for {acc}: {err}")
                dl_details[acc] = {
                    "ok": False,
                    "elapsed_s": 0.0,
                    "files": [],
                    "urls_found": 0,
                    "reason": err,
                }
                continue
            if not url_pairs:
                log.warning(f"  No URLs found for {acc} -- skipping")
                dl_details[acc] = {
                    "ok": False,
                    "elapsed_s": 0.0,
                    "files": [],
                    "urls_found": 0,
                    "reason": "no_urls",
                }
                continue

            dl_details[acc]["urls_found"] = len(url_pairs)
            for url, _ in url_pairs:
                all_download_jobs.append((acc, url))

    log.info(f"  Total files queued for download: {len(all_download_jobs)}")

    if all_download_jobs:
        with ThreadPoolExecutor(max_workers=download_workers) as pool:
            futures = {
                pool.submit(
                    _download_one,
                    acc,
                    url,
                    args.sra_dir,
                    args.request_timeout,
                    args.chunk_size,
                ): (acc, url)
                for acc, url in all_download_jobs
            }
            for fut in as_completed(futures):
                result = fut.result()
                acc = str(result["acc"])
                file_record = {
                    "url": result["url"],
                    "path": result["path"],
                    "ok": result["ok"],
                    "elapsed_s": result["elapsed_s"],
                    "bytes": result["bytes"],
                    "reason": result["reason"],
                }
                dl_details[acc]["files"].append(file_record)

                if result["ok"]:
                    log.info(
                        f"  Downloaded {os.path.basename(str(result['path']))} "
                        f"for {acc} in {result['elapsed_s']:.1f}s"
                    )
                else:
                    log.error(
                        f"  Download failed for {acc}: {result['url']} -- {result['reason']}"
                    )

    for acc in accessions:
        file_entries = dl_details[acc].get("files", [])
        if not file_entries:
            dl_details[acc]["ok"] = False
            dl_details[acc]["elapsed_s"] = 0.0
            continue
        acc_ok = all(bool(x.get("ok")) for x in file_entries)
        acc_elapsed = sum(float(x.get("elapsed_s", 0.0)) for x in file_entries)
        dl_details[acc]["ok"] = acc_ok
        dl_details[acc]["elapsed_s"] = round(acc_elapsed, 2)

    t_dl_end = time.time()
    download_time = t_dl_end - t_dl_start
    log.info(f"Phase 1 complete: {download_time:.1f}s total")

    # Phase 2: conversion with fasterq-dump.
    log.info("=" * 60)
    log.info("PHASE 2: Convert SRA -> FASTQ (fasterq-dump)")
    log.info("=" * 60)

    t_conv_start = time.time()
    conv_details: Dict[str, Dict[str, object]] = {}

    for acc in accessions:
        sra_path = _find_sra(args.sra_dir, acc)
        if sra_path is None:
            log.warning(f"  SRA file not found for {acc} -- skipping conversion")
            conv_details[acc] = {"ok": False, "elapsed_s": 0.0, "reason": "sra_not_found"}
            continue

        acc_fastq_dir = os.path.join(args.fastq_dir, acc)
        os.makedirs(acc_fastq_dir, exist_ok=True)

        elapsed, ok, stderr_tail = _run([
            "fasterq-dump",
            "--outdir", acc_fastq_dir,
            "--temp", acc_fastq_dir,
            "--threads", str(args.threads),
            "--split-3",
            "--skip-technical",
            sra_path,
        ], log)

        conv_details[acc] = {
            "ok": ok,
            "elapsed_s": round(elapsed, 2),
            "stderr_tail": stderr_tail,
        }
        log.info(f"  fasterq-dump {acc}: {'OK' if ok else 'FAILED'} in {elapsed:.1f}s")

    t_conv_end = time.time()
    conversion_time = t_conv_end - t_conv_start
    log.info(f"Phase 2 complete: {conversion_time:.1f}s total")

    # Phase 3: compress fastq files with pigz.
    log.info("=" * 60)
    log.info("PHASE 3: Compression (pigz)")
    log.info("=" * 60)

    fastq_files = list(Path(args.fastq_dir).rglob("*.fastq"))
    log.info(f"  Found {len(fastq_files)} .fastq file(s) to compress")

    t_comp_start = time.time()
    comp_details: Dict[str, Dict[str, object]] = {}

    for fq in fastq_files:
        out_gz = os.path.join(args.out_dir, fq.name + ".gz")
        elapsed, ok, stderr_tail = _run([
            "pigz", "-p", str(args.threads), str(fq),
        ], log)

        gz_src = str(fq) + ".gz"
        if ok and os.path.exists(gz_src):
            shutil.move(gz_src, out_gz)

        comp_details[fq.name] = {
            "ok": ok,
            "elapsed_s": round(elapsed, 2),
            "stderr_tail": stderr_tail,
            "out_file": out_gz,
        }
        log.info(f"  pigz {fq.name}: {'OK' if ok else 'FAILED'} in {elapsed:.1f}s")

    t_comp_end = time.time()
    compression_time = t_comp_end - t_comp_start
    log.info(f"Phase 3 complete: {compression_time:.1f}s total")

    total_time = time.time() - t_global_start

    result = {
        "tool": "pysradb_requests",
        "accessions": accessions,
        "t_start": t_global_start,
        "t_download_end": t_dl_end,
        "t_fasterq_end": t_conv_end,
        "t_pigz_end": t_comp_end,
        "t_end": t_comp_end,
        "download_time_s": round(download_time, 2),
        "conversion_time_s": round(conversion_time, 2),
        "compression_time_s": round(compression_time, 2),
        "total_time_s": round(total_time, 2),
        "details": {
            "requests_download": dl_details,
            "fasterq_dump": conv_details,
            "pigz": comp_details,
        },
    }

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    log.info(
        f"\n{'=' * 58}\n"
        f"  BENCHMARK SUMMARY (pysradb-like requests)\n"
        f"{'=' * 58}\n"
        f"  Download   (requests -> disk):        {download_time:>8.1f} s\n"
        f"  Conversion (fasterq-dump -> NVMe):   {conversion_time:>8.1f} s\n"
        f"  Compression(pigz -> disk):           {compression_time:>8.1f} s\n"
        f"  Total:                               {total_time:>8.1f} s\n"
        f"{'=' * 58}\n"
        f"  Results -> {json_file}\n"
        f"  Log     -> {log_file}\n"
        f"{'=' * 58}"
    )


if __name__ == "__main__":
    main()
