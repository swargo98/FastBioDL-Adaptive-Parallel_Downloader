#!/usr/bin/env python3
"""
download_sra_ena.py

A drop‑in replacement (or fallback) for prefetch: 
given an accession list, query ENA’s API for the FTP URLs and download them.
"""

import argparse
import subprocess
import requests
import os
import sys
from typing import List
import time

ENA_API = "https://www.ebi.ac.uk/ena/portal/api/filereport"

def get_ena_urls(acc: str, field: str = "sra_ftp") -> List[str]:
    """
    Query ENA for the given field (sra_ftp or fastq_ftp) for an accession.
    Returns a list of FTP URLs.
    """
    params = {
        "accession":   acc,
        "result":      "read_run",
        "fields":      field,
        "download":    "true"
    }
    r = requests.get(ENA_API, params=params, timeout=30)
    r.raise_for_status()
    lines = r.text.strip().splitlines()
    if len(lines) < 2:
        return []
    # last line is the data; columns are tab‑delimited
    url_field = lines[-1].split("\t")[1]
    return url_field.split(";")

def download_urls(urls: List[str], outdir: str):
    """
    For each FTP URL, invoke wget into the output directory.
    """
    for url in urls:
        print(f"[ENA] Downloading {url}")
        ret = subprocess.run(
            ["wget", "-nv", "-P", outdir, url],
            capture_output=True, text=True
        )
        if ret.returncode != 0:
            print(f"[ERR] wget failed for {url}:\n{ret.stderr}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(
        description="Download SRA (or FASTQ) via ENA mirror"
    )
    parser.add_argument("-i","--input", required=True,
                        help="Text file: one accession per line.")
    parser.add_argument("-o","--outdir", default=".",
                        help="Where to save downloads.")
    parser.add_argument("--fastq", action="store_true",
                        help="Download split FASTQ (fastq_ftp) instead of .sra")
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        sys.exit(f"Input file not found: {args.input}")
    os.makedirs(args.outdir, exist_ok=True)

    field = "fastq_ftp" if args.fastq else "sra_ftp"
    with open(args.input) as f:
        accs = [l.strip() for l in f if l.strip()]

    for acc in accs:
        try:
            urls = get_ena_urls(acc, field=field)
        except Exception as e:
            print(f"[ERR] ENA query failed for {acc}: {e}", file=sys.stderr)
            continue
        if not urls or urls[0] == "":
            print(f"[WARN] no ENA URLs for {acc}", file=sys.stderr)
            continue
        start = time.time()
        download_urls(urls, args.outdir)
        end = time.time()
        elapsed = end - start
        print(f"[INFO] Downloaded {acc} in {elapsed:.2f} seconds")

if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    elapsed = end - start
    print(f"[INFO] Finished in {elapsed:.2f} seconds")
