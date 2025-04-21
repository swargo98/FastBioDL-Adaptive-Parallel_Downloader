# #!/usr/bin/env python3
# """
# download_sra.py

# A script to download SRA files from NCBI given a text file of accession numbers.
# Prerequisites:
#   - Install SRA Toolkit (https://github.com/ncbi/sra-tools)
#   - Ensure `prefetch` is in your PATH

# Usage:
#   python download_sra.py -i accessions.txt -o /path/to/output_dir
#   python3 download_sra.py -i accessions.txt -o dest/
# """
# import argparse
# import subprocess
# import os
# import sys

# def download_sra(accession, output_dir):
#     """Download a single SRA accession using prefetch."""
#     try:
#         cmd = ["prefetch", accession, "--output-directory", output_dir]
#         result = subprocess.run(cmd, capture_output=True, text=True)
#         if result.returncode == 0:
#             print(f"[OK]   Downloaded {accession}")
#         else:
#             print(f"[ERR]  Failed {accession}: {result.stderr.strip()}")
#     except FileNotFoundError:
#         print("Error: prefetch command not found. Please install SRA Toolkit and add it to your PATH.")
#         sys.exit(1)


# def main():
#     parser = argparse.ArgumentParser(description="Download SRA files listed in a text file.")
#     parser.add_argument("-i", "--input", required=True,
#                         help="Path to text file with one SRA accession per line.")
#     parser.add_argument("-o", "--outdir", default=".",
#                         help="Directory to save downloaded SRA files.")
#     args = parser.parse_args()

#     if not os.path.isfile(args.input):
#         print(f"Error: Input file '{args.input}' not found.")
#         sys.exit(1)

#     os.makedirs(args.outdir, exist_ok=True)

#     with open(args.input) as f:
#         accessions = [line.strip() for line in f if line.strip()]

#     if not accessions:
#         print("No accessions found in input file.")
#         sys.exit(1)

#     for acc in accessions:
#         download_sra(acc, args.outdir)


# if __name__ == "__main__":
#     main()

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
        download_urls(urls, args.outdir)

if __name__ == "__main__":
    main()
