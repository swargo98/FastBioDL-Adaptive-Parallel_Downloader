# ─── sra_fastq_converter.py ───────────────────────────────────────────────────
"""
Utilities to assemble .lite.N parts, run fasterq-dump, and clean up.
Call `batch_convert(directory, n_workers=4)` from your main program.
"""

import os, glob, shutil, subprocess, multiprocessing as mp, logging as lg
from pathlib import Path
from typing import List

LOG = lg.getLogger("sra2fastq")

# --------------------------------------------------------------------------- #
# 1. Re-assemble multi-part SRA-Lite files                                    #
# --------------------------------------------------------------------------- #
def assemble_lite_parts(first_part: Path, keep_parts: bool = False) -> Path:
    """
    Turn SRR12345678.lite.1 + .2 + … → SRR12345678.sra and return its path.
    If `keep_parts` is False the chunk files are deleted afterwards.
    """
    prefix = first_part.with_suffix("")      # strip ".1"
    while prefix.suffix.startswith(".lite"):
        prefix = prefix.with_suffix("")      # strip ".lite"
    accession = prefix.name                 # e.g. "SRR12345678"

    parts = sorted(first_part.parent.glob(f"{accession}.lite.*"),
                   key=lambda p: int(p.suffix[1:]))   # numeric sort by ".1" ".2" …

    out_path = first_part.parent / f"{accession}.sra"
    LOG.info(f"Assembling {len(parts)} parts → {out_path.name}")

    with open(out_path, "wb") as w:
        for p in parts:
            with p.open("rb") as r:
                shutil.copyfileobj(r, w, length=128 * 1024)

    if not keep_parts:
        for p in parts:
            p.unlink(missing_ok=True)

    return out_path


# --------------------------------------------------------------------------- #
# 2. Convert a single .sra file to paired / single FASTQs via fasterq-dump     #
# --------------------------------------------------------------------------- #
def sra_to_fastq(sra_path: Path,
                 threads: int = max(1, mp.cpu_count() // 2),
                 temp_root: Path | None = None,
                 keep_sra: bool = False) -> None:
    """
    Run fasterq-dump on `sra_path`. Creates _1.fastq / _2.fastq (or _single).
    """
    temp_dir = temp_root or sra_path.parent / "tmp_fastq"
    cmd = ["fasterq-dump",
           "--split-files",
           "--threads", str(threads),
           "--temp", str(temp_dir),
           str(sra_path)]
    LOG.info(" ".join(cmd))

    temp_dir.mkdir(exist_ok=True, parents=True)
    subprocess.run(cmd, check=True)

    # tidy up
    shutil.rmtree(temp_dir, ignore_errors=True)
    if not keep_sra:
        sra_path.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# 3. Batch-scan a directory and process everything found                      #
# --------------------------------------------------------------------------- #
def _worker(job):
    """Helper for Pool: (accession, path_to_first_chunk_or_sra)."""
    acc, p = job
    try:
        p = Path(p)
        if p.suffix.startswith(".lite") and ".lite." in p.name:
            p = assemble_lite_parts(p)
        sra_to_fastq(p)
        LOG.info(f"[{acc}] ✔ finished")
    except subprocess.CalledProcessError as e:
        LOG.error(f"[{acc}] fasterq-dump failed: {e}")
    except Exception as exc:
        LOG.exception(f"[{acc}] unexpected error: {exc}")

def batch_convert(directory: str | os.PathLike, n_workers: int = 4) -> None:
    """
    Look for any *.sra, *.sralite, *.lite.1 in `directory` and convert them all.
    Uses a multiprocessing Pool for speed.
    """
    directory = Path(directory)
    jobs: List[tuple[str, str]] = []

    # classic single-file .sra / .sralite
    for sra in directory.glob("**/*.sr[al]*"):
        jobs.append((sra.stem, str(sra)))

    # first part of multi-chunk lite runs (".lite.1")
    for part1 in directory.glob("**/*.lite.1"):
        acc = part1.name.split(".lite.")[0]
        jobs.append((acc, str(part1)))

    if not jobs:
        LOG.warning("No SRA files found.")
        return

    LOG.info(f"Found {len(jobs)} accessions to convert → launching {n_workers} workers")
    with mp.Pool(processes=n_workers) as pool:
        pool.map(_worker, jobs)

# --------------------------------------------------------------------------- #
# Example CLI
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import argparse, sys
    argparse.logging.basicConfig(level="INFO", format="%(levelname)s: %(message)s")

    ap = argparse.ArgumentParser(description="Assemble SRA-Lite chunks and run fasterq-dump.")
    ap.add_argument("path", help="Directory that holds *.sra or *.lite.N files")
    ap.add_argument("-j", "--jobs", type=int, default=4, help="Concurrent conversions (CPU-bound)")
    args = ap.parse_args()

    batch_convert(args.path, n_workers=args.jobs)
