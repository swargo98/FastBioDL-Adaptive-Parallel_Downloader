#!/usr/bin/env python3
"""
benchmark_placement_conversion.py

Measures the effect of storage-tier placement on SRA-to-FASTQ conversion
throughput.  Generalizes the three-mode (staged/direct/hybrid) benchmark
into a full factorial over three placement decisions:

    sra_loc     -- tier from which fasterq-dump reads the .sra file
    fasterq_out -- tier for fasterq-dump .fastq output and scratch temp
    pigz_out    -- tier where pigz writes .fastq.gz

Each decision chooses between two user-declared tiers (``--fast-dir`` and
``--slow-dir``).  Cells are labelled with a three-character code using
``F`` for fast and ``S`` for slow, in the order above.  Eight cells total:

    FFF FFS FSF FSS SFF SFS SSF SSS

A final copy to ``--dest-dir`` is performed iff the pigz output tier is
not the same directory as ``--dest-dir``.  This corresponds to the
realistic scenario where a user wants the final .fastq.gz at a specific
persistent location regardless of where the conversion work happened.

Legacy mapping to the old three-mode naming:
    staged  -> FFF with dest = slow                (fasterq+pigz on fast, copy)
    direct  -> SSS with dest = slow                (everything on slow)
    hybrid  -> FFS with dest = slow                (fasterq on fast, pigz to slow)

The SRA file is downloaded once to the ``fast`` tier and staged to the
``slow`` tier before the sweep begins; per-cell setup copies from the
appropriate pre-staged location so that download time is not part of any
timed region.

Page-cache eviction between reps is attempted in three stages:
    1. write to /proc/sys/vm/drop_caches directly (privileged)
    2. sudo -n sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
    3. unprivileged fallback: write a decoy file larger than RAM on the
       tier being measured, fsync, delete, to force cache eviction

Output JSON contains per-rep timings (fasterq, pigz, copy, total), the
placement tuple for each cell, which cache-eviction method worked, and
summary statistics with 95% CIs.

Usage
-----
  python3 benchmark_placement_conversion.py SRR1234567 \\
      --fast-dir /mnt/raid0/$USER/scratch \\
      --slow-dir /tmp/$USER/scratch \\
      --dest-dir /mnt/raid0/$USER/results \\
      --threads 8 --reps 3
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import platform
import shutil
import subprocess
import sys
import time
from itertools import product
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

_T_CRIT_95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
              6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228}

# Placement cells: 3 binary decisions, fast (F) vs slow (S)
# Order of dims: (sra_loc, fasterq_out, pigz_out)
CELL_DIMS = ("sra_loc", "fasterq_out", "pigz_out")

# Map placement cells (with dest = slow) back to the legacy three-mode names
# so prior paper text / prior JSON can be compared directly.
LEGACY_CELL_MAP_DEST_SLOW = {
    "FFF": "staged",   # fasterq+pigz on fast, then copy to dest(slow)
    "FFS": "hybrid",   # fasterq on fast, pigz streams to slow
    "SSS": "direct",   # everything on slow
}


# ── shared helpers ────────────────────────────────────────────────────────

def run_cmd(cmd: List[str], timeout_s: int = 0,
            env: Optional[Dict[str, str]] = None) -> Tuple[int, float, str, str]:
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, timeout=(timeout_s if timeout_s > 0 else None),
            env=env,
        )
        return proc.returncode, time.perf_counter() - t0, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        return 124, time.perf_counter() - t0, exc.stdout or "", (exc.stderr or "") + "\nTIMEOUT"


def resolve_tool(binary: str, script_dir: Path) -> str:
    found = shutil.which(binary)
    if found:
        return found
    bundled = script_dir / "sratoolkit.3.1.0-ubuntu64" / "bin" / binary
    if bundled.exists() and os.access(bundled, os.X_OK):
        return str(bundled)
    raise RuntimeError(f"Required tool not found: {binary}")


def clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def summary_stats(values: List[float]) -> Dict:
    a = np.array(values, dtype=float)
    n = len(a)
    mean = float(np.mean(a))
    std = float(np.std(a, ddof=1)) if n > 1 else 0.0
    se = std / (n ** 0.5) if n > 1 else 0.0
    t_val = _T_CRIT_95.get(n - 1, 2.0)
    return {
        "n": n,
        "mean": round(mean, 4),
        "std": round(std, 4),
        "median": round(float(np.median(a)), 4),
        "min": round(float(np.min(a)), 4),
        "max": round(float(np.max(a)), 4),
        "ci_95_lo": round(mean - t_val * se, 4),
        "ci_95_hi": round(mean + t_val * se, 4),
    }


def speedup_stats(baseline_times: List[float],
                  treatment_times: List[float]) -> Dict:
    ratios = [b / t if t > 0 else float("inf")
              for b, t in zip(baseline_times, treatment_times)]
    return summary_stats(ratios)


def _human(nbytes: int) -> str:
    v = float(max(0, nbytes))
    for u in ("B", "KB", "MB", "GB", "TB"):
        if v < 1024.0 or u == "TB":
            return f"{v:.2f} {u}"
        v /= 1024.0


def _total_ram_bytes() -> int:
    """Best-effort RAM total.  Falls back to 16 GB if /proc/meminfo is absent."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kib = int(line.split()[1])
                    return kib * 1024
    except (OSError, ValueError):
        pass
    return 16 * 1024 ** 3


# ── cache eviction ────────────────────────────────────────────────────────

def _fadvise_dontneed(paths: List[Path]) -> int:
    """
    Ask the kernel to drop cached pages for the given files.  Returns the
    number of files successfully advised.  No-op on non-Linux.
    """
    if not hasattr(os, "posix_fadvise"):
        return 0
    n = 0
    for p in paths:
        try:
            fd = os.open(str(p), os.O_RDONLY)
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                n += 1
            finally:
                os.close(fd)
        except OSError:
            continue
    return n


def evict_cache(target_tier: Path, working_set_bytes: int,
                files_of_interest: Optional[List[Path]] = None,
                decoy_cap_bytes: Optional[int] = None) -> Dict:
    """
    Try four cache-eviction strategies in order:
      1. write '3' to /proc/sys/vm/drop_caches (requires CAP_SYS_ADMIN)
      2. sudo -n drop_caches (passwordless sudo)
      3. posix_fadvise(POSIX_FADV_DONTNEED) on files_of_interest
      4. decoy-write fallback, capped at decoy_cap_bytes

    The third strategy is targeted: it only evicts the specific files we
    care about, which is usually what we want and costs essentially
    nothing.  The decoy fallback is a last resort because on large-RAM
    systems it can take many minutes per call.
    """
    rec = {"method": None, "elapsed_s": 0.0, "success": False}
    t0 = time.perf_counter()

    # Strategy 1: direct drop_caches
    try:
        subprocess.run(["sync"], check=True, timeout=60)
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
        rec.update(method="direct", success=True,
                   elapsed_s=round(time.perf_counter() - t0, 3))
        return rec
    except (PermissionError, OSError, subprocess.SubprocessError):
        pass

    # Strategy 2: passwordless sudo
    try:
        proc = subprocess.run(
            ["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60,
        )
        if proc.returncode == 0:
            rec.update(method="sudo", success=True,
                       elapsed_s=round(time.perf_counter() - t0, 3))
            return rec
    except (OSError, subprocess.SubprocessError):
        pass

    # Strategy 3: targeted posix_fadvise on files we care about
    if files_of_interest:
        try:
            subprocess.run(["sync"], check=False, timeout=60)
        except (OSError, subprocess.SubprocessError):
            pass
        n_advised = _fadvise_dontneed(files_of_interest)
        if n_advised > 0:
            rec.update(method="fadvise_dontneed", success=True,
                       n_files=n_advised,
                       elapsed_s=round(time.perf_counter() - t0, 3))
            return rec

    # Strategy 4: unprivileged decoy-write fallback.
    # On large-RAM systems this is expensive; respect decoy_cap_bytes.
    if decoy_cap_bytes is not None and decoy_cap_bytes <= 0:
        rec.update(method="none",
                   success=False,
                   note="all strategies failed; decoy disabled",
                   elapsed_s=round(time.perf_counter() - t0, 3))
        return rec

    ram = _total_ram_bytes()
    requested = max(int(1.1 * ram), 2 * working_set_bytes, 1 << 30)
    if decoy_cap_bytes is not None:
        decoy_bytes = min(requested, decoy_cap_bytes)
    else:
        decoy_bytes = requested

    decoy_dir = target_tier / ".evict_decoy"
    try:
        clean_dir(decoy_dir)
        decoy_path = decoy_dir / "decoy.bin"
        block = b"\0" * (64 * 1024 * 1024)
        written = 0
        with open(decoy_path, "wb", buffering=0) as f:
            while written < decoy_bytes:
                n = min(len(block), decoy_bytes - written)
                f.write(block[:n])
                written += n
            f.flush()
            os.fsync(f.fileno())
        subprocess.run(["sync"], check=False, timeout=60)
        rec.update(
            method="decoy_write",
            success=True,
            elapsed_s=round(time.perf_counter() - t0, 3),
            decoy_bytes=decoy_bytes,
            decoy_capped=(decoy_cap_bytes is not None and decoy_bytes < requested),
        )
    except (OSError, MemoryError) as exc:
        rec.update(method="decoy_write_failed", success=False,
                   error=str(exc),
                   elapsed_s=round(time.perf_counter() - t0, 3))
    finally:
        shutil.rmtree(decoy_dir, ignore_errors=True)
    return rec


# ── download (same as before; reused unchanged for continuity) ────────────

def download_sra(accession: str, sra_dir: Path, script_dir: Path,
                 segment_size_mb: int = 512, max_segments: int = 8,
                 max_retries: int = 3) -> Tuple[Path, float, str]:
    try:
        import aiohttp  # noqa: F401 -- verified available
        import fastbiodl_upgrade as fb
    except ImportError as exc:
        raise RuntimeError(f"FastBioDL import failed: {exc}") from exc

    url_acc_pairs = fb.get_ncbi_urls(accession, field="sra_ftp")
    if not url_acc_pairs:
        raise RuntimeError(f"No SRA URL found for {accession}")

    url, _ = url_acc_pairs[0]
    filename = os.path.basename(url)
    local_path = sra_dir / accession / filename
    local_path.parent.mkdir(parents=True, exist_ok=True)

    fb.download_dir = str(sra_dir)
    fb.transfer_done = mp.Value("i", 0)
    fb.download_process_status = mp.Array("i", [1])

    counter = mp.Value("Q", 0)
    active = mp.Value("i", 0)
    reserved = mp.Value("Q", 0)
    pending = mp.Value("Q", 0)

    async def _dl():
        import aiohttp as _aio
        timeout = _aio.ClientTimeout(total=3600, connect=60, sock_read=300)
        connector = _aio.TCPConnector(
            limit=max_segments, limit_per_host=max_segments,
            ttl_dns_cache=300, enable_cleanup_closed=True,
        )
        async with _aio.ClientSession(
            connector=connector, timeout=timeout,
            headers={"User-Agent": "fastbiodl/3.0"},
        ) as session:
            dl = fb.SegmentedDownloader(
                session=session, url=url, local_path=str(local_path),
                segment_size=segment_size_mb * 1024 * 1024,
                max_segments=max_segments, process_id=0,
                process_counter=counter,
                active_connections=active,
                disk_reserved_bytes=reserved,
                min_pending_conversion_bytes=pending,
                disk_safety_margin_bytes=0,
                max_retries=max_retries,
            )
            return await dl.download_with_resume()

    t0 = time.perf_counter()
    success, paused, _ = asyncio.run(_dl())
    elapsed = time.perf_counter() - t0

    if not success:
        raise RuntimeError(f"Download {'paused' if paused else 'failed'} for {accession}")
    if not local_path.exists():
        raise RuntimeError(f"Success reported but file missing: {local_path}")
    return local_path, elapsed, url


def find_sra_file(sra_dir: Path, accession: str) -> Optional[Path]:
    for suffix in ["", ".sra", ".sralite.1", ".sralite.2", ".1", ".2"]:
        for base in [sra_dir, sra_dir / accession]:
            c = base / f"{accession}{suffix}"
            if c.exists() and c.is_file():
                return c
    for p in sorted(sra_dir.rglob(f"{accession}*")):
        if p.is_file() and p.stat().st_size > 0:
            return p
    return None


# ── conversion stages ────────────────────────────────────────────────────

def _run_fasterq(sra_path: Path, out_dir: Path, tmp_dir: Path,
                 fasterq_cmd: str, threads: int) -> float:
    rc, elapsed, _, err = run_cmd([
        fasterq_cmd, "--threads", str(threads),
        "--temp", str(tmp_dir), "--outdir", str(out_dir),
        "--split-3", "--skip-technical", str(sra_path),
    ], timeout_s=7200)
    if rc != 0:
        raise RuntimeError(f"fasterq-dump rc={rc}: {err.strip()[-500:]}")
    return elapsed


def _run_pigz(fastq_dir: Path, pigz_out_dir: Path,
              pigz_cmd: str, threads: int,
              same_tier_as_fastq: bool) -> float:
    """
    Compress every .fastq under ``fastq_dir``.  If the pigz output tier is
    the same filesystem as the fastq directory we compress in-place (pigz
    rewrites the file; simulates the ``direct`` mode).  Otherwise we stream
    pigz stdout to the destination tier (simulates the ``hybrid`` mode).
    """
    fastq_files = sorted(fastq_dir.glob("*.fastq"))
    if not fastq_files:
        raise RuntimeError(f"No .fastq produced in {fastq_dir}")

    pigz_out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()

    if same_tier_as_fastq:
        # In-place compression leaves the .gz alongside the .fastq
        for fq in fastq_files:
            rc, _, _, err = run_cmd(
                [pigz_cmd, "-1", "-p", str(threads), str(fq)])
            if rc != 0:
                raise RuntimeError(f"pigz rc={rc}: {err.strip()[-500:]}")
        # If the requested pigz_out_dir differs from fastq_dir but is on the
        # same tier (i.e. same filesystem, different path), rename into place.
        if pigz_out_dir.resolve() != fastq_dir.resolve():
            for gz in sorted(fastq_dir.glob("*.fastq.gz")):
                gz.rename(pigz_out_dir / gz.name)
    else:
        # Stream compressed output across tiers.  Do NOT keep the .fastq on
        # the source tier after; our caller will clean up fastq_dir.
        for fq in fastq_files:
            gz_dest = pigz_out_dir / (fq.name + ".gz")
            with open(gz_dest, "wb") as out_f:
                proc = subprocess.run(
                    [pigz_cmd, "-1", "-c", "-p", str(threads), str(fq)],
                    stdout=out_f, stderr=subprocess.PIPE, timeout=7200,
                )
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"pigz failed (stream): "
                        f"{proc.stderr.decode(errors='replace')[-500:]}")

    return time.perf_counter() - t0


def _output_summary(gz_dir: Path) -> Tuple[int, int]:
    gz = list(gz_dir.glob("*.fastq.gz"))
    return len(gz), sum(f.stat().st_size for f in gz)


def _path_tier(path: Path, fast_root: Path, slow_root: Path) -> str:
    """Return 'fast', 'slow', or 'other' depending on which root *path* is under."""
    p = path.resolve()
    fr = fast_root.resolve()
    sr = slow_root.resolve()
    try:
        p.relative_to(fr)
        return "fast"
    except ValueError:
        pass
    try:
        p.relative_to(sr)
        return "slow"
    except ValueError:
        pass
    return "other"


# ── per-cell pipeline ────────────────────────────────────────────────────

def run_cell(cell_code: str,
             sra_on_fast: Path, sra_on_slow: Path,
             fast_tier: Path, slow_tier: Path,
             dest_dir: Path,
             cell_work_fast: Path, cell_work_slow: Path,
             fasterq_cmd: str, pigz_cmd: str,
             threads: int) -> Dict:
    """
    Execute the conversion pipeline for one placement cell.

    Parameters
    ----------
    cell_code : 3-char string in {F, S}^3 giving (sra_loc, fasterq_out, pigz_out).
    sra_on_fast / sra_on_slow : pre-staged copies of the .sra file on each tier.
    fast_tier / slow_tier : tier root paths (used only to select between them).
    dest_dir : the user's final destination.  A copy step runs iff the pigz
               output directory differs from dest_dir.
    cell_work_fast / cell_work_slow : scratch roots for this cell on each tier.
    """
    assert len(cell_code) == 3 and set(cell_code) <= {"F", "S"}
    sra_loc_is_fast, fasterq_is_fast, pigz_is_fast = (c == "F" for c in cell_code)

    sra_path = sra_on_fast if sra_loc_is_fast else sra_on_slow
    fasterq_root = cell_work_fast if fasterq_is_fast else cell_work_slow
    pigz_root = cell_work_fast if pigz_is_fast else cell_work_slow

    # Determine which tier dest_dir belongs to, so we can short-circuit the
    # copy step whenever pigz already writes to the dest tier.  When the
    # pigz tier matches the dest tier we write pigz output DIRECTLY to
    # dest_dir; otherwise we stage it in a per-cell gz/ subdir.
    pigz_tier = "fast" if pigz_is_fast else "slow"
    dest_tier = _path_tier(dest_dir, fast_tier, slow_tier)

    fastq_dir = fasterq_root / "fastq"
    tmp_dir = fasterq_root / "tmp"

    if pigz_tier == dest_tier and dest_tier != "other":
        # pigz writes straight to the final destination.  No copy step.
        pigz_dir = dest_dir
    else:
        pigz_dir = pigz_root / "gz"

    clean_dir(fastq_dir)
    clean_dir(tmp_dir)
    clean_dir(pigz_dir)

    t_wall = time.perf_counter()

    fasterq_s = _run_fasterq(sra_path, fastq_dir, tmp_dir,
                             fasterq_cmd, threads)

    pigz_s = _run_pigz(
        fastq_dir=fastq_dir,
        pigz_out_dir=pigz_dir,
        pigz_cmd=pigz_cmd,
        threads=threads,
        same_tier_as_fastq=(fasterq_is_fast == pigz_is_fast),
    )

    # Final copy to destination iff pigz_dir is not dest_dir.
    copy_s = 0.0
    dest_dir.mkdir(parents=True, exist_ok=True)
    if pigz_dir.resolve() != dest_dir.resolve():
        t_copy = time.perf_counter()
        for gz in sorted(pigz_dir.glob("*.fastq.gz")):
            shutil.copy2(gz, dest_dir / gz.name)
        copy_s = time.perf_counter() - t_copy

    total_s = time.perf_counter() - t_wall

    # Authoritative output check: always read from dest_dir.
    n_dest, dest_bytes = _output_summary(dest_dir)
    n_pigz, pigz_bytes = _output_summary(pigz_dir)

    return {
        "cell": cell_code,
        "placement": {
            "sra_loc": "fast" if sra_loc_is_fast else "slow",
            "fasterq_out": "fast" if fasterq_is_fast else "slow",
            "pigz_out": "fast" if pigz_is_fast else "slow",
            "dest_tier": dest_tier,
            "dest": str(dest_dir),
            "pigz_writes_direct_to_dest": pigz_dir.resolve() == dest_dir.resolve(),
        },
        "fasterq_s": round(fasterq_s, 3),
        "pigz_s": round(pigz_s, 3),
        "copy_s": round(copy_s, 3),
        "total_s": round(total_s, 3),
        "output_files_at_dest": n_dest,
        "output_bytes_at_dest": dest_bytes,
        "output_files_at_pigz_dir": n_pigz,
        "output_bytes_at_pigz_dir": pigz_bytes,
    }


# ── main ──────────────────────────────────────────────────────────────────

def parse_cells(spec: str) -> List[str]:
    if not spec or spec.lower() == "all":
        return ["".join(t) for t in product("FS", repeat=3)]
    cells = [c.strip().upper() for c in spec.split(",") if c.strip()]
    for c in cells:
        if len(c) != 3 or set(c) - {"F", "S"}:
            raise argparse.ArgumentTypeError(
                f"Invalid cell code {c!r}; must be 3 chars from {{F, S}}")
    return cells


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Factorial placement benchmark for SRA conversion pipeline")
    ap.add_argument("accession", help="SRA accession (e.g. SRR390728)")
    ap.add_argument("--fast-dir", required=True,
                    help="Path to the 'fast' storage tier (e.g. NVMe, RAID0)")
    ap.add_argument("--slow-dir", required=True,
                    help="Path to the 'slow' storage tier (e.g. HDD, single disk)")
    ap.add_argument("--dest-dir", default="",
                    help="User's final destination.  Default: --slow-dir")
    ap.add_argument("--cells", default="all", type=parse_cells,
                    help="Comma-separated cell codes, e.g. 'FFF,FFS,SSS'."
                         " Use 'all' for the full 8-cell factorial.")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--json-out", default="")
    ap.add_argument("--download-segment-size-mb", type=int, default=512)
    ap.add_argument("--download-max-segments", type=int, default=8)
    ap.add_argument("--skip-eviction", action="store_true",
                    help="Do not attempt page-cache eviction between reps")
    ap.add_argument("--decoy-cap-bytes", type=int, default=8 * 1024 ** 3,
                    help="Max bytes to write for decoy-based cache eviction "
                         "(default: 8 GiB).  Only used if drop_caches/sudo/"
                         "fadvise all fail.  Set to 0 to disable the decoy "
                         "fallback entirely.")
    ap.add_argument("--cleanup", action="store_true")
    args = ap.parse_args()

    accession = args.accession.strip()
    script_dir = Path(__file__).resolve().parent

    fast_root = Path(args.fast_dir).resolve()
    slow_root = Path(args.slow_dir).resolve()
    dest_dir_root = Path(args.dest_dir).resolve() if args.dest_dir \
        else slow_root
    fast_root.mkdir(parents=True, exist_ok=True)
    slow_root.mkdir(parents=True, exist_ok=True)
    dest_dir_root.mkdir(parents=True, exist_ok=True)

    if fast_root == slow_root:
        print("[ERROR] --fast-dir and --slow-dir resolve to the same path; "
              "a two-tier factorial is meaningless.", file=sys.stderr)
        return 2

    cmds = {}
    for tool in ("fasterq-dump", "pigz"):
        try:
            cmds[tool] = resolve_tool(tool, script_dir)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    print(f"Tools: {cmds}")

    fast_work = fast_root / f"bench_place_{accession}"
    slow_work = slow_root / f"bench_place_{accession}"
    fast_work.mkdir(parents=True, exist_ok=True)
    slow_work.mkdir(parents=True, exist_ok=True)
    dest_dir = dest_dir_root / f"bench_place_{accession}_dest"
    dest_dir.mkdir(parents=True, exist_ok=True)

    # ── Download to fast tier once ───────────────────────────────────────
    sra_dir_fast = fast_work / "sra"
    sra_dir_fast.mkdir(parents=True, exist_ok=True)
    print(f"\n[1] Downloading {accession} to fast tier ({fast_root}) ...")
    try:
        sra_path_fast, dl_s, source_url = download_sra(
            accession, sra_dir_fast, script_dir,
            segment_size_mb=args.download_segment_size_mb,
            max_segments=args.download_max_segments,
        )
    except Exception as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        return 1
    if sra_path_fast is None or not sra_path_fast.exists():
        sra_path_fast = find_sra_file(sra_dir_fast, accession)
    if sra_path_fast is None:
        print(f"SRA not found under {sra_dir_fast}", file=sys.stderr)
        return 1
    sra_size = sra_path_fast.stat().st_size
    print(f"  {sra_path_fast}  ({_human(sra_size)})  in {dl_s:.1f}s")

    # Pre-stage a copy on the slow tier so SRA-on-slow reads are not
    # contaminated by a first-access copy.
    sra_dir_slow = slow_work / "sra"
    sra_dir_slow.mkdir(parents=True, exist_ok=True)
    sra_path_slow = sra_dir_slow / sra_path_fast.name
    if not sra_path_slow.exists() or sra_path_slow.stat().st_size != sra_size:
        print(f"[1b] Copying SRA to slow tier for SRA-on-slow cells ...")
        t0 = time.perf_counter()
        shutil.copy2(sra_path_fast, sra_path_slow)
        print(f"     staged in {time.perf_counter() - t0:.1f}s")

    # ── Run the factorial ────────────────────────────────────────────────
    cells = args.cells
    print(f"\n[2] Running {args.reps} reps across {len(cells)} placement cells: "
          f"{', '.join(cells)}")

    results: Dict[str, List[Dict]] = {c: [] for c in cells}
    eviction_log: List[Dict] = []
    step = 1
    working_set_bytes = sra_size * 2  # rough over-estimate for decoy sizing

    for rep in range(1, args.reps + 1):
        for cell in cells:
            step += 1

            # Evict cache on both tiers so reads cannot hit warm pages
            # from a prior rep.  We evict against the tier we are about to
            # read from most (SRA source).
            ev = {"rep": rep, "cell": cell, "attempts": []}
            if not args.skip_eviction:
                target_for_decoy = fast_root if cell[0] == "F" else slow_root
                # Files of interest: both SRA copies (one is the source for
                # this cell, the other may be cached from a prior cell).
                foi = [sra_path_fast, sra_path_slow]
                ev_rec = evict_cache(
                    target_for_decoy,
                    working_set_bytes,
                    files_of_interest=foi,
                    decoy_cap_bytes=args.decoy_cap_bytes,
                )
                ev["attempts"].append(ev_rec)
            eviction_log.append(ev)

            cell_fast = fast_work / f"{cell}_r{rep}"
            cell_slow = slow_work / f"{cell}_r{rep}"
            cell_dest = dest_dir / f"{cell}_r{rep}"

            print(f"  [{step}] rep={rep}/{args.reps}  cell={cell} ... ",
                  end="", flush=True)
            try:
                r = run_cell(
                    cell_code=cell,
                    sra_on_fast=sra_path_fast,
                    sra_on_slow=sra_path_slow,
                    fast_tier=fast_root, slow_tier=slow_root,
                    dest_dir=cell_dest,
                    cell_work_fast=cell_fast, cell_work_slow=cell_slow,
                    fasterq_cmd=cmds["fasterq-dump"],
                    pigz_cmd=cmds["pigz"],
                    threads=args.threads,
                )
                r["rep"] = rep
                r["legacy_mode"] = LEGACY_CELL_MAP_DEST_SLOW.get(cell, "other") \
                    if dest_dir_root.resolve() == slow_root.resolve() \
                    else "n/a"
                results[cell].append(r)
                print(f"total={r['total_s']:.1f}s  "
                      f"fasterq={r['fasterq_s']:.1f}  "
                      f"pigz={r['pigz_s']:.1f}  "
                      f"copy={r['copy_s']:.1f}")
            except RuntimeError as exc:
                print(f"FAILED: {exc}")
                results[cell].append({"cell": cell, "rep": rep, "error": str(exc)})

            # Reclaim space between reps
            shutil.rmtree(cell_fast, ignore_errors=True)
            shutil.rmtree(cell_slow, ignore_errors=True)
            shutil.rmtree(cell_dest, ignore_errors=True)

    # ── Summary stats ────────────────────────────────────────────────────
    summaries = {}
    for cell, reps_list in results.items():
        ok = [r for r in reps_list if "error" not in r]
        if not ok:
            continue
        summaries[cell] = {}
        for field in ("total_s", "fasterq_s", "pigz_s", "copy_s"):
            vals = [r[field] for r in ok if field in r]
            if vals:
                summaries[cell][field] = summary_stats(vals)

    # Pairwise speedup: use SSS (all-slow) as baseline if present, else the
    # first cell by code.  >1 means the other cell is faster than baseline.
    baseline_cell = "SSS" if "SSS" in summaries else (
        sorted(summaries.keys())[0] if summaries else None)
    speedups = {}
    if baseline_cell is not None:
        base_ok = [r for r in results[baseline_cell] if "error" not in r]
        for cell in cells:
            if cell == baseline_cell:
                continue
            alt_ok = [r for r in results[cell] if "error" not in r]
            n = min(len(base_ok), len(alt_ok))
            if n > 0:
                speedups[f"{baseline_cell}_vs_{cell}"] = speedup_stats(
                    [base_ok[i]["total_s"] for i in range(n)],
                    [alt_ok[i]["total_s"] for i in range(n)],
                )

    # ── Report ───────────────────────────────────────────────────────────
    report = {
        "benchmark": "placement_conversion",
        "accession": accession,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "system": {
            "hostname": platform.node(),
            "cpu_count": mp.cpu_count(),
            "ram_bytes": _total_ram_bytes(),
            "threads_used": args.threads,
            "reps": args.reps,
            "fasterq_dump": cmds["fasterq-dump"],
            "pigz": cmds["pigz"],
        },
        "tiers": {
            "fast_dir": str(fast_root),
            "slow_dir": str(slow_root),
            "dest_dir": str(dest_dir_root),
            "dest_equals_slow": dest_dir_root.resolve() == slow_root.resolve(),
            "dest_equals_fast": dest_dir_root.resolve() == fast_root.resolve(),
        },
        "download": {
            "elapsed_s": round(dl_s, 3),
            "source_url": source_url,
            "sra_size_bytes": sra_size,
        },
        "cells_requested": cells,
        "per_rep": results,
        "summary": summaries,
        "speedup_baseline": baseline_cell,
        "speedups": speedups,
        "cache_eviction_log": eviction_log,
    }

    out_json = (Path(args.json_out) if args.json_out
                else dest_dir_root / f"benchmark_placement_conversion_{accession}.json")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    with out_json.open("w") as f:
        json.dump(report, f, indent=2)

    # Console summary
    print(f"\n{'=' * 75}")
    print(f"  PLACEMENT BENCHMARK  --  {accession}")
    print(f"  fast={fast_root}")
    print(f"  slow={slow_root}")
    print(f"  dest={dest_dir_root}")
    print(f"{'=' * 75}")
    print(f"  cell  {'total':>10s}   {'fasterq':>10s}   {'pigz':>10s}   {'copy':>10s}   legacy")
    for cell in cells:
        if cell not in summaries:
            print(f"  {cell}  (no successful reps)")
            continue
        s = summaries[cell]
        legacy = LEGACY_CELL_MAP_DEST_SLOW.get(cell, "other") \
            if dest_dir_root.resolve() == slow_root.resolve() else "n/a"
        print(f"  {cell}  "
              f"{s.get('total_s', {}).get('mean', 0):>8.1f}s  "
              f"{s.get('fasterq_s', {}).get('mean', 0):>8.1f}s  "
              f"{s.get('pigz_s', {}).get('mean', 0):>8.1f}s  "
              f"{s.get('copy_s', {}).get('mean', 0):>8.1f}s  "
              f"  {legacy}")
    if speedups:
        print(f"\n  Speedup baseline: {baseline_cell}  (>1 means other cell faster)")
        for name, sp in speedups.items():
            print(f"    {name}: {sp['mean']:.2f}x  "
                  f"95%CI=[{sp['ci_95_lo']:.2f}, {sp['ci_95_hi']:.2f}]")
    # Cache-eviction method distribution
    ev_methods = {}
    for e in eviction_log:
        for a in e.get("attempts", []):
            ev_methods[a.get("method", "none")] = \
                ev_methods.get(a.get("method", "none"), 0) + 1
    if ev_methods:
        print(f"\n  Cache-eviction methods used: {ev_methods}")
    print(f"  JSON -> {out_json}")
    print(f"{'=' * 75}")

    if args.cleanup:
        shutil.rmtree(fast_work, ignore_errors=True)
        shutil.rmtree(slow_work, ignore_errors=True)
        shutil.rmtree(dest_dir, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
