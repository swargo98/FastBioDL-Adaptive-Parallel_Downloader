#!/usr/bin/env python3
"""
benchmark_placement_mover.py

Generalization of benchmark_mover_transfer.py to arbitrary source and
destination storage tiers.  Compares FastBioDL's FileMover against naive
transfer methods for moving .fastq.gz files from ``--src-dir`` (e.g. NVMe
or RAID0) to ``--dst-dir`` (e.g. HDD or network filesystem).

Methods
-------
  shutil_seq       -- Sequential shutil.copy2
  shutil_parallel  -- ThreadPoolExecutor + shutil.copy2
  cp               -- Single GNU cp subprocess
  filemover        -- FastBioDL's mover.py (Manager proxies + optimizer)

The .fastq.gz payload is prepared once on the source tier (download +
fasterq-dump + pigz on ``--src-dir``), then staged to a fresh directory
before each rep.  This is required because FileMover deletes source files
after transfer; staging untimed also equalizes the starting state for all
methods.

Cache eviction between reps: attempts drop_caches (root), sudo drop_caches,
then an unprivileged decoy-write fallback.  Method used is logged per rep.

Same-tier copies (``--src-dir`` == ``--dst-dir``) are permitted and are a
useful control: any difference between methods under a same-tier copy
reflects pure method overhead, not I/O characteristics.

Output
------
One JSON file per accession containing per-rep timings, throughput (MB/s),
summary statistics, pairwise speedups, and the cache-eviction log.

Usage
-----
  python3 benchmark_placement_mover.py SRR1234567 \\
      --src-dir /mnt/raid0/$USER/scratch \\
      --dst-dir /tmp/$USER/scratch \\
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

_T_CRIT_95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
              6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228}


# ── shared helpers (kept identical to benchmark_placement_conversion.py) ─

def run_cmd(cmd: List[str], timeout_s: int = 0) -> Tuple[int, float, str, str]:
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, timeout=(timeout_s if timeout_s > 0 else None),
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
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError):
        pass
    return 16 * 1024 ** 3


def evict_cache(target_tier: Path, working_set_bytes: int) -> Dict:
    rec = {"method": None, "elapsed_s": 0.0, "success": False}
    t0 = time.perf_counter()

    try:
        subprocess.run(["sync"], check=True, timeout=60)
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
        rec.update(method="direct", success=True,
                   elapsed_s=round(time.perf_counter() - t0, 3))
        return rec
    except (PermissionError, OSError, subprocess.SubprocessError):
        pass

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

    ram = _total_ram_bytes()
    decoy_bytes = max(2 * ram, 2 * working_set_bytes, 1 << 30)
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
        )
    except (OSError, MemoryError) as exc:
        rec.update(method="decoy_write_failed", success=False,
                   error=str(exc),
                   elapsed_s=round(time.perf_counter() - t0, 3))
    finally:
        shutil.rmtree(decoy_dir, ignore_errors=True)
    return rec


# ── download + golden preparation ────────────────────────────────────────

def download_sra(accession: str, sra_dir: Path, script_dir: Path,
                 segment_size_mb: int = 512, max_segments: int = 8,
                 max_retries: int = 3) -> Tuple[Path, float, str]:
    try:
        import aiohttp  # noqa: F401
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


def prepare_fastq_gz(sra_path: Path, golden_dir: Path,
                     fasterq_cmd: str, pigz_cmd: str,
                     threads: int) -> Tuple[float, float, int]:
    tmp_dir = golden_dir.parent / "prep_tmp"
    clean_dir(golden_dir)
    clean_dir(tmp_dir)

    rc, fasterq_s, _, err = run_cmd([
        fasterq_cmd, "--threads", str(threads),
        "--temp", str(tmp_dir), "--outdir", str(golden_dir),
        "--split-3", "--skip-technical", str(sra_path),
    ], timeout_s=7200)
    if rc != 0:
        raise RuntimeError(f"fasterq-dump failed: {err.strip()[-500:]}")
    shutil.rmtree(tmp_dir, ignore_errors=True)

    fastq_files = sorted(golden_dir.glob("*.fastq"))
    if not fastq_files:
        raise RuntimeError("No .fastq produced during preparation")
    t0 = time.perf_counter()
    for fq in fastq_files:
        rc, _, _, err = run_cmd([pigz_cmd, "-1", "-p", str(threads), str(fq)])
        if rc != 0:
            raise RuntimeError(f"pigz failed: {err.strip()[-500:]}")
    pigz_s = time.perf_counter() - t0

    total_bytes = sum(f.stat().st_size for f in golden_dir.glob("*.fastq.gz"))
    return fasterq_s, pigz_s, total_bytes


def stage_golden_to(golden_dir: Path, staging_dir: Path) -> int:
    clean_dir(staging_dir)
    total = 0
    for gz in sorted(golden_dir.glob("*.fastq.gz")):
        shutil.copy2(gz, staging_dir / gz.name)
        total += gz.stat().st_size
    return total


# ── transfer methods ─────────────────────────────────────────────────────

def xfer_shutil_seq(src: Path, dst: Path, _threads: int) -> float:
    clean_dir(dst)
    t0 = time.perf_counter()
    for f in sorted(src.rglob("*.fastq.gz")):
        rel = f.relative_to(src)
        dest = dst / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, dest)
    return time.perf_counter() - t0


def xfer_shutil_parallel(src: Path, dst: Path, threads: int) -> float:
    clean_dir(dst)
    files = sorted(src.rglob("*.fastq.gz"))

    def _copy_one(f: Path) -> None:
        rel = f.relative_to(src)
        dest = dst / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, dest)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        futs = [pool.submit(_copy_one, f) for f in files]
        for fut in as_completed(futs):
            fut.result()
    return time.perf_counter() - t0


def xfer_cp(src: Path, dst: Path, _threads: int) -> float:
    clean_dir(dst)
    files = sorted(str(f) for f in src.rglob("*.fastq.gz"))
    if not files:
        return 0.0
    t0 = time.perf_counter()
    proc = subprocess.run(
        ["cp", "--"] + files + [str(dst) + "/"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=7200,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"cp failed: {proc.stderr.decode(errors='replace')[-500:]}")
    return time.perf_counter() - t0


def xfer_filemover(src: Path, dst: Path, threads: int) -> float:
    from config_fastbiodl import configurations
    configurations["method"] = "probe"
    configurations["fixed_probing"] = {"thread": threads}
    configurations.setdefault("K", 1.01)

    from mover import FileMover

    clean_dir(dst)
    move_queue = mp.Queue()
    config = {
        "thread_limit": threads,
        "probing_sec": 5,
        "io_limit": -1,
    }

    mover = FileMover(
        move_queue=move_queue,
        tmpfs_dir=str(src),
        root_dir=str(dst),
        config=config,
    )

    t0 = time.perf_counter()
    mover.begin()
    for gz in sorted(src.rglob("*.fastq.gz")):
        move_queue.put(str(gz))
    move_queue.put(None)
    mover.stop(timeout=3600)
    return time.perf_counter() - t0


# ── main ──────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Transfer-method benchmark for arbitrary source/destination tiers")
    ap.add_argument("accession", help="SRA accession (e.g. SRR390728)")
    ap.add_argument("--src-dir", required=True,
                    help="Source tier root (e.g. NVMe, RAID0)")
    ap.add_argument("--dst-dir", required=True,
                    help="Destination tier root (e.g. HDD, single disk, network)")
    ap.add_argument("--prep-dir", default="",
                    help="Where to do the one-time download + conversion. "
                         "Default: --src-dir.  Useful if src-dir is small and "
                         "preparation needs more space.")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--methods", default="shutil_seq,shutil_parallel,cp,filemover",
                    help="Comma-separated methods to run")
    ap.add_argument("--json-out", default="")
    ap.add_argument("--download-segment-size-mb", type=int, default=512)
    ap.add_argument("--download-max-segments", type=int, default=8)
    ap.add_argument("--skip-eviction", action="store_true")
    ap.add_argument("--cleanup", action="store_true")
    args = ap.parse_args()

    accession = args.accession.strip()
    script_dir = Path(__file__).resolve().parent
    src_root = Path(args.src_dir).resolve()
    dst_root = Path(args.dst_dir).resolve()
    prep_root = Path(args.prep_dir).resolve() if args.prep_dir else src_root

    for p in (src_root, dst_root, prep_root):
        p.mkdir(parents=True, exist_ok=True)

    cmds = {}
    for tool in ("fasterq-dump", "pigz"):
        try:
            cmds[tool] = resolve_tool(tool, script_dir)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2

    src_work = src_root / f"bench_mover_{accession}"
    dst_work = dst_root / f"bench_mover_{accession}"
    prep_work = prep_root / f"bench_mover_prep_{accession}"
    src_work.mkdir(parents=True, exist_ok=True)
    dst_work.mkdir(parents=True, exist_ok=True)
    prep_work.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Download to prep tier ────────────────────────────────────
    sra_dir = prep_work / "sra"
    golden_dir = prep_work / "golden"
    sra_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[1] Downloading {accession} to {prep_root} ...")
    try:
        sra_path, dl_s, source_url = download_sra(
            accession, sra_dir, script_dir,
            segment_size_mb=args.download_segment_size_mb,
            max_segments=args.download_max_segments,
        )
    except Exception as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        return 1
    if sra_path is None or not sra_path.exists():
        sra_path = find_sra_file(sra_dir, accession)
    if sra_path is None:
        print(f"SRA file not found under {sra_dir}", file=sys.stderr)
        return 1

    sra_size = sra_path.stat().st_size
    print(f"  {sra_path}  ({_human(sra_size)})  in {dl_s:.1f}s")

    # ── Step 2: Prepare golden .fastq.gz ─────────────────────────────────
    print(f"\n[2] Converting {accession} to .fastq.gz (golden copy) ...")
    fq_s, pz_s, golden_bytes = prepare_fastq_gz(
        sra_path, golden_dir,
        cmds["fasterq-dump"], cmds["pigz"], args.threads,
    )
    golden_files = sorted(golden_dir.glob("*.fastq.gz"))
    print(f"  {len(golden_files)} file(s), {_human(golden_bytes)}  "
          f"(fasterq={fq_s:.1f}s, pigz={pz_s:.1f}s)")

    # If prep_dir != src_dir, move the golden set to src so that staging
    # copies from the actual source tier.
    if prep_root != src_root:
        src_golden = src_work / "golden"
        clean_dir(src_golden)
        print(f"\n[2b] Copying golden set to source tier ({src_root}) ...")
        for gz in sorted(golden_dir.glob("*.fastq.gz")):
            shutil.copy2(gz, src_golden / gz.name)
        golden_dir = src_golden

    # ── Step 3: Run transfer methods ─────────────────────────────────────
    method_fns = {
        "shutil_seq": xfer_shutil_seq,
        "shutil_parallel": xfer_shutil_parallel,
        "cp": xfer_cp,
        "filemover": xfer_filemover,
    }
    requested = [m.strip() for m in args.methods.split(",") if m.strip()]
    unknown = [m for m in requested if m not in method_fns]
    if unknown:
        print(f"[ERROR] Unknown method(s): {unknown}", file=sys.stderr)
        return 2
    methods = {m: method_fns[m] for m in requested}

    results: Dict[str, List[Dict]] = {m: [] for m in methods}
    eviction_log: List[Dict] = []

    print(f"\n[3] Running {args.reps} reps x {len(methods)} methods "
          f"({src_root} -> {dst_root})")
    for rep in range(1, args.reps + 1):
        for method_name, fn in methods.items():
            staging = src_work / f"staging_{method_name}_r{rep}"
            staged_bytes = stage_golden_to(golden_dir, staging)

            ev_rec = {"rep": rep, "method": method_name, "attempts": []}
            if not args.skip_eviction:
                # Evict cache against destination tier so writes don't hit
                # cached pages, and then against source so reads start cold.
                ev_rec["attempts"].append(evict_cache(dst_root, staged_bytes))
                ev_rec["attempts"].append(evict_cache(src_root, staged_bytes))
            eviction_log.append(ev_rec)

            dest = dst_work / f"{method_name}_r{rep}"
            print(f"  rep={rep}/{args.reps}  method={method_name:16s} ... ",
                  end="", flush=True)
            try:
                elapsed = fn(staging, dest, args.threads)
                dest_bytes = sum(
                    f.stat().st_size for f in dest.rglob("*.fastq.gz"))
                throughput_mbs = (staged_bytes / (1024 * 1024)) / elapsed \
                    if elapsed > 0 else 0.0

                rec = {
                    "method": method_name,
                    "rep": rep,
                    "elapsed_s": round(elapsed, 3),
                    "bytes": staged_bytes,
                    "throughput_mbs": round(throughput_mbs, 2),
                    "output_bytes": dest_bytes,
                    "byte_match": dest_bytes == staged_bytes,
                }
                results[method_name].append(rec)
                print(f"{elapsed:.1f}s  {throughput_mbs:.1f} MB/s  "
                      f"match={dest_bytes == staged_bytes}")
            except Exception as exc:
                print(f"FAILED: {exc}")
                results[method_name].append(
                    {"method": method_name, "rep": rep, "error": str(exc)})

            shutil.rmtree(staging, ignore_errors=True)
            shutil.rmtree(dest, ignore_errors=True)

    # ── Summary ──────────────────────────────────────────────────────────
    summaries = {}
    for method_name, reps_list in results.items():
        ok = [r for r in reps_list if "error" not in r]
        if ok:
            summaries[method_name] = {
                "elapsed_s": summary_stats([r["elapsed_s"] for r in ok]),
                "throughput_mbs": summary_stats([r["throughput_mbs"] for r in ok]),
            }

    speedups = {}
    if "shutil_seq" in results:
        baseline_ok = [r for r in results["shutil_seq"] if "error" not in r]
        for alt in requested:
            if alt == "shutil_seq":
                continue
            alt_ok = [r for r in results[alt] if "error" not in r]
            n = min(len(baseline_ok), len(alt_ok))
            if n > 0:
                speedups[f"{alt}_vs_shutil_seq"] = speedup_stats(
                    [baseline_ok[i]["elapsed_s"] for i in range(n)],
                    [alt_ok[i]["elapsed_s"] for i in range(n)],
                )

    same_tier = src_root.resolve() == dst_root.resolve()

    report = {
        "benchmark": "placement_mover",
        "accession": accession,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "system": {
            "hostname": platform.node(),
            "cpu_count": mp.cpu_count(),
            "ram_bytes": _total_ram_bytes(),
            "threads_used": args.threads,
            "reps": args.reps,
        },
        "tiers": {
            "src_dir": str(src_root),
            "dst_dir": str(dst_root),
            "prep_dir": str(prep_root),
            "same_tier": same_tier,
        },
        "download": {
            "elapsed_s": round(dl_s, 3),
            "source_url": source_url,
            "sra_size_bytes": sra_size,
        },
        "golden": {
            "files": len(golden_files),
            "total_bytes": golden_bytes,
            "fasterq_s": round(fq_s, 3),
            "pigz_s": round(pz_s, 3),
        },
        "methods_requested": requested,
        "per_rep": results,
        "summary": summaries,
        "speedups": speedups,
        "cache_eviction_log": eviction_log,
    }

    out_json = (Path(args.json_out) if args.json_out
                else dst_root / f"benchmark_placement_mover_{accession}.json")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    with out_json.open("w") as f:
        json.dump(report, f, indent=2)

    print(f"\n{'=' * 75}")
    print(f"  MOVER BENCHMARK  --  {accession}")
    print(f"  src={src_root}")
    print(f"  dst={dst_root}")
    print(f"  payload: {len(golden_files)} file(s), {_human(golden_bytes)}  "
          f"(same_tier={same_tier})")
    print(f"{'=' * 75}")
    for m in requested:
        if m in summaries:
            s = summaries[m]["throughput_mbs"]
            t = summaries[m]["elapsed_s"]
            print(f"  {m:16s}  "
                  f"time={t['mean']:7.1f}s  "
                  f"throughput={s['mean']:7.1f} MB/s  "
                  f"95%CI=[{s['ci_95_lo']:.1f}, {s['ci_95_hi']:.1f}]")
    for name, sp in speedups.items():
        print(f"  speedup {name}: {sp['mean']:.2f}x  "
              f"95%CI=[{sp['ci_95_lo']:.2f}, {sp['ci_95_hi']:.2f}]")

    ev_methods = {}
    for e in eviction_log:
        for a in e.get("attempts", []):
            ev_methods[a.get("method", "none")] = \
                ev_methods.get(a.get("method", "none"), 0) + 1
    if ev_methods:
        print(f"  Cache-eviction methods used: {ev_methods}")
    print(f"  JSON -> {out_json}")
    print(f"{'=' * 75}")

    if args.cleanup:
        shutil.rmtree(src_work, ignore_errors=True)
        shutil.rmtree(dst_work, ignore_errors=True)
        if prep_root != src_root:
            shutil.rmtree(prep_work, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
