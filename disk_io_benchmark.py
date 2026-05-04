#!/usr/bin/env python3
"""
disk_io_benchmark.py

Storage-tier characterization harness for the SeqFlux experiment-setup
section. Each tier (NVMe, Lustre, HDD, RAID0, etc.) is treated as an
independent input; the harness runs the same set of fio workloads against
each tier, then runs cross-tier copy workloads over every ordered pair.

Test types
----------
    seq_write_1m            large block sequential write (fasterq-dump
                            output, downloader, pigz output)
    seq_read_1m             large block sequential read (pigz input,
                            move stage source)
    seq_rw_concurrent       parallel sequential read + write streams on
                            the same tier (pigz reading FASTQ and writing
                            .gz on the same volume)
    mixed_random_64k        70/30 read/write at 64k random offsets
                            (fasterq-dump temp/scratch traffic)
    fsync_latency_4k        4k synchronous writes with fsync after each
                            (segment metadata atomic update path)
    metadata_create_unlink  per-second file create + unlink rate
                            (Lustre/HDD metadata cost, transient temp
                            files materialized by fasterq-dump)
    cross_tier_copy         sequential read from tier A in parallel with
                            sequential write to tier B (placement stage)

For every test except metadata_create_unlink and cross_tier_copy, the
harness sweeps numjobs over a configurable list (default 1, 2, 4, 8, 16)
to expose the throughput plateau and the latency knee that justify the
admission-gate thresholds in Section IV-E of the paper.

Output
------
A single JSON file with one record per (test, tier or tier pair, numjobs,
repeat). Each record contains the full fio JSON payload for the run plus
a small flat summary of bandwidth (KiB/s) and completion latency (ns,
mean and p99). The driver does not aggregate across repeats; aggregation
is left to the analysis stage.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Tier configuration
# ---------------------------------------------------------------------------

@dataclass
class Tier:
    name: str
    path: Path
    # If True, the harness will skip mixed_random_64k and metadata tests
    # for this tier. HDD spindles produce numbers that are correct but
    # uninteresting for the random workload; we still run them by default.
    skip_random: bool = False


def parse_tiers(tier_args: List[str]) -> List[Tier]:
    tiers: List[Tier] = []
    seen: set[str] = set()
    for raw in tier_args:
        if "=" not in raw:
            raise SystemExit(f"--tier expects NAME=PATH, got: {raw!r}")
        name, path = raw.split("=", 1)
        name = name.strip()
        path = Path(path.strip()).resolve()
        if name in seen:
            raise SystemExit(f"duplicate tier name: {name}")
        seen.add(name)
        path.mkdir(parents=True, exist_ok=True)
        tiers.append(Tier(name=name, path=path))
    if not tiers:
        raise SystemExit("at least one --tier NAME=PATH is required")
    return tiers


# ---------------------------------------------------------------------------
# fio invocation
# ---------------------------------------------------------------------------

def require_fio() -> str:
    fio = shutil.which("fio")
    if not fio:
        raise SystemExit(
            "fio not found on PATH. On Ubuntu/Debian: apt-get install fio. "
            "On Expanse: module load fio (or install into the conda env)."
        )
    return fio


def run_fio(fio_bin: str, job_lines: List[str], log_path: Path) -> Dict[str, Any]:
    """Run a fio job described by ``job_lines`` and return parsed JSON.

    The job is written to a temporary file rather than passed inline so
    that fio's own logging in case of failure shows the exact spec.
    """
    job_file = log_path.with_suffix(".fio")
    job_file.write_text("\n".join(job_lines) + "\n")

    cmd = [fio_bin, "--output-format=json", f"--output={log_path}", str(job_file)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(f"[fio FAILED] cmd: {' '.join(cmd)}\n")
        sys.stderr.write(proc.stdout + "\n" + proc.stderr + "\n")
        raise SystemExit(proc.returncode)

    with log_path.open() as fh:
        return json.load(fh)


def summarize_fio(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Extract a small flat summary from fio's JSON output.

    For multi-job runs we sum bandwidths and report the population mean
    of per-job mean latencies; p99 is the max of per-job p99s, which is
    the conservative interpretation appropriate for a saturation study.
    """
    jobs = payload.get("jobs", [])
    read_bw = sum(j.get("read", {}).get("bw", 0) for j in jobs)
    write_bw = sum(j.get("write", {}).get("bw", 0) for j in jobs)

    def _mean_lat(direction: str) -> Optional[float]:
        vals = [
            j.get(direction, {}).get("clat_ns", {}).get("mean")
            for j in jobs
            if j.get(direction, {}).get("clat_ns", {}).get("mean") not in (None, 0)
        ]
        return float(statistics.mean(vals)) if vals else None

    def _p99_lat(direction: str) -> Optional[float]:
        vals = []
        for j in jobs:
            pcts = j.get(direction, {}).get("clat_ns", {}).get("percentile", {})
            v = pcts.get("99.000000")
            if v not in (None, 0):
                vals.append(v)
        return float(max(vals)) if vals else None

    return {
        "read_bw_kib_s": read_bw,
        "write_bw_kib_s": write_bw,
        "read_clat_mean_ns": _mean_lat("read"),
        "write_clat_mean_ns": _mean_lat("write"),
        "read_clat_p99_ns": _p99_lat("read"),
        "write_clat_p99_ns": _p99_lat("write"),
        "njobs": len(jobs),
    }


# ---------------------------------------------------------------------------
# Job specs
# ---------------------------------------------------------------------------

def common_lines(directory: Path, runtime: int, ramp: int, size: str) -> List[str]:
    return [
        "[global]",
        "ioengine=libaio",
        "direct=1",
        "group_reporting=0",
        f"directory={directory}",
        f"size={size}",
        f"runtime={runtime}",
        f"ramp_time={ramp}",
        "time_based=1",
        "fallocate=native",
        "thread=0",
    ]


def spec_seq_write(tier: Tier, numjobs: int, size: str, runtime: int, ramp: int) -> List[str]:
    lines = common_lines(tier.path, runtime, ramp, size)
    lines += [
        "[seq_write_1m]",
        "rw=write",
        "bs=1M",
        "iodepth=32",
        f"numjobs={numjobs}",
        "stonewall",
    ]
    return lines


def spec_seq_read(tier: Tier, numjobs: int, size: str, runtime: int, ramp: int) -> List[str]:
    lines = common_lines(tier.path, runtime, ramp, size)
    lines += [
        "[seq_read_1m]",
        "rw=read",
        "bs=1M",
        "iodepth=32",
        f"numjobs={numjobs}",
        "stonewall",
    ]
    return lines


def spec_seq_rw_concurrent(tier: Tier, numjobs: int, size: str, runtime: int, ramp: int) -> List[str]:
    """Two concurrent stream sets on the same tier: readers and writers.

    This matches the pigz-on-same-volume case in which a single process
    has one read stream and one write stream against the same device.
    """
    lines = common_lines(tier.path, runtime, ramp, size)
    lines += [
        "[seq_read_stream]",
        "rw=read",
        "bs=1M",
        "iodepth=16",
        f"numjobs={numjobs}",
        "[seq_write_stream]",
        "rw=write",
        "bs=1M",
        "iodepth=16",
        f"numjobs={numjobs}",
    ]
    return lines


def spec_mixed_random(tier: Tier, numjobs: int, size: str, runtime: int, ramp: int) -> List[str]:
    """70/30 random read/write at 64 KiB. Approximates fasterq-dump's
    scratch traffic during decode and reconstruction more faithfully
    than a pure 4k OLTP-style pattern."""
    lines = common_lines(tier.path, runtime, ramp, size)
    lines += [
        "[mixed_random_64k]",
        "rw=randrw",
        "rwmixread=70",
        "bs=64k",
        "iodepth=32",
        f"numjobs={numjobs}",
        "stonewall",
    ]
    return lines


def spec_fsync_latency(tier: Tier, runtime: int, ramp: int) -> List[str]:
    """Single-job 4 KiB synchronous-write workload with fsync after each
    write. iodepth=1 because the metadata-update path is inherently
    serial on a single segment."""
    lines = common_lines(tier.path, runtime, ramp, size="64M")
    # Override ioengine: psync is the right model for the metadata path.
    lines = [l for l in lines if not l.startswith("ioengine=") and not l.startswith("direct=")]
    lines += [
        "ioengine=psync",
        "direct=0",
        "[fsync_latency_4k]",
        "rw=write",
        "bs=4k",
        "fsync=1",
        "iodepth=1",
        "numjobs=1",
        "stonewall",
    ]
    return lines


def run_metadata_test(tier: Tier, n_files: int, repeat: int) -> Dict[str, Any]:
    """Create then unlink ``n_files`` zero-byte files in ``tier.path``,
    timing each phase. fio's filecreate and filedelete engines are not
    universally available, so we measure directly with os.* calls."""
    bench_dir = tier.path / f"meta_{repeat}_{int(time.time())}"
    bench_dir.mkdir(parents=True, exist_ok=False)
    try:
        names = [bench_dir / f"f_{i:06d}" for i in range(n_files)]

        t0 = time.perf_counter()
        for p in names:
            fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o600)
            os.close(fd)
        t1 = time.perf_counter()
        # Force directory durability so we are not biased by deferred work.
        try:
            dir_fd = os.open(bench_dir, os.O_RDONLY)
            os.fsync(dir_fd)
            os.close(dir_fd)
        except OSError:
            pass
        t2 = time.perf_counter()
        for p in names:
            os.unlink(p)
        t3 = time.perf_counter()

        return {
            "n_files": n_files,
            "create_seconds": t1 - t0,
            "create_dirfsync_seconds": t2 - t1,
            "unlink_seconds": t3 - t2,
            "create_rate_per_s": n_files / max(t1 - t0, 1e-9),
            "unlink_rate_per_s": n_files / max(t3 - t2, 1e-9),
        }
    finally:
        # Best-effort cleanup, including any survivors from a partial run.
        shutil.rmtree(bench_dir, ignore_errors=True)


def spec_cross_tier_copy(
    src: Tier, dst: Tier, numjobs: int, size: str, runtime: int, ramp: int
) -> List[str]:
    """Independent read jobs on src and write jobs on dst running
    concurrently. This is the move-stage workload, which on Expanse is
    Lustre to NVMe and on Fabric/local is NVMe to HDD."""
    lines = [
        "[global]",
        "ioengine=libaio",
        "direct=1",
        "group_reporting=0",
        f"size={size}",
        f"runtime={runtime}",
        f"ramp_time={ramp}",
        "time_based=1",
        "fallocate=native",
        "[read_src]",
        f"directory={src.path}",
        "rw=read",
        "bs=1M",
        "iodepth=32",
        f"numjobs={numjobs}",
        "[write_dst]",
        f"directory={dst.path}",
        "rw=write",
        "bs=1M",
        "iodepth=32",
        f"numjobs={numjobs}",
    ]
    return lines


# ---------------------------------------------------------------------------
# Cleanup of fio test files
# ---------------------------------------------------------------------------

def clean_tier_files(tier: Tier) -> None:
    """Remove fio-created test files but leave the tier directory itself."""
    for entry in tier.path.iterdir():
        try:
            if entry.is_file():
                entry.unlink()
            elif entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--tier",
        action="append",
        required=True,
        help="storage tier in the form NAME=PATH; may be repeated",
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument(
        "--numjobs",
        type=str,
        default="1,2,4,8,16",
        help="comma-separated list of numjobs values to sweep",
    )
    parser.add_argument(
        "--size",
        type=str,
        default="4G",
        help="per-job working set size for fio bandwidth tests",
    )
    parser.add_argument("--runtime", type=int, default=30, help="runtime per fio invocation in seconds")
    parser.add_argument("--ramp", type=int, default=5, help="ramp_time skipped before measurement")
    parser.add_argument(
        "--metadata-files",
        type=int,
        default=20000,
        help="number of zero-byte files for the metadata create/unlink test",
    )
    parser.add_argument(
        "--skip",
        type=str,
        default="",
        help="comma-separated list of tests to skip (seq_write,seq_read,seq_rw,random,fsync,metadata,cross_tier)",
    )
    parser.add_argument("--json-out", type=Path, required=True)
    parser.add_argument("--log-dir", type=Path, default=None,
                        help="directory for raw fio JSON logs (default: alongside --json-out)")
    args = parser.parse_args()

    tiers = parse_tiers(args.tier)
    fio_bin = require_fio()
    numjobs_list = [int(x) for x in args.numjobs.split(",") if x.strip()]
    skip = {s.strip() for s in args.skip.split(",") if s.strip()}

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    log_dir = args.log_dir or args.json_out.parent / f"{args.json_out.stem}_fio_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    records: List[Dict[str, Any]] = []
    run_started = time.time()

    def record(test: str, tier_label: str, numjobs: int, repeat: int,
               summary: Dict[str, Any], extra: Optional[Dict[str, Any]] = None,
               raw_log: Optional[str] = None) -> None:
        rec = {
            "test": test,
            "tier": tier_label,
            "numjobs": numjobs,
            "repeat": repeat,
            "summary": summary,
            "raw_log": raw_log,
            "extra": extra or {},
            "wall_unix": time.time(),
        }
        records.append(rec)
        # Flush after each record so a crash mid-run loses at most one entry.
        with args.json_out.open("w") as fh:
            json.dump(
                {
                    "schema": 1,
                    "started_unix": run_started,
                    "tiers": [{"name": t.name, "path": str(t.path)} for t in tiers],
                    "records": records,
                },
                fh,
                indent=2,
            )

    def run_bandwidth_test(test_name: str, spec_fn) -> None:
        if test_name in skip:
            print(f"[skip] {test_name}")
            return
        for tier in tiers:
            for nj in numjobs_list:
                for rep in range(args.repeats):
                    label = f"{test_name}__{tier.name}__nj{nj}__r{rep}"
                    log_path = log_dir / f"{label}.json"
                    print(f"[run]  {label}")
                    spec = spec_fn(tier, nj, args.size, args.runtime, args.ramp)
                    payload = run_fio(fio_bin, spec, log_path)
                    summary = summarize_fio(payload)
                    record(test_name, tier.name, nj, rep, summary,
                           raw_log=str(log_path))
                    clean_tier_files(tier)

    # Fixed-concurrency tests
    def run_fsync_test() -> None:
        if "fsync" in skip:
            print("[skip] fsync_latency_4k")
            return
        for tier in tiers:
            for rep in range(args.repeats):
                label = f"fsync_latency_4k__{tier.name}__r{rep}"
                log_path = log_dir / f"{label}.json"
                print(f"[run]  {label}")
                spec = spec_fsync_latency(tier, args.runtime, args.ramp)
                payload = run_fio(fio_bin, spec, log_path)
                summary = summarize_fio(payload)
                record("fsync_latency_4k", tier.name, 1, rep, summary,
                       raw_log=str(log_path))
                clean_tier_files(tier)

    def run_metadata() -> None:
        if "metadata" in skip:
            print("[skip] metadata_create_unlink")
            return
        for tier in tiers:
            for rep in range(args.repeats):
                label = f"metadata_create_unlink__{tier.name}__r{rep}"
                print(f"[run]  {label}")
                result = run_metadata_test(tier, args.metadata_files, rep)
                # No fio summary; embed timings in extra and leave summary empty.
                record("metadata_create_unlink", tier.name, 1, rep,
                       summary={"njobs": 1}, extra=result)

    def run_cross_tier() -> None:
        if "cross_tier" in skip or len(tiers) < 2:
            if "cross_tier" in skip:
                print("[skip] cross_tier_copy")
            else:
                print("[skip] cross_tier_copy (need >= 2 tiers)")
            return
        for src in tiers:
            for dst in tiers:
                if src.name == dst.name:
                    continue
                pair = f"{src.name}__to__{dst.name}"
                for nj in numjobs_list:
                    for rep in range(args.repeats):
                        label = f"cross_tier_copy__{pair}__nj{nj}__r{rep}"
                        log_path = log_dir / f"{label}.json"
                        print(f"[run]  {label}")
                        spec = spec_cross_tier_copy(src, dst, nj, args.size,
                                                    args.runtime, args.ramp)
                        payload = run_fio(fio_bin, spec, log_path)
                        summary = summarize_fio(payload)
                        record("cross_tier_copy", pair, nj, rep, summary,
                               raw_log=str(log_path))
                        clean_tier_files(src)
                        clean_tier_files(dst)

    # Run order is chosen so that cheap tests run early and the long
    # concurrency sweeps do not block first-cut analysis if the job is
    # killed by a wall-clock limit.
    run_fsync_test()
    run_metadata()
    run_bandwidth_test("seq_write_1m", spec_seq_write)
    run_bandwidth_test("seq_read_1m", spec_seq_read)
    run_bandwidth_test("seq_rw_concurrent", spec_seq_rw_concurrent)
    run_bandwidth_test("mixed_random_64k", spec_mixed_random)
    run_cross_tier()

    print(f"[done] {len(records)} records written to {args.json_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
