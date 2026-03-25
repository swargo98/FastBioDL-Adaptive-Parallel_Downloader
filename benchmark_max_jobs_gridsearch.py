#!/usr/bin/env python3
"""
benchmark_max_jobs_gridsearch.py

Grid-search benchmark for FastBioDL conversion concurrency knobs:
- max_conversion_jobs in {1, 2, 3}
- max_pigz_jobs in {1, 2, 3}

Workflow
--------
1. Download one accession using FastBioDL segmented downloader.
2. Run a single-file warm-up conversion (converter + pigz pipeline).
3. For each (max_conversion_jobs, max_pigz_jobs) pair:
   - Materialize 3 copies of the downloaded .sra file.
   - Run SRAConverter so fasterq-dump and pigz overlap concurrently.
   - Record total wall time and phase overlap metrics.
4. Report the best pair and save a JSON report.
5. Optionally clean the RAID0 work directory.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def resolve_tool(binary: str, script_dir: Path) -> str:
    """Resolve tool from PATH, then fallback to bundled sratoolkit in repo."""
    in_path = shutil.which(binary)
    if in_path:
        return in_path

    bundled = script_dir / "sratoolkit.3.1.0-ubuntu64" / "bin" / binary
    if bundled.exists() and os.access(bundled, os.X_OK):
        return str(bundled)

    raise RuntimeError(
        f"Required command not found: {binary}. Tried PATH and {bundled}"
    )


def prepare_tool_path(script_dir: Path) -> None:
    """Ensure bundled fasterq-dump is reachable by child worker processes."""
    bundled_bin = script_dir / "sratoolkit.3.1.0-ubuntu64" / "bin"
    if bundled_bin.exists() and shutil.which("fasterq-dump") is None:
        os.environ["PATH"] = f"{bundled_bin}:{os.environ.get('PATH', '')}"


def download_with_fastbiodl(
    accession: str,
    sra_dir: Path,
    segment_size_mb: int,
    max_segments: int,
    max_retries: int,
) -> Tuple[Path, float, str]:
    """Download one accession using FastBioDL segmented downloader."""
    try:
        import aiohttp
        import fastbiodl_upgrade as fb
    except Exception as exc:
        raise RuntimeError(f"FastBioDL downloader import failed: {exc}") from exc

    url_acc_pairs = fb.get_ncbi_urls(accession, field="sra_ftp")
    if not url_acc_pairs:
        raise RuntimeError(f"No SRA download URL found for {accession}")

    url, source_acc = url_acc_pairs[0]
    filename = os.path.basename(url)
    local_path = sra_dir / source_acc / filename
    local_path.parent.mkdir(parents=True, exist_ok=True)

    fb.download_dir = str(sra_dir)
    fb.transfer_done = mp.Value("i", 0)
    fb.download_process_status = mp.Array("i", [1])

    process_counter = mp.Value("Q", 0)
    active_connections = mp.Value("i", 0)
    disk_reserved_bytes = mp.Value("Q", 0)
    min_pending_conversion_bytes = mp.Value("Q", 0)

    async def _download_once() -> Tuple[bool, bool, int]:
        timeout = aiohttp.ClientTimeout(total=3600, connect=60, sock_read=300)
        connector = aiohttp.TCPConnector(
            limit=max_segments,
            limit_per_host=max_segments,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": "fastbiodl/3.0"},
        ) as session:
            downloader = fb.SegmentedDownloader(
                session=session,
                url=url,
                local_path=str(local_path),
                segment_size=max(1, int(segment_size_mb)) * 1024 * 1024,
                max_segments=max(1, int(max_segments)),
                process_id=0,
                process_counter=process_counter,
                active_connections=active_connections,
                disk_reserved_bytes=disk_reserved_bytes,
                min_pending_conversion_bytes=min_pending_conversion_bytes,
                disk_safety_margin_bytes=0,
                max_retries=max(0, int(max_retries)),
            )
            return await downloader.download_with_resume()

    t0 = time.perf_counter()
    success, was_paused, _connections = asyncio.run(_download_once())
    elapsed = time.perf_counter() - t0

    if not success:
        state = "paused" if was_paused else "failed"
        raise RuntimeError(f"FastBioDL downloader {state} for {accession} ({url})")

    if not local_path.exists():
        raise RuntimeError(f"FastBioDL reported success but file missing: {local_path}")

    return local_path, elapsed, url


def find_sra_file(sra_dir: Path, accession: str) -> Optional[Path]:
    """Find downloaded SRA with common naming variants."""
    candidates = [
        sra_dir / accession,
        sra_dir / f"{accession}.sra",
        sra_dir / f"{accession}.sralite.1",
        sra_dir / f"{accession}.sralite.2",
    ]
    for c in candidates:
        if c.exists() and c.is_file():
            return c

    for p in sorted(sra_dir.rglob(f"{accession}*")):
        if p.is_file() and (p.suffix == ".sra" or p.name.startswith(accession)):
            return p
    return None


def clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def prepare_three_inputs(master_sra: Path, input_dir: Path, accession: str) -> List[Path]:
    """Create three independent SRA inputs (original + two copies) for one run."""
    clean_dir(input_dir)
    suffix = "".join(master_sra.suffixes) or ".sra"
    copies = [
        input_dir / f"{accession}_copy1{suffix}",
        input_dir / f"{accession}_copy2{suffix}",
        input_dir / f"{accession}_copy3{suffix}",
    ]
    for dst in copies:
        shutil.copy2(master_sra, dst)
    return copies


def queue_to_list(q: mp.Queue) -> List[str]:
    out: List[str] = []
    while True:
        try:
            item = q.get_nowait()
        except Exception:
            break
        if item is None:
            continue
        out.append(str(item))
    return sorted(out)


def run_cmd(cmd: List[str], timeout_s: int = 0) -> Tuple[int, float, str, str]:
    """Run a command and return (rc, elapsed_s, stdout, stderr)."""
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=(timeout_s if timeout_s > 0 else None),
        )
        return proc.returncode, time.perf_counter() - t0, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        elapsed = time.perf_counter() - t0
        return 124, elapsed, exc.stdout or "", (exc.stderr or "") + "\nTIMEOUT"


def run_strict_sequential_baseline(
    inputs: List[Path],
    work_dir: Path,
    fasterq_cmd: str,
    pigz_cmd: str,
    threads: int,
    pigz_threads: int,
) -> Dict[str, object]:
    """
    Run a strict non-overlapped baseline.

    For each SRA input: fasterq-dump completes first, then pigz compresses all
    produced FASTQ files, then move to the next SRA input.
    """
    clean_dir(work_dir)
    fastq_root = work_dir / "fastq"
    tmp_root = work_dir / "tmp"
    fastq_root.mkdir(parents=True, exist_ok=True)
    tmp_root.mkdir(parents=True, exist_ok=True)

    converted = 0
    failed = 0
    gz_outputs: List[str] = []
    per_input: List[Dict[str, object]] = []

    t0 = time.perf_counter()
    for idx, sra_path in enumerate(inputs, start=1):
        out_dir = fastq_root / f"job_{idx}"
        temp_dir = tmp_root / f"job_{idx}"
        out_dir.mkdir(parents=True, exist_ok=True)
        temp_dir.mkdir(parents=True, exist_ok=True)

        rc_fq, fq_s, _out_fq, err_fq = run_cmd(
            [
                fasterq_cmd,
                "--threads",
                str(max(1, int(threads))),
                "--temp",
                str(temp_dir),
                "--outdir",
                str(out_dir),
                "--split-3",
                "--skip-technical",
                str(sra_path),
            ],
            timeout_s=7200,
        )
        if rc_fq != 0:
            failed += 1
            per_input.append(
                {
                    "input": str(sra_path),
                    "status": "failed_fasterq",
                    "fasterq_s": round(fq_s, 3),
                    "error_tail": err_fq.strip()[-500:],
                }
            )
            continue

        fastq_files = sorted(out_dir.glob("*.fastq"))
        if not fastq_files:
            failed += 1
            per_input.append(
                {
                    "input": str(sra_path),
                    "status": "failed_no_fastq",
                    "fasterq_s": round(fq_s, 3),
                    "error_tail": "No FASTQ files produced",
                }
            )
            continue

        pigz_s_total = 0.0
        pigz_ok = True
        pigz_err_tail = ""
        for fq in fastq_files:
            rc_pz, pz_s, _out_pz, err_pz = run_cmd(
                [pigz_cmd, "-1", "-p", str(max(1, int(pigz_threads))), str(fq)],
                timeout_s=3600,
            )
            pigz_s_total += pz_s
            if rc_pz != 0:
                pigz_ok = False
                pigz_err_tail = err_pz.strip()[-500:]
                break

        if not pigz_ok:
            failed += 1
            per_input.append(
                {
                    "input": str(sra_path),
                    "status": "failed_pigz",
                    "fasterq_s": round(fq_s, 3),
                    "pigz_s": round(pigz_s_total, 3),
                    "error_tail": pigz_err_tail,
                }
            )
            continue

        produced = sorted(str(p) for p in out_dir.glob("*.fastq.gz"))
        gz_outputs.extend(produced)
        converted += 1
        per_input.append(
            {
                "input": str(sra_path),
                "status": "ok",
                "fasterq_s": round(fq_s, 3),
                "pigz_s": round(pigz_s_total, 3),
                "gz_outputs": produced,
            }
        )

    total_s = time.perf_counter() - t0
    return {
        "mode": "strict_sequential_no_overlap",
        "total_s": round(total_s, 3),
        "converted_count": int(converted),
        "failed_count": int(failed),
        "moved_outputs_count": len(gz_outputs),
        "moved_outputs": sorted(gz_outputs),
        "overlap_s": 0.0,
        "per_input": per_input,
    }


def run_converter_once(
    inputs: List[Path],
    work_dir: Path,
    threads: int,
    max_conversion_jobs: int,
    max_pigz_jobs: int,
    probe_sec: float,
    cpu_threshold: float,
    nvme_threshold: float,
    required_factor: float,
    reserve_factor: float,
    pigz_reserve_factor: float,
    disk_safety_margin_gb: float,
    nvme_device: str,
    stop_timeout_s: float,
) -> Dict[str, object]:
    from converter import SRAConverter

    clean_dir(work_dir)
    processing_queue = mp.Queue()
    move_queue = mp.Queue()

    shared_reserved = mp.Value("Q", 0)
    shared_pending = mp.Value("Q", 0)

    converter = SRAConverter(
        processing_queue=processing_queue,
        move_queue=move_queue,
        work_dir=str(work_dir),
        nvme_device=nvme_device,
        threads_per_job=max(1, int(threads)),
        cpu_threshold=float(cpu_threshold),
        nvme_threshold=float(nvme_threshold),
        max_jobs=max(1, int(max_conversion_jobs)),
        max_pigz_jobs=max(1, int(max_pigz_jobs)),
        required_size_factor=float(required_factor),
        reserve_size_factor=float(reserve_factor),
        pigz_reserve_factor=float(pigz_reserve_factor),
        disk_safety_margin_gb=float(disk_safety_margin_gb),
        shared_reserved_bytes=shared_reserved,
        shared_pending_headroom_bytes=shared_pending,
        probing_sec=float(probe_sec),
    )

    for p in inputs:
        processing_queue.put(str(p))
    processing_queue.put(None)

    t0 = time.perf_counter()
    converter.start()
    converter.stop(timeout=float(stop_timeout_s))
    total_s = time.perf_counter() - t0

    moved_outputs = queue_to_list(move_queue)

    fq_start = float(converter.t_first_fasterq_start)
    fq_end = float(converter.t_last_fasterq_done)
    pz_start = float(converter.t_first_pigz_start)
    pz_end = float(converter.t_last_pigz_done)

    fasterq_span_s = max(0.0, fq_end - fq_start) if (fq_start > 0.0 and fq_end > 0.0) else 0.0
    pigz_span_s = max(0.0, pz_end - pz_start) if (pz_start > 0.0 and pz_end > 0.0) else 0.0
    overlap_s = 0.0
    if fq_start > 0.0 and fq_end > 0.0 and pz_start > 0.0 and pz_end > 0.0:
        overlap_s = max(0.0, min(fq_end, pz_end) - max(fq_start, pz_start))

    return {
        "total_s": round(total_s, 3),
        "converted_count": int(converter.converted_count),
        "failed_count": int(converter.failed_count),
        "moved_outputs_count": len(moved_outputs),
        "moved_outputs": moved_outputs,
        "fasterq_span_s": round(fasterq_span_s, 3),
        "pigz_span_s": round(pigz_span_s, 3),
        "overlap_s": round(overlap_s, 3),
    }


def print_grid_summary(results: List[Dict[str, object]]) -> None:
    print("\nGrid results (lower total_s is better):")
    print("conv_jobs pigz_jobs total_s converted failed overlap_s")
    for row in results:
        print(
            f"{row['max_conversion_jobs']:>9} {row['max_pigz_jobs']:>9} "
            f"{row['total_s']:>7.3f} {row['converted_count']:>9} "
            f"{row['failed_count']:>6} {row['overlap_s']:>8.3f}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Grid search benchmark for max_conversion_jobs and max_pigz_jobs (1..3)"
    )
    parser.add_argument("accession", help="Single accession (e.g., SRR390728)")
    parser.add_argument(
        "--work-root",
        default="/mnt/raid0/fastbiodl_gridsearch",
        help="RAID0 working directory used during benchmark",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="Optional explicit JSON report output path",
    )
    parser.add_argument("--threads", type=int, default=8, help="threads_per_job for converter workers")
    parser.add_argument("--nvme-device", default="md0", help="Device name for AdmissionGate")
    parser.add_argument("--probe-sec", type=float, default=1.0, help="Admission probe interval")
    parser.add_argument("--cpu-threshold", type=float, default=85.0, help="Admission CPU ceiling")
    parser.add_argument("--nvme-threshold", type=float, default=92.0, help="Admission NVMe ceiling")
    parser.add_argument("--required-factor", type=float, default=10.0, help="Converter required size factor")
    parser.add_argument("--reserve-factor", type=float, default=10.5, help="Converter reservation factor")
    parser.add_argument("--pigz-reserve-factor", type=float, default=3.5, help="Converter pigz reserve factor")
    parser.add_argument("--disk-safety-margin-gb", type=float, default=0.0, help="Converter disk safety margin")
    parser.add_argument("--stop-timeout", type=float, default=7200.0, help="SRAConverter stop timeout (seconds)")
    parser.add_argument("--download-segment-size-mb", type=int, default=512, help="FastBioDL segment size MB")
    parser.add_argument("--download-max-segments", type=int, default=8, help="FastBioDL max segments")
    parser.add_argument("--download-max-retries", type=int, default=3, help="FastBioDL max retries")
    parser.add_argument(
        "--cleanup-work-root",
        action="store_true",
        help="Delete --work-root after benchmark finishes",
    )
    args = parser.parse_args()

    accession = args.accession.strip()
    if not accession:
        print("Accession is required", file=sys.stderr)
        return 2

    script_dir = Path(__file__).resolve().parent
    prepare_tool_path(script_dir)

    # Validate required tools early.
    try:
        fasterq_cmd = resolve_tool("fasterq-dump", script_dir)
        pigz_cmd = resolve_tool("pigz", script_dir)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    work_root = Path(args.work_root).resolve()
    download_dir = work_root / "download"
    warmup_dir = work_root / "warmup"
    runs_dir = work_root / "runs"

    clean_dir(work_root)
    download_dir.mkdir(parents=True, exist_ok=True)
    warmup_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)

    print(f"[1/6] Downloading accession with FastBioDL: {accession}")
    try:
        downloaded_sra, dl_s, source_url = download_with_fastbiodl(
            accession=accession,
            sra_dir=download_dir,
            segment_size_mb=args.download_segment_size_mb,
            max_segments=args.download_max_segments,
            max_retries=args.download_max_retries,
        )
    except Exception as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        return 1

    if not downloaded_sra.exists():
        found = find_sra_file(download_dir, accession)
        if found is None:
            print(f"Could not locate downloaded SRA under {download_dir}", file=sys.stderr)
            return 1
        downloaded_sra = found

    sra_size = downloaded_sra.stat().st_size
    print(
        f"Downloaded: {downloaded_sra} ({sra_size / (1024 ** 3):.3f} GB) in {dl_s:.2f}s\n"
        f"Source URL: {source_url}"
    )

    print("[2/6] Warm-up conversion (single input; converter + pigz overlap path)")
    warmup_input_dir = warmup_dir / "input"
    warmup_inputs = prepare_three_inputs(downloaded_sra, warmup_input_dir, accession)[:1]
    warmup_result = run_converter_once(
        inputs=warmup_inputs,
        work_dir=warmup_dir / "work",
        threads=args.threads,
        max_conversion_jobs=1,
        max_pigz_jobs=1,
        probe_sec=args.probe_sec,
        cpu_threshold=args.cpu_threshold,
        nvme_threshold=args.nvme_threshold,
        required_factor=args.required_factor,
        reserve_factor=args.reserve_factor,
        pigz_reserve_factor=args.pigz_reserve_factor,
        disk_safety_margin_gb=args.disk_safety_margin_gb,
        nvme_device=args.nvme_device,
        stop_timeout_s=args.stop_timeout,
    )
    if warmup_result["failed_count"] != 0 or warmup_result["converted_count"] != 1:
        print(f"Warm-up conversion failed: {warmup_result}", file=sys.stderr)
        return 1

    print("[3/6] Running strict sequential baseline (no overlap; analogous to (1,1) without concurrency overlap)")
    seq_root = work_root / "sequential_baseline"
    seq_input_dir = seq_root / "input"
    seq_work_dir = seq_root / "work"
    seq_inputs = prepare_three_inputs(downloaded_sra, seq_input_dir, accession)
    sequential_result = run_strict_sequential_baseline(
        inputs=seq_inputs,
        work_dir=seq_work_dir,
        fasterq_cmd=fasterq_cmd,
        pigz_cmd=pigz_cmd,
        threads=args.threads,
        pigz_threads=args.threads,
    )
    if sequential_result["failed_count"] != 0:
        print(f"Sequential baseline had failures: {sequential_result}", file=sys.stderr)
        return 1

    print("[4/6] Preparing 3-copy datasets and running 3x3 grid search")
    results: List[Dict[str, object]] = []
    for max_conv in (1, 2, 3):
        for max_pigz in (1, 2, 3):
            run_id = f"c{max_conv}_p{max_pigz}"
            print(f"  - Running {run_id}")
            run_root = runs_dir / run_id
            input_dir = run_root / "input"
            work_dir = run_root / "work"

            run_inputs = prepare_three_inputs(downloaded_sra, input_dir, accession)
            try:
                run_result = run_converter_once(
                    inputs=run_inputs,
                    work_dir=work_dir,
                    threads=args.threads,
                    max_conversion_jobs=max_conv,
                    max_pigz_jobs=max_pigz,
                    probe_sec=args.probe_sec,
                    cpu_threshold=args.cpu_threshold,
                    nvme_threshold=args.nvme_threshold,
                    required_factor=args.required_factor,
                    reserve_factor=args.reserve_factor,
                    pigz_reserve_factor=args.pigz_reserve_factor,
                    disk_safety_margin_gb=args.disk_safety_margin_gb,
                    nvme_device=args.nvme_device,
                    stop_timeout_s=args.stop_timeout,
                )
                row: Dict[str, object] = {
                    "max_conversion_jobs": max_conv,
                    "max_pigz_jobs": max_pigz,
                    **run_result,
                    "status": "ok",
                }
            except Exception as exc:
                row = {
                    "max_conversion_jobs": max_conv,
                    "max_pigz_jobs": max_pigz,
                    "status": "failed",
                    "error": str(exc),
                    "total_s": float("inf"),
                    "converted_count": 0,
                    "failed_count": 3,
                    "moved_outputs_count": 0,
                    "moved_outputs": [],
                    "fasterq_span_s": 0.0,
                    "pigz_span_s": 0.0,
                    "overlap_s": 0.0,
                }
            results.append(row)

    successful = [r for r in results if r.get("status") == "ok" and int(r.get("failed_count", 1)) == 0]
    if not successful:
        print("No successful grid-search runs. See JSON report for details.", file=sys.stderr)
        return 1

    best = min(successful, key=lambda r: float(r["total_s"]))

    print("[5/6] Result summary")
    print(
        "Sequential baseline total: "
        f"{float(sequential_result['total_s']):.3f}s "
        f"(converted={sequential_result['converted_count']}, failed={sequential_result['failed_count']})"
    )
    print_grid_summary(successful)
    print(
        "\nBest pair: "
        f"max_conversion_jobs={best['max_conversion_jobs']}, "
        f"max_pigz_jobs={best['max_pigz_jobs']} "
        f"(total={float(best['total_s']):.3f}s)"
    )

    report = {
        "accession": accession,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "download": {
            "elapsed_s": round(dl_s, 3),
            "source_url": source_url,
            "sra_path": str(downloaded_sra),
            "sra_size_bytes": sra_size,
        },
        "warmup": warmup_result,
        "sequential_baseline": sequential_result,
        "grid": results,
        "best": best,
        "search_space": {
            "max_conversion_jobs": [1, 2, 3],
            "max_pigz_jobs": [1, 2, 3],
            "copies_per_run": 3,
        },
    }

    out_json = Path(args.json_out).resolve() if args.json_out else (
        Path(__file__).resolve().parent / "benchmark" / f"benchmark_max_jobs_{accession}.json"
    )
    out_json.parent.mkdir(parents=True, exist_ok=True)
    with out_json.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"JSON report: {out_json}")

    if args.cleanup_work_root:
        print(f"[6/6] Cleaning RAID0 work directory: {work_root}")
        shutil.rmtree(work_root, ignore_errors=True)
    else:
        print(f"[6/6] Keeping RAID0 work directory: {work_root}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
