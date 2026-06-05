#!/usr/bin/env python3
"""Download-only benchmark runner for FastBioDL comparison tools.

Each invocation benchmarks one tool on one accession list. It writes:
  - a per-second CSV with instantaneous throughput and active workers
  - a JSON summary for the run
  - a command/run log

The platform wrapper scripts drive the large/medium/small x repeat x tool loop.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import multiprocessing as mp
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import requests

from ncbi_lookup import get_ncbi_urls as shared_get_ncbi_urls


TOOLS = ("fastbiodl", "kingfisher", "pysradb", "sratools")


class RunState:
    """Small thread-safe state object sampled by the throughput logger."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.active_workers = 0
        self.active_connections = 0
        self.bytes_downloaded = 0

    def set_active(self, workers: int, connections: int) -> None:
        with self._lock:
            self.active_workers = max(0, int(workers))
            self.active_connections = max(0, int(connections))

    def add_bytes(self, count: int) -> None:
        with self._lock:
            self.bytes_downloaded += max(0, int(count))

    def snapshot(self) -> Tuple[int, int, int]:
        with self._lock:
            return self.bytes_downloaded, self.active_workers, self.active_connections


class ThroughputLogger:
    """Writes one throughput sample per interval until stopped."""

    def __init__(
        self,
        csv_path: Path,
        total_bytes_func: Callable[[], int],
        activity_func: Callable[[], Tuple[int, int]],
        configured_workers: int,
        interval_s: float = 1.0,
        throughput_logs: Optional[deque] = None,
        throughput_lock: Optional[threading.Lock] = None,
    ) -> None:
        self.csv_path = csv_path
        self.total_bytes_func = total_bytes_func
        self.activity_func = activity_func
        self.configured_workers = max(1, int(configured_workers))
        self.interval_s = max(0.2, float(interval_s))
        self.throughput_logs = throughput_logs
        self.throughput_lock = throughput_lock
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="throughput-logger", daemon=True)
        self._start_time = 0.0
        self._previous_time = 0.0
        self._previous_total = 0
        self._last_sample_elapsed = -1.0

    def start(self) -> None:
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        with self.csv_path.open("w", encoding="utf-8") as fh:
            fh.write(
                "timestamp_iso,timestamp_epoch,elapsed_sec,bytes_delta,total_bytes,"
                "current_mbps,avg_mbps,configured_workers,active_workers,active_connections\n"
            )
        self._start_time = time.time()
        self._previous_time = self._start_time
        self._previous_total = max(0, int(self.total_bytes_func()))
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_s * 2.0))
        self.sample(force=True)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            self.sample(force=False)

    def sample(self, force: bool) -> None:
        now = time.time()
        elapsed = now - self._start_time
        if not force and elapsed <= 0:
            return
        if force and self._last_sample_elapsed >= 0 and elapsed <= self._last_sample_elapsed + 0.05:
            return

        total = max(0, int(self.total_bytes_func()))
        delta = total - self._previous_total
        if delta < 0:
            delta = 0
        delta_time = max(0.001, now - self._previous_time)
        current_mbps = (delta * 8.0) / (delta_time * 1_000_000.0)
        avg_mbps = (total * 8.0) / (max(0.001, elapsed) * 1_000_000.0)
        active_workers, active_connections = self.activity_func()
        timestamp_iso = dt.datetime.fromtimestamp(now).isoformat(timespec="seconds")

        with self.csv_path.open("a", encoding="utf-8") as fh:
            fh.write(
                f"{timestamp_iso},{now:.3f},{elapsed:.3f},{delta},{total},"
                f"{current_mbps:.3f},{avg_mbps:.3f},{self.configured_workers},"
                f"{int(active_workers)},{int(active_connections)}\n"
            )

        if self.throughput_logs is not None and self.throughput_lock is not None:
            with self.throughput_lock:
                self.throughput_logs.append(round(current_mbps, 3))

        self._previous_total = total
        self._previous_time = now
        self._last_sample_elapsed = elapsed


def accession_group_name(input_path: Path) -> str:
    raw = input_path.stem or "accessions"
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in raw)
    return safe.strip("._-") or "accessions"


def read_accessions(input_path: Path) -> List[str]:
    with input_path.open(encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip()]


def human_bytes(num_bytes: int) -> str:
    value = float(max(0, int(num_bytes)))
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if value < 1024.0 or unit == "PiB":
            return f"{value:.2f}{unit}"
        value /= 1024.0
    return "0.00B"


def dir_size_bytes(path: Path) -> int:
    total = 0
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                    except OSError:
                        continue
        except OSError:
            continue
    return total


def get_ncbi_urls(acc: str, field: str, tool_name: str) -> List[Tuple[str, str]]:
    return shared_get_ncbi_urls(
        acc,
        field=field,
        max_attempts=5,
        backoff_base=1.0,
        timeout=30,
        max_rps=2.0,
        user_agent=f"benchmark-download-only-{tool_name}/1.0 (+https://github.com/)",
        tool_name=f"benchmark_download_only_{tool_name}",
        email=os.environ.get("NCBI_EMAIL", ""),
        api_key=os.environ.get("NCBI_API_KEY", ""),
        logger=logging,
    )


def fetch_urls_parallel(
    accessions: Iterable[str],
    field: str,
    tool_name: str,
    lookup_workers: int,
) -> Tuple[List[Tuple[str, str]], Dict[str, Dict[str, object]]]:
    accession_list = list(accessions)
    details: Dict[str, Dict[str, object]] = {
        acc: {"ok": False, "urls_found": 0, "reason": ""} for acc in accession_list
    }

    def _fetch(acc: str) -> Tuple[str, List[Tuple[str, str]], str]:
        try:
            return acc, get_ncbi_urls(acc, field, tool_name), ""
        except Exception as exc:
            return acc, [], str(exc)

    jobs: List[Tuple[str, str]] = []
    worker_count = max(1, min(len(accession_list) or 1, int(lookup_workers)))
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {pool.submit(_fetch, acc): acc for acc in accession_list}
        for fut in as_completed(futures):
            acc, url_pairs, err = fut.result()
            if err:
                details[acc].update({"ok": False, "reason": err})
                logging.error("URL fetch failed for %s: %s", acc, err)
                continue
            if not url_pairs:
                details[acc].update({"ok": False, "reason": "no_urls"})
                logging.warning("No URLs found for %s", acc)
                continue
            details[acc].update({"ok": True, "urls_found": len(url_pairs)})
            for url, source_acc in url_pairs:
                jobs.append((source_acc or acc, url))
    return jobs, details


def run_subprocess(cmd: List[str], timeout: int = 7200) -> Tuple[float, bool, str]:
    logging.info("$ %s", " ".join(cmd))
    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        elapsed = time.time() - t0
        stderr_tail = (proc.stderr or b"").decode(errors="replace").strip()[-1000:]
        stdout_tail = (proc.stdout or b"").decode(errors="replace").strip()[-1000:]
        if stdout_tail:
            logging.debug("stdout tail: %s", stdout_tail)
        if proc.returncode != 0:
            logging.error("Command failed rc=%s: %s", proc.returncode, stderr_tail)
        return elapsed, proc.returncode == 0, stderr_tail
    except subprocess.TimeoutExpired as exc:
        elapsed = time.time() - t0
        stderr_tail = ""
        if exc.stderr:
            stderr_tail = exc.stderr.decode(errors="replace").strip()[-1000:]
        logging.error("Command timed out after %.1fs: %s", elapsed, stderr_tail)
        return elapsed, False, "timeout"
    except FileNotFoundError as exc:
        elapsed = time.time() - t0
        logging.error("Command not found: %s", exc)
        return elapsed, False, str(exc)


def run_kingfisher(args: argparse.Namespace, accessions: List[str], logger: ThroughputLogger, state: RunState) -> Dict[str, object]:
    field = "fastq_ftp" if args.fastq else "sra_ftp"
    details: Dict[str, Dict[str, object]] = {}
    logger.start()
    try:
        for idx, acc in enumerate(accessions, start=1):
            logging.info("ACCESSION %s/%s: %s", idx, len(accessions), acc)
            acc_dir = args.sra_dir / acc
            shutil.rmtree(acc_dir, ignore_errors=True)
            acc_dir.mkdir(parents=True, exist_ok=True)

            t_lookup = time.time()
            try:
                url_pairs = get_ncbi_urls(acc, field, "kingfisher")
                lookup_elapsed = time.time() - t_lookup
            except Exception as exc:
                details[acc] = {
                    "ok": False,
                    "urls_found": 0,
                    "url_lookup_elapsed_s": round(time.time() - t_lookup, 3),
                    "reason": str(exc),
                    "files": [],
                }
                logging.error("URL fetch failed for %s: %s", acc, exc)
                continue

            files = []
            acc_ok = bool(url_pairs)
            for url, _source_acc in url_pairs:
                state.set_active(1, args.threads)
                elapsed, ok, stderr_tail = run_subprocess([
                    "aria2c",
                    "--continue=true",
                    f"--split={args.threads}",
                    f"--max-connection-per-server={args.threads}",
                    "--min-split-size=5M",
                    "--max-tries=3",
                    "--retry-wait=5",
                    f"--dir={acc_dir}",
                    url,
                ])
                state.set_active(0, 0)
                acc_ok = acc_ok and ok
                files.append({
                    "url": url,
                    "ok": ok,
                    "elapsed_s": round(elapsed, 3),
                    "stderr_tail": stderr_tail,
                })
            details[acc] = {
                "ok": acc_ok,
                "urls_found": len(url_pairs),
                "url_lookup_elapsed_s": round(lookup_elapsed, 3),
                "files": files,
            }
    finally:
        state.set_active(0, 0)
        logger.stop()
    return {"tool": "kingfisher", "details": details}


def run_pysradb(args: argparse.Namespace, accessions: List[str], logger: ThroughputLogger, state: RunState) -> Dict[str, object]:
    field = "fastq_ftp" if args.fastq else "sra_ftp"
    t_lookup = time.time()
    jobs, details = fetch_urls_parallel(accessions, field, "pysradb", args.lookup_workers)
    lookup_elapsed = time.time() - t_lookup
    download_details: Dict[str, Dict[str, object]] = {
        acc: {"ok": False, "files": [], "urls_found": details.get(acc, {}).get("urls_found", 0)}
        for acc in accessions
    }

    active_lock = threading.Lock()
    active_downloads = 0

    def _set_active(delta: int) -> None:
        nonlocal active_downloads
        with active_lock:
            active_downloads = max(0, active_downloads + delta)
            state.set_active(active_downloads, active_downloads)

    def _download_one(source_acc: str, url: str) -> Dict[str, object]:
        _set_active(1)
        t0 = time.time()
        filename = os.path.basename(url.split("?", 1)[0]) or source_acc
        out_dir = args.sra_dir / source_acc
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / filename
        bytes_written = 0
        try:
            with requests.get(url, stream=True, timeout=args.request_timeout) as resp:
                resp.raise_for_status()
                with out_path.open("wb") as fh:
                    for chunk in resp.iter_content(chunk_size=args.chunk_size):
                        if not chunk:
                            continue
                        fh.write(chunk)
                        chunk_len = len(chunk)
                        bytes_written += chunk_len
                        state.add_bytes(chunk_len)
            return {
                "acc": source_acc,
                "url": url,
                "path": str(out_path),
                "ok": True,
                "elapsed_s": round(time.time() - t0, 3),
                "bytes": bytes_written,
                "reason": "",
            }
        except Exception as exc:
            return {
                "acc": source_acc,
                "url": url,
                "path": str(out_path),
                "ok": False,
                "elapsed_s": round(time.time() - t0, 3),
                "bytes": bytes_written,
                "reason": str(exc),
            }
        finally:
            _set_active(-1)

    logger.start()
    try:
        if jobs:
            with ThreadPoolExecutor(max_workers=max(1, args.threads)) as pool:
                futures = {pool.submit(_download_one, acc, url): (acc, url) for acc, url in jobs}
                for fut in as_completed(futures):
                    result = fut.result()
                    acc = str(result["acc"])
                    download_details.setdefault(acc, {"ok": False, "files": [], "urls_found": 0})
                    download_details[acc]["files"].append(result)
                    if result["ok"]:
                        logging.info(
                            "Downloaded %s for %s in %.1fs",
                            os.path.basename(str(result["path"])),
                            acc,
                            float(result["elapsed_s"]),
                        )
                    else:
                        logging.error("Download failed for %s: %s", acc, result["reason"])
    finally:
        state.set_active(0, 0)
        logger.stop()

    for acc in accessions:
        files = download_details.get(acc, {}).get("files", [])
        download_details[acc]["ok"] = bool(files) and all(bool(item.get("ok")) for item in files)
        if acc in details and details[acc].get("reason"):
            download_details[acc]["reason"] = details[acc]["reason"]

    return {
        "tool": "pysradb",
        "url_lookup_elapsed_s": round(lookup_elapsed, 3),
        "details": download_details,
    }


def run_sratools(args: argparse.Namespace, accessions: List[str], logger: ThroughputLogger, state: RunState) -> Dict[str, object]:
    details: Dict[str, Dict[str, object]] = {}
    logger.start()
    try:
        for idx, acc in enumerate(accessions, start=1):
            logging.info("ACCESSION %s/%s: %s", idx, len(accessions), acc)
            state.set_active(1, 1)
            elapsed, ok, stderr_tail = run_subprocess([
                "prefetch",
                "--output-directory", str(args.sra_dir),
                "--max-size", args.prefetch_max_size,
                acc,
            ])
            state.set_active(0, 0)
            details[acc] = {
                "ok": ok,
                "elapsed_s": round(elapsed, 3),
                "stderr_tail": stderr_tail,
            }
    finally:
        state.set_active(0, 0)
        logger.stop()
    return {"tool": "sratools", "details": details}


def drain_queue_until_empty(source_queue: mp.Queue) -> List[str]:
    items: List[str] = []
    while True:
        try:
            item = source_queue.get_nowait()
        except queue.Empty:
            break
        except Exception:
            break
        if item is not None:
            items.append(str(item))
    return items


def configure_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    for handler in (logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)):
        handler.setFormatter(formatter)
        root.addHandler(handler)


def tool_result_ok(result: Dict[str, object]) -> bool:
    task_count = result.get("task_count")
    if task_count is not None:
        return (
            int(result.get("success_count", 0)) == int(task_count)
            and int(result.get("failed_count", 0)) == 0
        )

    details = result.get("details", {})
    if isinstance(details, dict) and details:
        return all(bool(item.get("ok")) for item in details.values() if isinstance(item, dict))
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one download-only benchmark and write per-second throughput logs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-i", "--input", required=True, type=Path)
    parser.add_argument("--tool", required=True, choices=TOOLS)
    parser.add_argument("--sra-dir", required=True, type=Path)
    parser.add_argument("--results-dir", required=True, type=Path)
    parser.add_argument("--platform", default="unknown")
    parser.add_argument("--accession-tag", default="")
    parser.add_argument("--run-index", type=int, default=1)
    parser.add_argument("--threads", type=int, default=8,
                        help="Tool worker/thread count for kingfisher and pysradb")
    parser.add_argument("--fastbiodl-workers", type=int, default=8,
                        help="FastBioDL downloader worker limit")
    parser.add_argument("--lookup-workers", type=int, default=3)
    parser.add_argument("--log-interval", type=float, default=1.0)
    parser.add_argument("--fastq", action="store_true",
                        help="Fetch fastq_ftp URLs instead of sra_ftp")
    parser.add_argument("--request-timeout", type=int, default=1200)
    parser.add_argument("--chunk-size", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--prefetch-max-size", default="100G")
    parser.add_argument("--segment-size-mb", type=int, default=512)
    parser.add_argument("--max-segments", type=int, default=8)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--probing-sec", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.input = args.input.resolve()
    args.sra_dir = args.sra_dir.resolve()
    args.results_dir = args.results_dir.resolve()

    if not args.input.exists():
        raise FileNotFoundError(f"Missing accession list: {args.input}")

    accession_tag = args.accession_tag or accession_group_name(args.input)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = args.results_dir / "download_only" / args.platform / accession_tag / args.tool
    csv_path = run_dir / f"run_{args.run_index:02d}_{ts}_throughput.csv"
    json_path = run_dir / f"run_{args.run_index:02d}_{ts}_summary.json"
    log_path = run_dir / f"run_{args.run_index:02d}_{ts}.log"

    configure_logging(log_path)
    args.sra_dir.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(parents=True, exist_ok=True)

    accessions = read_accessions(args.input)
    if not accessions:
        raise RuntimeError(f"No accessions found in {args.input}")

    logging.info("Download-only benchmark")
    logging.info("tool=%s platform=%s accession_tag=%s run=%s", args.tool, args.platform, accession_tag, args.run_index)
    logging.info("input=%s accessions=%s", args.input, len(accessions))
    logging.info("sra_dir=%s results_dir=%s", args.sra_dir, args.results_dir)
    logging.info("throughput_csv=%s", csv_path)

    state = RunState()
    configured_workers = args.fastbiodl_workers if args.tool == "fastbiodl" else args.threads
    if args.tool == "sratools":
        configured_workers = 1

    if args.tool == "pysradb":
        total_bytes_func = lambda: state.snapshot()[0]
        activity_func = lambda: state.snapshot()[1:]
    elif args.tool == "fastbiodl":
        total_bytes_func = lambda: 0
        activity_func = lambda: (0, 0)
    else:
        total_bytes_func = lambda: dir_size_bytes(args.sra_dir)
        activity_func = lambda: state.snapshot()[1:]

    throughput_logger = ThroughputLogger(
        csv_path=csv_path,
        total_bytes_func=total_bytes_func,
        activity_func=activity_func,
        configured_workers=configured_workers,
        interval_s=args.log_interval,
    )

    t_start = time.time()
    status = "ok"
    error = ""
    try:
        if args.tool == "fastbiodl":
            # Late-bind process counters inside a small wrapper so the logger can
            # read the real FastBioDL counters after they are created.
            result = run_fastbiodl_with_logger(args, accessions, throughput_logger)
        elif args.tool == "kingfisher":
            result = run_kingfisher(args, accessions, throughput_logger, state)
        elif args.tool == "pysradb":
            result = run_pysradb(args, accessions, throughput_logger, state)
        elif args.tool == "sratools":
            result = run_sratools(args, accessions, throughput_logger, state)
        else:
            raise RuntimeError(f"Unsupported tool: {args.tool}")
    except Exception as exc:
        status = "failed"
        error = str(exc)
        logging.exception("Download-only benchmark failed")
        result = {"tool": args.tool, "details": {}}

    if status == "ok" and not tool_result_ok(result):
        status = "failed"
        error = "one or more downloads failed"
        logging.error(error)

    t_end = time.time()
    final_bytes = dir_size_bytes(args.sra_dir)
    summary = {
        "status": status,
        "error": error,
        "tool": args.tool,
        "platform": args.platform,
        "accession_tag": accession_tag,
        "run_index": args.run_index,
        "input": str(args.input),
        "accessions": accessions,
        "sra_dir": str(args.sra_dir),
        "results_dir": str(args.results_dir),
        "throughput_csv": str(csv_path),
        "log_file": str(log_path),
        "t_start": t_start,
        "t_end": t_end,
        "download_wall_time_s": round(t_end - t_start, 3),
        "downloaded_dir_bytes": final_bytes,
        "downloaded_dir_human": human_bytes(final_bytes),
        "configured_workers": configured_workers,
        "tool_result": result,
    }
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    logging.info("Summary JSON: %s", json_path)
    logging.info("Final downloaded bytes in sra_dir: %s (%s bytes)", human_bytes(final_bytes), final_bytes)
    return 0 if status == "ok" else 1


def run_fastbiodl_with_logger(args: argparse.Namespace, accessions: List[str], logger: ThroughputLogger) -> Dict[str, object]:
    """Run FastBioDL and install logger callbacks after its counters exist."""
    try:
        import fastbiodl_upgrade as fb
    except Exception as exc:
        raise RuntimeError(f"Could not import FastBioDL downloader: {exc}") from exc

    try:
        mp.set_start_method("fork")
    except RuntimeError:
        pass

    fb.RUN_LOG_DIR = str(args.results_dir / "fastbiodl_internal_logs")
    Path(fb.RUN_LOG_DIR).mkdir(parents=True, exist_ok=True)
    fb.configurations["cpu_count"] = mp.cpu_count()
    fb.configurations["thread_limit"] = max(1, int(args.fastbiodl_workers))
    fb.configurations["segment_size"] = max(1, int(args.segment_size_mb)) * 1024 * 1024
    fb.configurations["max_segments"] = max(1, int(args.max_segments))
    fb.configurations["max_retries"] = max(0, int(args.max_retries))
    fb.configurations["probing_sec"] = max(1, int(args.probing_sec))
    fb.probing_time = fb.configurations.get("probing_sec", 5)

    field = "fastq_ftp" if args.fastq else "sra_ftp"
    t_lookup = time.time()
    jobs, lookup_details = fetch_urls_parallel(accessions, field, "fastbiodl", args.lookup_workers)
    lookup_elapsed = time.time() - t_lookup

    download_queue: mp.JoinableQueue = mp.JoinableQueue()
    failed_queue: mp.Queue = mp.Queue()
    processing_queue: mp.Queue = mp.Queue()

    for source_acc, url in jobs:
        filename = os.path.basename(url.split("?", 1)[0]) or source_acc
        download_queue.put((url, os.path.join(source_acc, filename), 0))

    initial_task_count = len(jobs)
    if initial_task_count == 0:
        raise RuntimeError("No files to download")

    num_workers = min(initial_task_count, max(1, int(args.fastbiodl_workers)))
    initial_active = min(2, num_workers)

    fb.download_dir = str(args.sra_dir)
    fb.download_complete = mp.Value("i", 0)
    fb.failed_count = mp.Value("i", 0)
    fb.transfer_done = mp.Value("i", 0)
    fb.active_connections = mp.Value("i", 0)
    fb.files_to_download = mp.Value("i", initial_task_count)
    fb.download_process_status = mp.Array("i", [1 if i < initial_active else 0 for i in range(num_workers)])
    fb.start = mp.Value("d", 0.0)

    process_counters = [mp.Value("Q", 0) for _ in range(num_workers)]
    shared_disk_reserved_bytes = mp.Value("Q", 0)
    shared_min_pending_conversion_bytes = mp.Value("Q", 0)
    disk_safety_margin_bytes = int(
        float(fb.configurations.get("download_disk_safety_margin_gb", 2.0)) * 1024 * 1024 * 1024
    )

    logger.total_bytes_func = lambda: sum(counter.value for counter in process_counters)
    logger.activity_func = lambda: (sum(fb.download_process_status), fb.active_connections.value)

    workers = [
        mp.Process(
            target=fb.download_file_worker,
            args=(
                idx,
                download_queue,
                failed_queue,
                fb.failed_count,
                process_counters[idx],
                fb.active_connections,
                shared_disk_reserved_bytes,
                shared_min_pending_conversion_bytes,
                disk_safety_margin_bytes,
                processing_queue,
            ),
        )
        for idx in range(num_workers)
    ]

    throughput_logs: deque = deque(maxlen=10000)
    throughput_lock = threading.Lock()
    logger.throughput_logs = throughput_logs
    logger.throughput_lock = throughput_lock

    fb.start.value = time.time()
    logger.start()
    for proc in workers:
        proc.daemon = True
        proc.start()

    optimizer = threading.Thread(
        target=fb.run_download_optimizer,
        args=(fb.download_probing, throughput_logs, throughput_lock),
        name="fastbiodl-download-optimizer",
        daemon=True,
    )
    optimizer.start()

    try:
        while (fb.download_complete.value + fb.failed_count.value) < initial_task_count and fb.transfer_done.value == 0:
            time.sleep(0.5)
    finally:
        fb.transfer_done.value = 1
        logger.stop()

    optimizer.join(timeout=max(2.0, float(fb.probing_time) * 2.0))
    for proc in workers:
        proc.join(timeout=10.0)
        if proc.is_alive():
            logging.warning("Terminating stuck FastBioDL worker pid=%s", proc.pid)
            proc.terminate()
            proc.join(timeout=5.0)

    queued_paths = drain_queue_until_empty(processing_queue)
    failed = drain_queue_until_empty(failed_queue)
    details: Dict[str, Dict[str, object]] = {
        acc: {
            "ok": bool(lookup_details.get(acc, {}).get("ok")),
            "urls_found": lookup_details.get(acc, {}).get("urls_found", 0),
            "reason": lookup_details.get(acc, {}).get("reason", ""),
        }
        for acc in accessions
    }
    return {
        "tool": "fastbiodl",
        "url_lookup_elapsed_s": round(lookup_elapsed, 3),
        "task_count": initial_task_count,
        "success_count": int(fb.download_complete.value),
        "failed_count": int(fb.failed_count.value),
        "queued_download_paths": queued_paths,
        "failed_downloads": failed,
        "details": details,
    }


if __name__ == "__main__":
    sys.exit(main())
