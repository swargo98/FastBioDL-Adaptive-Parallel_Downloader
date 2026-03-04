#!/usr/bin/env python3
"""
converter.py — SRA to FASTQ conversion stage for the fastbiodl pipeline.

Architecture:
  - SRAConverter class owns all state and worker lifecycle
  - AdmissionGate polls /proc/stat (CPU) and /proc/diskstats (NVMe) before
    starting each new fasterq-dump job
  - Workers are subprocesses running fasterq-dump + pigz compression
  - Completed fastq.gz paths are pushed to move_queue for receiver.py

Usage (from fastbiodl_upgrade.py):
    from converter import SRAConverter
    converter = SRAConverter(
        processing_queue=processing_queue,
        move_queue=move_queue,
        work_dir="/mnt/nvme0n1/fastbiodl/",
        nvme_device="nvme0n1",
        threads_per_job=4,
        cpu_threshold=85.0,
        nvme_threshold=80.0,
        max_jobs=None,        # None = cpu_count (soft sanity ceiling only)
        probing_sec=5,
    )
    converter.start()
    # ... wait for download stage to finish ...
    converter.stop()

Fixes applied
─────────────
[My #1]  _worker_procs list is now protected by _procs_lock (Lock) to
         eliminate the dispatcher/collector race on append vs. iterate+reassign.

[My #3]  _dispatcher_loop skips (with a warning) any path that does not end
         in .sra so non-SRA files from --fastq mode do not reach fasterq-dump.

[My #5]  _result_collector_loop drains the result queue once the while-loop
         exits so results that arrive in the stop-race window are not dropped.

[My #8]  _conversion_worker cleans up acc_fastq_dir on partial pigz failure
         so orphan .fastq.gz files do not accumulate on disk.

[Img #2] stop() now waits for _active_jobs to reach 0 (up to timeout) before
         terminating worker procs, so no in-flight conversion is killed mid-run.

[Img #3] Bare `except Exception` replaced with `except queue.Empty` in
         _dispatcher_loop and _result_collector_loop so real errors surface.

[Img #5] Worker processes launched with daemon=False so the OS does not tear
         them down if the parent thread exits unexpectedly mid-conversion.
"""

import os
import queue
import time
import logging
import subprocess
import datetime
import multiprocessing as mp
from threading import Thread, Lock
from collections import deque
from typing import Optional


#############################
# System metric helpers
#############################

def _read_cpu_times():
    """
    Read aggregate CPU times from /proc/stat.
    Returns (idle, total) jiffies as a tuple.
    """
    with open("/proc/stat") as f:
        line = f.readline()  # first line: cpu <user> <nice> <system> <idle> <iowait> ...
    fields = line.split()
    values = [int(x) for x in fields[1:]]
    idle  = values[3] + values[4]   # idle + iowait
    total = sum(values)
    return idle, total


def cpu_utilization_pct(prev_idle: int, prev_total: int) -> tuple:
    """
    Compute CPU utilization % since last call.
    Returns (util_pct, new_idle, new_total).
    """
    idle, total = _read_cpu_times()
    d_idle  = idle  - prev_idle
    d_total = total - prev_total
    util = 100.0 * (1.0 - d_idle / d_total) if d_total > 0 else 0.0
    return round(util, 1), idle, total


def _read_diskstats(device: str) -> Optional[dict]:
    """
    Parse /proc/diskstats for a given device name (e.g. 'nvme0n1').
    Returns dict with io_in_progress and ms_doing_io, or None if not found.
    """
    with open("/proc/diskstats") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 14:
                continue
            if parts[2] == device:
                return {
                    "reads_completed":  int(parts[3]),
                    "writes_completed": int(parts[7]),
                    "ms_reading":       int(parts[6]),
                    "ms_writing":       int(parts[10]),
                    "io_in_progress":   int(parts[11]),
                    "ms_doing_io":      int(parts[12]),
                }
    return None


def nvme_utilization_pct(device: str, prev_ms: int, interval_sec: float) -> tuple:
    """
    Compute NVMe utilization % over an interval.
    Utilization = (ms_doing_io delta) / (interval_ms) * 100.
    Returns (util_pct, new_ms_doing_io).
    """
    stats = _read_diskstats(device)
    if stats is None:
        return 0.0, prev_ms
    new_ms = stats["ms_doing_io"]
    delta_ms = new_ms - prev_ms
    interval_ms = interval_sec * 1000.0
    util = min(100.0, round(100.0 * delta_ms / interval_ms, 1)) if interval_ms > 0 else 0.0
    return util, new_ms


#############################
# Admission Gate
#############################

class AdmissionGate:
    """
    Polls CPU and NVMe utilization.
    admit() blocks until both are below their thresholds.
    """

    def __init__(
        self,
        nvme_device: str,
        cpu_threshold: float = 85.0,
        nvme_threshold: float = 80.0,
        poll_interval: float = 1.0,
    ):
        self.nvme_device    = nvme_device
        self.cpu_threshold  = cpu_threshold
        self.nvme_threshold = nvme_threshold
        self.poll_interval  = poll_interval

        # Seed initial readings
        self._cpu_idle, self._cpu_total = _read_cpu_times()
        stats = _read_diskstats(nvme_device)
        self._nvme_ms = stats["ms_doing_io"] if stats else 0

        # Expose last-observed metrics for logging
        self.last_cpu_util  = 0.0
        self.last_nvme_util = 0.0

    def admit(self, stop_event=None) -> bool:
        """
        Block until CPU < cpu_threshold AND NVMe < nvme_threshold.
        Returns True when admission is granted, False if stop_event is set.
        """
        while True:
            if stop_event and stop_event.is_set():
                return False

            time.sleep(self.poll_interval)

            cpu_util, self._cpu_idle, self._cpu_total = cpu_utilization_pct(
                self._cpu_idle, self._cpu_total
            )
            nvme_util, self._nvme_ms = nvme_utilization_pct(
                self.nvme_device, self._nvme_ms, self.poll_interval
            )

            self.last_cpu_util  = cpu_util
            self.last_nvme_util = nvme_util

            if cpu_util < self.cpu_threshold and nvme_util < self.nvme_threshold:
                return True

            logging.debug(
                f"[AdmissionGate] Waiting — CPU: {cpu_util}%, "
                f"NVMe: {nvme_util}% (thresholds: {self.cpu_threshold}% / {self.nvme_threshold}%)"
            )


#############################
# Per-job worker
#############################

def _cleanup_dir(path: str):
    """Remove a directory and all its contents, silently ignoring errors."""
    import shutil
    try:
        shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass


def _conversion_worker(
    job_id: int,
    sra_path: str,
    fastq_dir: str,
    temp_dir: str,
    threads: int,
    result_queue: mp.Queue,
    byte_counter: mp.Value,
):
    """
    Runs fasterq-dump on one .sra file, then compresses output with pigz.
    Pushes (sra_path, [fastq_gz_paths], success) to result_queue when done.
    Increments byte_counter as output files grow (polled during compression).
    """
    accession = os.path.splitext(os.path.basename(sra_path))[0]
    acc_fastq_dir = os.path.join(fastq_dir, accession)
    # Each job gets its own isolated temp directory — prevents filename
    # collisions between concurrent fasterq-dump processes.
    job_temp_dir = os.path.join(temp_dir, f"{accession}_{job_id}")
    os.makedirs(acc_fastq_dir, exist_ok=True)
    os.makedirs(job_temp_dir, exist_ok=True)

    import shutil
    for f in os.listdir(acc_fastq_dir):
        try:
            os.remove(os.path.join(acc_fastq_dir, f))
        except OSError as e:
            logging.warning(f"[Converter #{job_id}] Could not clear stale file {f}: {e}")

    logging.info(f"[Converter #{job_id}] Starting fasterq-dump for {accession}")

    # ── Step 1: fasterq-dump ──────────────────────────────────────────────────
    fasterq_cmd = [
        "fasterq-dump",
        "--threads",  str(threads),
        "--temp",     job_temp_dir,   # isolated per-job temp — no cross-job collisions
        "--outdir",   acc_fastq_dir,
        "--split-3",                  # separate R1/R2/unpaired
        "--skip-technical",
        sra_path,
    ]

    try:
        proc = subprocess.run(
            fasterq_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=7200,             # 2h hard timeout per file
        )
        if proc.returncode != 0:
            err = proc.stderr.decode(errors="replace").strip()
            logging.error(f"[Converter #{job_id}] fasterq-dump failed for {accession}: {err}")
            _cleanup_dir(job_temp_dir)
            _cleanup_dir(acc_fastq_dir)
            result_queue.put((sra_path, [], False))
            return
    except subprocess.TimeoutExpired:
        logging.error(f"[Converter #{job_id}] fasterq-dump timed out for {accession}")
        _cleanup_dir(job_temp_dir)
        _cleanup_dir(acc_fastq_dir)
        result_queue.put((sra_path, [], False))
        return
    except FileNotFoundError:
        logging.error(f"[Converter #{job_id}] fasterq-dump not found in PATH")
        _cleanup_dir(job_temp_dir)
        _cleanup_dir(acc_fastq_dir)
        result_queue.put((sra_path, [], False))
        return

    # Temp dir is now empty (fasterq-dump cleans its own scratch) — remove it.
    _cleanup_dir(job_temp_dir)

    logging.info(f"[Converter #{job_id}] fasterq-dump done for {accession}, compressing ...")

    # ── Step 2: pigz compress each .fastq output ──────────────────────────────
    fastq_files = [
        os.path.join(acc_fastq_dir, f)
        for f in os.listdir(acc_fastq_dir)
        if f.endswith(".fastq")
    ]

    if not fastq_files:
        logging.error(
            f"[Converter #{job_id}] No .fastq files found after fasterq-dump for {accession}"
        )
        result_queue.put((sra_path, [], False))
        return

    fastq_gz_files = []
    # Launch all pigz processes simultaneously
    procs = {
        fq: subprocess.Popen(
            ["pigz", "-p", str(max(1, threads // len(fastq_files))), fq],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        for fq in fastq_files
    }

    for fq, proc in procs.items():
        try:
            proc.wait(timeout=3600)
            if proc.returncode != 0:
                err = proc.stderr.read().decode(errors="replace").strip()
                logging.error(f"[Converter #{job_id}] pigz failed for {fq}: {err}")
                _cleanup_dir(acc_fastq_dir)
                result_queue.put((sra_path, [], False))
                return
            gz_path = fq + ".gz"
            if os.path.exists(gz_path):
                fastq_gz_files.append(gz_path)
                with byte_counter.get_lock():
                    byte_counter.value += os.path.getsize(gz_path)
        except subprocess.TimeoutExpired:
            proc.kill()
            logging.error(f"[Converter #{job_id}] pigz timed out for {fq}")
            _cleanup_dir(acc_fastq_dir)
            result_queue.put((sra_path, [], False))
            return
        except FileNotFoundError:
            logging.error(f"[Converter #{job_id}] pigz not found in PATH")
            _cleanup_dir(acc_fastq_dir)
            result_queue.put((sra_path, [], False))
            return

    logging.info(
        f"[Converter #{job_id}] Completed {accession}: "
        f"{[os.path.basename(f) for f in fastq_gz_files]}"
    )
    result_queue.put((sra_path, fastq_gz_files, True))


#############################
# Throughput reporter
#############################

def _report_conversion_throughput(
    byte_counter: mp.Value,
    active_jobs: mp.Value,
    throughput_logs: deque,
    throughput_lock: Lock,
    stop_event,
    log_dir: str = "logs",
):
    """
    Logs conversion throughput (MB/s of compressed output) once per second.
    Mirrors the download throughput reporter pattern.
    """
    os.makedirs(log_dir, exist_ok=True)
    t = time.time()
    fname = os.path.join(
        log_dir,
        f"log_conversion_{datetime.datetime.fromtimestamp(t).strftime('%Y%m%d_%H%M%S')}.csv"
    )
    with open(fname, "w") as f:
        f.write("timestamp,elapsed_sec,current_mbs,avg_mbs,active_jobs\n")

    start_time = time.time()
    prev_bytes = 0

    while not stop_event.is_set():
        time.sleep(1.0)
        t1 = time.time()
        elapsed = round(t1 - start_time, 1)

        cur_bytes   = byte_counter.value
        delta_bytes = cur_bytes - prev_bytes
        prev_bytes  = cur_bytes

        curr_mbs = round(delta_bytes / (1024 * 1024), 2)
        avg_mbs  = round(cur_bytes / (elapsed * 1024 * 1024), 2) if elapsed > 0 else 0.0
        jobs     = active_jobs.value

        with throughput_lock:
            throughput_logs.append(curr_mbs)

        logging.info(
            f"Conversion @{elapsed}s: Current: {curr_mbs}MB/s, "
            f"Avg: {avg_mbs}MB/s, Active jobs: {jobs}"
        )
        with open(fname, "a") as f:
            f.write(f"{t1},{elapsed},{curr_mbs},{avg_mbs},{jobs}\n")


#############################
# SRAConverter
#############################

class SRAConverter:
    """
    Manages the SRA → fastq.gz conversion pipeline stage.

    Parameters
    ----------
    processing_queue : mp.Queue
        Source queue — receives absolute .sra file paths from the downloader.
    move_queue : mp.Queue
        Sink queue — receives absolute .fastq.gz file paths for receiver.py.
    work_dir : str
        Root on NVMe for staging (fastq output and temp files).
    nvme_device : str
        Bare device name for /proc/diskstats, e.g. 'nvme0n1'.
    threads_per_job : int
        --threads passed to fasterq-dump and pigz.
    cpu_threshold : float
        CPU % ceiling for admission gate (default 85.0).
    nvme_threshold : float
        NVMe utilization % ceiling for admission gate (default 80.0).
    max_jobs : int | None
        Hard sanity ceiling on concurrent jobs.  None → cpu_count.
    probing_sec : float
        Poll interval inside admission gate (seconds).
    """

    def __init__(
        self,
        processing_queue: mp.Queue,
        move_queue: mp.Queue,
        work_dir: str = "/mnt/nvme0n1/fastbiodl/",
        nvme_device: str = "nvme0n1",
        threads_per_job: int = 4,
        cpu_threshold: float = 85.0,
        nvme_threshold: float = 80.0,
        max_jobs: Optional[int] = None,
        probing_sec: float = 1.0,
    ):
        self.processing_queue = processing_queue
        self.move_queue       = move_queue
        self.work_dir         = work_dir
        self.fastq_dir        = os.path.join(work_dir, "fastq")
        self.temp_dir         = os.path.join(work_dir, "tmp")
        self.nvme_device      = nvme_device
        self.threads_per_job  = threads_per_job
        self.cpu_threshold    = cpu_threshold
        self.nvme_threshold   = nvme_threshold
        self.max_jobs         = max_jobs or mp.cpu_count()
        self.probing_sec      = probing_sec

        # Shared state
        self._active_jobs     = mp.Value("i", 0)
        self._byte_counter    = mp.Value("Q", 0)   # unsigned 64-bit
        self._converted_count = mp.Value("i", 0)
        self._failed_count    = mp.Value("i", 0)

        # Result queue from worker processes → collector
        self._result_queue    = mp.Queue()

        # Throughput tracking
        self._throughput_logs = deque(maxlen=10000)
        self._throughput_lock = Lock()

        # FIX [My #1]: lock that protects _worker_procs against the
        # dispatcher (append) vs. collector (iterate + reassign) race.
        self._procs_lock      = Lock()

        # Lifecycle
        self._stop_event      = mp.Event()   # mp.Event so workers can observe it
        self._threads         = []
        self._worker_procs    = []
        self._job_id_counter  = 0

        os.makedirs(self.fastq_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)

    # ── Public API ──────────────────────────────────────────────────────────

    def start(self):
        """Start dispatcher, result collector, and throughput reporter threads."""
        t_dispatch = Thread(target=self._dispatcher_loop,        name="conv-dispatcher", daemon=True)
        t_collect  = Thread(target=self._result_collector_loop,  name="conv-collector",  daemon=True)
        t_report   = Thread(
            target=_report_conversion_throughput,
            args=(
                self._byte_counter,
                self._active_jobs,
                self._throughput_logs,
                self._throughput_lock,
                self._stop_event,
            ),
            name="conv-reporter",
            daemon=True,
        )

        for t in (t_dispatch, t_collect, t_report):
            t.start()
            self._threads.append(t)

        logging.info(
            f"[SRAConverter] Started — max_jobs={self.max_jobs}, "
            f"threads_per_job={self.threads_per_job}, "
            f"nvme_device={self.nvme_device}, "
            f"cpu_threshold={self.cpu_threshold}%, "
            f"nvme_threshold={self.nvme_threshold}%"
        )

    def stop(self, timeout: float = 7200.0):
        """
        Signal all threads to stop and wait for in-flight conversions to finish.

        FIX [Img #2]: The original stop() just set _stop_event and joined the
        management threads with a short timeout, then immediately terminated any
        living worker procs.  That kills a 2-hour fasterq-dump after 30 s.

        New behaviour:
          1. Set _stop_event so the dispatcher stops accepting new work.
          2. Busy-wait (up to timeout) for _active_jobs to drain to 0 —
             i.e. all in-flight conversions have posted their result.
          3. Only then join the management threads and reap worker procs.
        """
        self._stop_event.set()

        # Wait for all in-flight jobs to finish naturally.
        deadline = time.time() + timeout
        while self._active_jobs.value > 0:
            if time.time() > deadline:
                logging.warning(
                    f"[SRAConverter] stop() timed out after {timeout}s "
                    f"with {self._active_jobs.value} job(s) still active — "
                    f"terminating remaining workers."
                )
                break
            time.sleep(1.0)

        # Join management threads (they will have exited or be close to it).
        for t in self._threads:
            t.join(timeout=10.0)

        # Reap worker procs — only terminate those still alive after the wait.
        with self._procs_lock:
            for p in self._worker_procs:
                if p.is_alive():
                    p.terminate()
                    p.join(timeout=5)

        logging.info(
            f"[SRAConverter] Stopped — "
            f"converted={self._converted_count.value}, "
            f"failed={self._failed_count.value}"
        )

    @property
    def converted_count(self) -> int:
        return self._converted_count.value

    @property
    def failed_count(self) -> int:
        return self._failed_count.value

    # ── Internal loops ──────────────────────────────────────────────────────

    def _dispatcher_loop(self):
        """
        Pulls .sra paths from processing_queue one at a time.
        Before launching each job:
          1. Waits for active_jobs < max_jobs  (hard sanity ceiling)
          2. Waits for AdmissionGate to grant based on CPU + NVMe utilization
        Then spawns a worker process for that job.
        """
        gate = AdmissionGate(
            nvme_device=self.nvme_device,
            cpu_threshold=self.cpu_threshold,
            nvme_threshold=self.nvme_threshold,
            poll_interval=self.probing_sec,
        )

        while not self._stop_event.is_set():
            # ── (a) pull next file ─────────────────────────────────────────
            # FIX [Img #3]: catch queue.Empty specifically — not bare Exception
            # which would swallow programming errors and make them silent.
            try:
                sra_path = self.processing_queue.get(timeout=2.0)
            except queue.Empty:
                continue

            if sra_path is None:            # sentinel value — no more files
                logging.info("[SRAConverter] Received sentinel, dispatcher exiting")
                break

            # FIX [My #3]: skip any file that is not a plain .sra — e.g. when
            # the pipeline is run with --fastq and FASTQ tarballs land here.
            if not sra_path.endswith(".sra"):
                logging.warning(
                    f"[SRAConverter] Skipping non-SRA file (expected .sra): {sra_path}"
                )
                self.move_queue.put(sra_path)
                continue

            logging.info(f"[SRAConverter] Dequeued: {sra_path}")

            # ── (b) sanity ceiling — wait if too many jobs already running ──
            while self._active_jobs.value >= self.max_jobs:
                if self._stop_event.is_set():
                    return
                time.sleep(0.5)

            # ── (c) admission gate — wait for CPU + NVMe headroom ──────────
            granted = gate.admit(stop_event=self._stop_event)
            if not granted:
                return

            logging.info(
                f"[SRAConverter] Admission granted "
                f"(CPU {gate.last_cpu_util}%, NVMe {gate.last_nvme_util}%) "
                f"— launching job for {os.path.basename(sra_path)}"
            )

            # ── (d) launch worker process ───────────────────────────────────
            self._job_id_counter += 1
            job_id = self._job_id_counter

            # FIX [Img #5]: daemon=False so if this parent thread exits
            # unexpectedly, the OS does not tear down an in-progress
            # fasterq-dump or pigz and leave a corrupt output file.
            p = mp.Process(
                target=_conversion_worker,
                args=(
                    job_id,
                    sra_path,
                    self.fastq_dir,
                    self.temp_dir,
                    self.threads_per_job,
                    self._result_queue,
                    self._byte_counter,
                ),
                daemon=False,   # FIX [Img #5]
            )
            p.start()

            # FIX [My #1]: hold _procs_lock while appending
            with self._procs_lock:
                self._worker_procs.append(p)

            with self._active_jobs.get_lock():
                self._active_jobs.value += 1

            logging.info(
                f"[SRAConverter] Job #{job_id} started for {os.path.basename(sra_path)} "
                f"(active jobs: {self._active_jobs.value})"
            )

    def _result_collector_loop(self):
        """
        Drains _result_queue.
        On success: pushes each fastq.gz path to move_queue, deletes .sra.
        On failure: logs the error, .sra stays for manual inspection.
        Decrements active_jobs counter.
        """
        while not self._stop_event.is_set() or self._active_jobs.value > 0:
            # FIX [Img #3]: catch queue.Empty only, not bare Exception.
            try:
                sra_path, fastq_gz_files, success = self._result_queue.get(timeout=2.0)
            except queue.Empty:
                continue

            self._handle_result(sra_path, fastq_gz_files, success)

        # FIX [My #5]: drain any results that arrived between the while-loop
        # condition being checked False and this line executing.  Without this
        # drain a result can be silently dropped in the stop-event race window.
        while True:
            try:
                sra_path, fastq_gz_files, success = self._result_queue.get_nowait()
                self._handle_result(sra_path, fastq_gz_files, success)
            except queue.Empty:
                break

    def _handle_result(self, sra_path: str, fastq_gz_files: list, success: bool):
        """
        Shared result-processing logic used by both the normal collector loop
        and the post-stop drain.  Extracted to avoid code duplication.
        """
        with self._active_jobs.get_lock():
            self._active_jobs.value = max(0, self._active_jobs.value - 1)

        # FIX [My #1]: hold _procs_lock while iterating and reassigning
        # _worker_procs so the dispatcher cannot append concurrently.
        with self._procs_lock:
            for p in self._worker_procs:
                if not p.is_alive():
                    p.join(timeout=1)
            self._worker_procs = [p for p in self._worker_procs if p.is_alive()]

        if success:
            with self._converted_count.get_lock():
                self._converted_count.value += 1

            for gz_path in fastq_gz_files:
                self.move_queue.put(gz_path)
                logging.info(f"[SRAConverter] → move_queue: {gz_path}")

            # Remove .sra to free space once all .fastq.gz are safely queued.
            try:
                os.remove(sra_path)
                logging.info(f"[SRAConverter] Removed source .sra: {sra_path}")
            except OSError as e:
                logging.warning(f"[SRAConverter] Could not remove {sra_path}: {e}")

        else:
            with self._failed_count.get_lock():
                self._failed_count.value += 1
            logging.error(
                f"[SRAConverter] Conversion failed for {sra_path}, "
                f"leaving file for inspection"
            )

        logging.info(
            f"[SRAConverter] Status — active: {self._active_jobs.value}, "
            f"done: {self._converted_count.value}, "
            f"failed: {self._failed_count.value}"
        )