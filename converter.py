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
from threading import Thread, Lock, Event
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
    accession = os.path.basename(sra_path).split(".")[0]
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

    t_fasterq_start = time.time()   # ← benchmark timing: fasterq-dump started
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
            result_queue.put((sra_path, [], False, t_fasterq_start, 0.0, 0.0, 0.0))
            return
    except subprocess.TimeoutExpired:
        logging.error(f"[Converter #{job_id}] fasterq-dump timed out for {accession}")
        _cleanup_dir(job_temp_dir)
        _cleanup_dir(acc_fastq_dir)
        result_queue.put((sra_path, [], False, t_fasterq_start, 0.0, 0.0, 0.0))
        return
    except FileNotFoundError:
        logging.error(f"[Converter #{job_id}] fasterq-dump not found in PATH")
        _cleanup_dir(job_temp_dir)
        _cleanup_dir(acc_fastq_dir)
        result_queue.put((sra_path, [], False, t_fasterq_start, 0.0, 0.0, 0.0))
        return

    # Temp dir is now empty (fasterq-dump cleans its own scratch) — remove it.
    _cleanup_dir(job_temp_dir)

    t_fasterq_done = time.time()   # ← benchmark timing: fasterq-dump finished
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
        result_queue.put((sra_path, [], False, t_fasterq_start, t_fasterq_done, 0.0, 0.0))
        return

    fastq_gz_files = []
    t_pigz_start = time.time()   # ← benchmark timing: pigz started (all jobs launched simultaneously)
    # Launch all pigz processes simultaneously
    procs = {
        fq: subprocess.Popen(
            ["pigz", "-p", str(max(1, threads)), fq],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        for fq in fastq_files
    }

    for fq, proc in procs.items():
        gz_path = fq + ".gz"
        prev_gz_size = 0
        deadline = time.time() + 3600
        try:
            # Poll the growing .gz file every second so byte_counter reflects
            # live progress rather than a single lump-sum update at job end.
            # This is what makes the conversion throughput reporter show non-zero
            # MB/s while pigz is running instead of staying at 0.0 MB/s.
            while True:
                try:
                    proc.wait(timeout=1.0)
                    # pigz finished — capture any remaining bytes
                    if os.path.exists(gz_path):
                        cur_gz_size = os.path.getsize(gz_path)
                        delta = cur_gz_size - prev_gz_size
                        if delta > 0:
                            with byte_counter.get_lock():
                                byte_counter.value += delta
                    break  # exit polling loop
                except subprocess.TimeoutExpired:
                    if time.time() > deadline:
                        proc.kill()
                        raise subprocess.TimeoutExpired(proc.args, 3600)
                    # Drip incremental bytes into the shared counter
                    if os.path.exists(gz_path):
                        cur_gz_size = os.path.getsize(gz_path)
                        delta = cur_gz_size - prev_gz_size
                        if delta > 0:
                            with byte_counter.get_lock():
                                byte_counter.value += delta
                            prev_gz_size = cur_gz_size

            if proc.returncode != 0:
                err = proc.stderr.read().decode(errors="replace").strip()
                logging.error(f"[Converter #{job_id}] pigz failed for {fq}: {err}")
                _cleanup_dir(acc_fastq_dir)
                result_queue.put((sra_path, [], False, t_fasterq_start, t_fasterq_done, t_pigz_start, 0.0))
                return
            if os.path.exists(gz_path):
                fastq_gz_files.append(gz_path)
        except subprocess.TimeoutExpired:
            logging.error(f"[Converter #{job_id}] pigz timed out for {fq}")
            _cleanup_dir(acc_fastq_dir)
            result_queue.put((sra_path, [], False, t_fasterq_start, t_fasterq_done, t_pigz_start, 0.0))
            return
        except FileNotFoundError:
            logging.error(f"[Converter #{job_id}] pigz not found in PATH")
            _cleanup_dir(acc_fastq_dir)
            result_queue.put((sra_path, [], False, t_fasterq_start, t_fasterq_done, t_pigz_start, 0.0))
            return

    t_pigz_done = time.time()   # ← benchmark timing: all pigz finished
    logging.info(
        f"[Converter #{job_id}] Completed {accession}: "
        f"{[os.path.basename(f) for f in fastq_gz_files]}"
    )
    result_queue.put((sra_path, fastq_gz_files, True,
                      t_fasterq_start, t_fasterq_done,
                      t_pigz_start, t_pigz_done))


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
    fastq_dir: str = "/mnt/nvme0n1/fastbiodl/fastq",
):
    """
    Logs conversion throughput (MB/s of output) once per second.
    Tracks fasterq-dump (.fastq files) and pigz (.fastq.gz files) SEPARATELY.
    Continues monitoring until all active jobs complete (not just until stop_event).
    """
    os.makedirs(log_dir, exist_ok=True)
    t = time.time()
    fname = os.path.join(
        log_dir,
        f"log_conversion_{datetime.datetime.fromtimestamp(t).strftime('%Y%m%d_%H%M%S')}.csv"
    )
    
    try:
        with open(fname, "w") as f:
            f.write("timestamp,elapsed_sec,convert_mbs,compress_mbs,total_mbs,active_jobs,fastq_mb,fastqgz_mb\n")

        start_time = time.time()
        prev_fastq_bytes = 0
        prev_fastqgz_bytes = 0

        logging.info("[ConversionReporter] Started monitoring conversion progress")

        # Continue until stop_event AND all jobs are done
        while not stop_event.is_set() or active_jobs.value > 0:
            try:
                time.sleep(1.0)
                t1 = time.time()
                elapsed = round(t1 - start_time, 1)
                jobs = active_jobs.value

                # Measure .fastq (fasterq-dump output) and .fastq.gz (pigz output) SEPARATELY
                fastq_bytes = 0
                fastqgz_bytes = 0
                if os.path.exists(fastq_dir):
                    for root, dirs, files in os.walk(fastq_dir):
                        for f in files:
                            fpath = os.path.join(root, f)
                            try:
                                fsize = os.path.getsize(fpath)
                                if f.endswith('.fastq.gz'):
                                    fastqgz_bytes += fsize
                                elif f.endswith('.fastq'):
                                    fastq_bytes += fsize
                            except OSError:
                                pass  # File might have been deleted

                # Calculate per-second throughput for each phase
                delta_fastq = fastq_bytes - prev_fastq_bytes
                delta_fastqgz = fastqgz_bytes - prev_fastqgz_bytes
                prev_fastq_bytes = fastq_bytes
                prev_fastqgz_bytes = fastqgz_bytes

                convert_mbs = round(delta_fastq / (1024 * 1024), 2)
                compress_mbs = round(delta_fastqgz / (1024 * 1024), 2)
                total_mbs = convert_mbs + compress_mbs

                fastq_mb = round(fastq_bytes / (1024 * 1024), 2)
                fastqgz_mb = round(fastqgz_bytes / (1024 * 1024), 2)

                with throughput_lock:
                    throughput_logs.append(total_mbs)

                # Separate log lines for better clarity
                # Always show throughput (even if 0) when jobs are active
                if jobs > 0:
                    if convert_mbs > 0:
                        logging.info(
                            f"fasterq-dump @{elapsed}s: {convert_mbs}MB/s "
                            f"(total: {fastq_mb}MB .fastq, active: {jobs})"
                        )
                    if compress_mbs > 0:
                        logging.info(
                            f"pigz @{elapsed}s: {compress_mbs}MB/s "
                            f"(total: {fastqgz_mb}MB .fastq.gz, active: {jobs})"
                        )
                    # If no throughput but jobs are running, they're still processing
                    # (e.g., fasterq-dump extracting in temp space before writing output)
                    if convert_mbs == 0 and compress_mbs == 0:
                        logging.info(
                            f"Conversion @{elapsed}s: 0MB/s (processing, active: {jobs})"
                        )
                else:
                    # No jobs running - truly idle
                    logging.info(f"Conversion @{elapsed}s: idle")
                
                with open(fname, "a") as f:
                    f.write(f"{t1},{elapsed},{convert_mbs},{compress_mbs},{total_mbs},{jobs},{fastq_mb},{fastqgz_mb}\n")
                    
            except Exception as e:
                logging.error(f"[ConversionReporter] Error in reporter loop iteration: {e}")
                # Continue running despite errors
                continue
        
        logging.info(f"[ConversionReporter] Stopped (jobs={active_jobs.value})")
        
    except Exception as e:
        logging.error(f"[ConversionReporter] Fatal error, thread exiting: {e}")
        import traceback
        logging.error(traceback.format_exc())


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
        threads_per_job: int = 8,
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

        # Benchmark timing: per-phase epoch timestamps aggregated across all jobs.
        # "first_start" tracks the earliest start (min), "last_done" the latest
        # end (max).  0.0 means the phase has not been observed yet.
        # Together they give the true wall-clock span of each phase and let the
        # caller compute inter-phase overlap.
        self._t_first_fasterq_start = mp.Value("d", 0.0)
        self._t_last_fasterq_done   = mp.Value("d", 0.0)
        self._t_first_pigz_start    = mp.Value("d", 0.0)
        self._t_last_pigz_done      = mp.Value("d", 0.0)

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
        # Signals that the dispatcher thread has exited naturally (via sentinel).
        # stop() waits for this before setting _stop_event so that a file waiting
        # at the AdmissionGate is never interrupted mid-queue-drain (Bug #6).
        self._dispatcher_done = Event()      # threading.Event — intra-process only
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
                "logs",
                self.fastq_dir,  # Pass fastq_dir for file size monitoring
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

        FIX [Bug #6]: Do NOT set _stop_event immediately.  The dispatcher exits
        on its own once it receives the None sentinel placed by the caller.  If
        _stop_event is raised while the dispatcher is blocked inside
        AdmissionGate.admit(), the in-flight item gets requeued but never
        re-processed, effectively dropping it.

        New behaviour:
          1. Wait (up to timeout) for the dispatcher to exit naturally via the
             sentinel.  _dispatcher_done is set by _dispatcher_loop when done.
          2. Only after the dispatcher has fully drained do we set _stop_event
             so the result-collector loop knows to stop waiting for new results.
          3. Busy-wait for _active_jobs to drain to 0.
          4. Join management threads and reap worker procs.
        """
        # Fix #4: single shared deadline across all shutdown phases so total
        # wait is bounded by `timeout`, not 2×timeout.
        _stop_deadline = time.time() + timeout

        # Step 1 — let the dispatcher finish on its own (sentinel-driven exit).
        remaining = max(0.0, _stop_deadline - time.time())
        if not self._dispatcher_done.wait(timeout=remaining):
            logging.warning(
                f"[SRAConverter] stop() timed out waiting for dispatcher to finish "
                f"(timeout={timeout}s) — forcing stop."
            )

        # Step 2 — NOW it is safe to set _stop_event: the dispatcher has already
        # exited, so no in-queue item can be stranded in AdmissionGate.admit().
        self._stop_event.set()

        # Wait for all in-flight jobs to finish naturally.
        while self._active_jobs.value > 0:
            if time.time() > _stop_deadline:
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

    @property
    def t_first_fasterq_start(self) -> float:
        """Epoch timestamp of the earliest fasterq-dump start across all jobs (0.0 if none yet)."""
        return self._t_first_fasterq_start.value

    @property
    def t_last_fasterq_done(self) -> float:
        """Epoch timestamp of the latest fasterq-dump completion across all jobs (0.0 if none yet)."""
        return self._t_last_fasterq_done.value

    @property
    def t_first_pigz_start(self) -> float:
        """Epoch timestamp of the earliest pigz start across all jobs (0.0 if none yet)."""
        return self._t_first_pigz_start.value

    @property
    def t_last_pigz_done(self) -> float:
        """Epoch timestamp of the latest pigz completion across all jobs (0.0 if none yet)."""
        return self._t_last_pigz_done.value

    # ── Internal loops ──────────────────────────────────────────────────────

    def _dispatcher_loop(self):
        """
        Pulls .sra paths from processing_queue one at a time.
        Before launching each job:
          1. Waits for active_jobs < max_jobs  (hard sanity ceiling)
          2. Waits for AdmissionGate to grant based on CPU + NVMe utilization
        Then spawns a worker process for that job.
        Sets _dispatcher_done when it exits so stop() knows it is safe to
        raise _stop_event (Bug #6 fix).
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
                self._dispatcher_done.set()  # unblock stop() (Bug #6 fix)
                break

            # FIX [My #3]: skip any file that is not a plain .sra — e.g. when
            # the pipeline is run with --fastq and FASTQ tarballs land here.
            import re
            SRA_FILE_RE = re.compile(r'\.(sra|lite\.\d+)$')

            if not SRA_FILE_RE.search(os.path.basename(sra_path)):
                logging.warning(
                    f"[SRAConverter] Skipping non-SRA file (expected .sra): {sra_path}"
                )
                self.move_queue.put(sra_path)
                continue

            logging.info(f"[SRAConverter] Dequeued: {sra_path}")

            # ── (b) sanity ceiling — wait if too many jobs already running ──
            while self._active_jobs.value >= self.max_jobs:
                time.sleep(0.5)

            # ── (c) admission gate — wait for CPU + NVMe headroom ──────────
            # The dispatcher only exits via the sentinel from now on, so
            # pass stop_event=None — we no longer want the gate to abort
            # mid-drain (that was the original Bug #6 root cause).
            granted = gate.admit(stop_event=None)
            if not granted:
                # Should never happen now that we pass stop_event=None,
                # but keep as a last-resort safety net.
                logging.warning(
                    f"[SRAConverter] AdmissionGate returned False unexpectedly for "
                    f"{os.path.basename(sra_path)} — requeueing"
                )
                self.processing_queue.put(sra_path)
                continue

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

        # Reached only if _stop_event fired before a sentinel arrived (e.g. SIGINT).
        # Make sure stop() doesn't hang indefinitely waiting for _dispatcher_done.
        self._dispatcher_done.set()

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
                sra_path, fastq_gz_files, success, \
                    t_fasterq_start, t_fasterq_done, \
                    t_pigz_start, t_pigz_done = self._result_queue.get(timeout=2.0)
            except queue.Empty:
                continue

            self._handle_result(sra_path, fastq_gz_files, success,
                                t_fasterq_start, t_fasterq_done,
                                t_pigz_start, t_pigz_done)

        # FIX [My #5]: drain any results that arrived between the while-loop
        # condition being checked False and this line executing.  Without this
        # drain a result can be silently dropped in the stop-event race window.
        while True:
            try:
                sra_path, fastq_gz_files, success, \
                    t_fasterq_start, t_fasterq_done, \
                    t_pigz_start, t_pigz_done = self._result_queue.get_nowait()
                self._handle_result(sra_path, fastq_gz_files, success,
                                    t_fasterq_start, t_fasterq_done,
                                    t_pigz_start, t_pigz_done)
            except queue.Empty:
                break

    def _handle_result(self, sra_path: str, fastq_gz_files: list, success: bool,
                       t_fasterq_start: float = 0.0, t_fasterq_done: float = 0.0,
                       t_pigz_start: float = 0.0, t_pigz_done: float = 0.0):
        """
        Shared result-processing logic used by both the normal collector loop
        and the post-stop drain.  Extracted to avoid code duplication.

        t_fasterq_start / t_fasterq_done : epoch timestamps bracketing fasterq-dump
        t_pigz_start    / t_pigz_done    : epoch timestamps bracketing pigz

        Across concurrent jobs we track:
          _t_first_fasterq_start  — earliest start (min), for overlap calculation
          _t_last_fasterq_done    — latest  end   (max)
          _t_first_pigz_start     — earliest start (min)
          _t_last_pigz_done       — latest  end   (max)
        0.0 is used as a sentinel meaning "not observed yet".
        """
        with self._active_jobs.get_lock():
            self._active_jobs.value = max(0, self._active_jobs.value - 1)

        # fasterq-dump window ─────────────────────────────────────────────────
        if t_fasterq_start > 0.0:
            with self._t_first_fasterq_start.get_lock():
                prev = self._t_first_fasterq_start.value
                if prev == 0.0 or t_fasterq_start < prev:
                    self._t_first_fasterq_start.value = t_fasterq_start
        if t_fasterq_done > 0.0:
            with self._t_last_fasterq_done.get_lock():
                if t_fasterq_done > self._t_last_fasterq_done.value:
                    self._t_last_fasterq_done.value = t_fasterq_done

        # pigz window ─────────────────────────────────────────────────────────
        if t_pigz_start > 0.0:
            with self._t_first_pigz_start.get_lock():
                prev = self._t_first_pigz_start.value
                if prev == 0.0 or t_pigz_start < prev:
                    self._t_first_pigz_start.value = t_pigz_start
        if t_pigz_done > 0.0:
            with self._t_last_pigz_done.get_lock():
                if t_pigz_done > self._t_last_pigz_done.value:
                    self._t_last_pigz_done.value = t_pigz_done

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