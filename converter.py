#!/usr/bin/env python3
"""
converter.py — SRA to FASTQ conversion stage for the seqflux pipeline.

Architecture (FFS-direct, no move phase):
  - SRAConverter class owns all state and worker lifecycle.
  - AdmissionGate polls /proc/stat (CPU) and /proc/diskstats (NVMe) before
    starting each new fasterq-dump job.
  - Separate worker pools run fasterq-dump and pigz independently.
    The pools use Python threads that launch external subprocesses; this
    avoids forking Python worker processes after the converter's reporter and
    collector threads have already started.
  - fasterq-dump writes .fastq files to work_dir (FAST TIER).
  - pigz reads .fastq from the fast tier and streams .fastq.gz output
    DIRECTLY to compressed_output_dir (SLOW TIER) via `pigz -c`; there is
    no separate move stage and no internal FileMover.
  - Completed .fastq.gz paths are pushed to move_queue purely as a
    completion-signal channel for the caller; the files are already at
    their final destination. The queue is never consumed by SRAConverter
    itself and may be drained inline by the caller after stop().

Usage (from seqflux.py):
    from converter import SRAConverter
    converter = SRAConverter(
        processing_queue=processing_queue,
        move_queue=move_queue,
        work_dir="<fast_tier>/seqflux/",            # fast tier
        compressed_output_dir="<slow_tier>/output/",  # slow tier
        nvme_device=os.environ.get("EXPANSE_NVME_DEVICE", "nvme0n1"),
        threads_per_job=4,
        cpu_threshold=85.0,
        nvme_threshold=92.0,
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

[My #8]  _fasterq_worker and _pigz_worker isolate each job in its own output dir and
         cleans that dir on partial pigz failure so concurrent jobs cannot
         trample staged FASTQ/FASTQ.GZ files.

[Img #2] stop() now waits for _active_jobs to reach 0 (up to timeout) before
         terminating worker procs, so no in-flight conversion is killed mid-run.

[Img #3] Bare `except Exception` replaced with `except queue.Empty` in
         _dispatcher_loop and _result_collector_loop so real errors surface.

[Img #5] Converter workers now use threads around the external tools rather
         than multiprocessing.Process, avoiding fork-after-threads deadlocks.
"""

import os
import queue
import time
import logging
import subprocess
import datetime
import shutil
import multiprocessing as mp
from threading import Thread, Lock, Event
from collections import deque
from typing import Optional

from config_seqflux import get_seqflux_work_dir
from storage_config import get_nvme_device


#############################
# System metric helpers
#############################

def _human_bytes(num_bytes: int) -> str:
    """Format byte counts for readable logging."""
    value = float(max(0, num_bytes))
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if value < 1024.0 or unit == "PB":
            return f"{value:.2f}{unit}"
        value /= 1024.0

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
        nvme_threshold: float = 92.0,
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
        # Require saturation for multiple consecutive samples to avoid
        # blocking launches on one-sample utilization spikes.
        consecutive_over = 0

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
                consecutive_over = 0
                return True

            consecutive_over += 1
            if consecutive_over < 2:
                continue

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


def _terminate_pigz_processes(procs: dict, exclude=None):
    """Best-effort reap of sibling pigz processes after partial failure."""
    for proc in procs.values():
        if proc is exclude:
            continue
        if proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass

    deadline = time.time() + 5.0
    for proc in procs.values():
        if proc is exclude:
            continue
        if proc.poll() is not None:
            continue
        remaining = max(0.0, deadline - time.time())
        try:
            proc.wait(timeout=remaining)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                proc.wait(timeout=1.0)
            except Exception:
                pass


def _decrement_shared_counter(counter):
    """Atomically decrement a shared counter without letting it go negative."""
    with counter.get_lock():
        counter.value = max(0, counter.value - 1)


def _fasterq_worker(
    job_id: int,
    sra_path: str,
    sra_size: int,
    fastq_dir: str,
    temp_dir: str,
    threads: int,
    pigz_task_queue: mp.Queue,
    result_queue: mp.Queue,
    phase_queue: mp.Queue,
    active_fasterq_jobs,
    removed_sra_counter,
):
    """
    Runs fasterq-dump for one .sra file and hands successful jobs to pigz queue.
    On fasterq failure, pushes a failed result directly to result_queue.
    """
    source_name = os.path.basename(sra_path)
    accession = source_name.split(".")[0]
    job_output_dir = os.path.join(fastq_dir, accession, f"{source_name}__job{job_id}")
    job_temp_dir = os.path.join(temp_dir, f"{accession}_{job_id}")
    t_fasterq_start = time.time()
    t_fasterq_done = 0.0
    handed_to_pigz = False
    result_emitted = False

    try:
        os.makedirs(job_output_dir, exist_ok=True)
        os.makedirs(job_temp_dir, exist_ok=True)

        logging.info(f"[Converter #{job_id}] Starting fasterq-dump for {accession}")

        fasterq_cmd = [
            "fasterq-dump",
            "--threads", str(threads),
            "--temp", job_temp_dir,
            "--outdir", job_output_dir,
            "--split-3",
            "--skip-technical",
            sra_path,
        ]

        try:
            proc = subprocess.run(
                fasterq_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=7200,
            )
            if proc.returncode != 0:
                err = proc.stderr.decode(errors="replace").strip()
                logging.error(f"[Converter #{job_id}] fasterq-dump failed for {accession}: {err}")
                _cleanup_dir(job_temp_dir)
                _cleanup_dir(job_output_dir)
                result_queue.put((sra_path, [], False, t_fasterq_start, 0.0, 0.0, 0.0))
                result_emitted = True
                return
        except subprocess.TimeoutExpired:
            logging.error(f"[Converter #{job_id}] fasterq-dump timed out for {accession}")
            _cleanup_dir(job_temp_dir)
            _cleanup_dir(job_output_dir)
            result_queue.put((sra_path, [], False, t_fasterq_start, 0.0, 0.0, 0.0))
            result_emitted = True
            return
        except FileNotFoundError:
            logging.error(f"[Converter #{job_id}] fasterq-dump not found in PATH")
            _cleanup_dir(job_temp_dir)
            _cleanup_dir(job_output_dir)
            result_queue.put((sra_path, [], False, t_fasterq_start, 0.0, 0.0, 0.0))
            result_emitted = True
            return

        _cleanup_dir(job_temp_dir)
        t_fasterq_done = time.time()

        try:
            phase_queue.put(("PIGZ_START", sra_path, int(sra_size)))
        except Exception as e:
            logging.warning(
                f"[Converter #{job_id}] Could not publish phase transition for {accession}: {e}"
            )

        try:
            removed_size = os.path.getsize(sra_path)
            os.remove(sra_path)
            with removed_sra_counter.get_lock():
                removed_sra_counter.value += removed_size
                removed_total = removed_sra_counter.value
            logging.info(
                f"[Converter #{job_id}] Removed source .sra after fasterq-dump: {sra_path} "
                f"({_human_bytes(removed_size)}, total {_human_bytes(removed_total)})"
            )
        except OSError as e:
            logging.warning(f"[Converter #{job_id}] Could not remove {sra_path} after fasterq-dump: {e}")

        logging.info(f"[Converter #{job_id}] fasterq-dump done for {accession}, queueing pigz ...")

        try:
            pigz_task_queue.put(
                (
                    job_id,
                    sra_path,
                    job_output_dir,
                    threads,
                    t_fasterq_start,
                    t_fasterq_done,
                )
            )
            handed_to_pigz = True
        except Exception as e:
            logging.error(f"[Converter #{job_id}] Failed to queue pigz task for {accession}: {e}")
            _cleanup_dir(job_output_dir)
            result_queue.put((sra_path, [], False, t_fasterq_start, t_fasterq_done, 0.0, 0.0))
            result_emitted = True

    except Exception as e:
        logging.error(
            f"[Converter #{job_id}] _fasterq_worker fatal error for "
            f"{accession}: {e}"
        )
        import traceback
        logging.error(traceback.format_exc())
        _cleanup_dir(job_temp_dir)
        if not handed_to_pigz:
            _cleanup_dir(job_output_dir)
            if not result_emitted:
                try:
                    result_queue.put(
                        (sra_path, [], False, t_fasterq_start, t_fasterq_done, 0.0, 0.0)
                    )
                    result_emitted = True
                except Exception:
                    pass
    finally:
        # This must cover the entire worker body. The old code only decremented
        # after fasterq-dump had launched; a pre-launch exception, such as an
        # output/temp directory failure in the child, leaked active_fasterq_jobs
        # and left the converter reporting one active fasterq job forever.
        try:
            _decrement_shared_counter(active_fasterq_jobs)
        except Exception as exc:
            logging.error(
                f"[Converter #{job_id}] Failed to decrement active_fasterq_jobs: {exc}"
            )
        if not handed_to_pigz and not result_emitted:
            try:
                result_queue.put(
                    (sra_path, [], False, t_fasterq_start, t_fasterq_done, 0.0, 0.0)
                )
            except Exception:
                pass


def _pigz_worker(
    job_id: int,
    sra_path: str,
    job_output_dir: str,
    threads: int,
    t_fasterq_start: float,
    t_fasterq_done: float,
    compressed_output_dir: str,
    result_queue,
    byte_counter,
    removed_fastq_counter,
    active_pigz_jobs,
):
    """Compress fasterq outputs to .fastq.gz directly on the slow tier.
 
    Key change vs the prior implementation: pigz stderr is redirected to a
    real on-disk file rather than subprocess.PIPE. The original used PIPE
    without a reader thread, which deadlocks proc.wait() the moment pigz
    writes more than the kernel pipe buffer (~64 KB) of stderr. Files have
    no fixed buffer; the deadlock condition cannot exist.
 
    Additional defenses:
      * stdin=subprocess.DEVNULL so pigz cannot inadvertently block on read.
      * try/finally around the entire body so active_pigz_jobs is
        decremented on every exit path (success, failure, exception, kill).
      * 1800 s hard deadline per pigz invocation. Above any legitimate
        pigz -1 runtime for SRA FASTQs; if ever tripped, that is a separate
        bug worth investigating before raising the constant.
      * stderr sidecar file is removed on success when empty; kept on disk
        otherwise so the next person debugging has something to read.
 
    Result tuple shape preserved:
        (sra_path, fastq_gz_files, success,
         t_fasterq_start, t_fasterq_done, t_pigz_start, t_pigz_done)
    """
    DEADLINE_SEC = 1800.0
    accession = os.path.basename(sra_path).split(".")[0]
    logging.info(f"[Converter #{job_id}] Starting pigz for {accession}")
 
    t_pigz_start = time.time()
    fastq_gz_files = []
    removed_fastq_bytes_job = 0
    result_emitted = False
 
    try:
        # Locate the .fastq files fasterq-dump emitted for this job.
        try:
            fastq_files = [
                os.path.join(job_output_dir, f)
                for f in os.listdir(job_output_dir)
                if f.endswith(".fastq")
            ]
        except OSError as e:
            logging.error(
                f"[Converter #{job_id}] Could not list job_output_dir "
                f"{job_output_dir}: {e}"
            )
            result_queue.put(
                (sra_path, [], False, t_fasterq_start, t_fasterq_done,
                 t_pigz_start, 0.0)
            )
            result_emitted = True
            return
 
        if not fastq_files:
            logging.error(
                f"[Converter #{job_id}] No .fastq files found after "
                f"fasterq-dump for {accession}"
            )
            _cleanup_dir(job_output_dir)
            result_queue.put(
                (sra_path, [], False, t_fasterq_start, t_fasterq_done,
                 t_pigz_start, 0.0)
            )
            result_emitted = True
            return
 
        try:
            os.makedirs(compressed_output_dir, exist_ok=True)
        except OSError as e:
            logging.error(
                f"[Converter #{job_id}] Could not create compressed_output_dir "
                f"{compressed_output_dir}: {e}"
            )
            _cleanup_dir(job_output_dir)
            result_queue.put(
                (sra_path, [], False, t_fasterq_start, t_fasterq_done,
                 t_pigz_start, 0.0)
            )
            result_emitted = True
            return
 
        # Process each .fastq sequentially. The earlier implementation
        # launched concurrent pigz across split outputs of the same job,
        # but that complicated stderr handling for no real throughput gain:
        # the outer dispatcher already runs multiple jobs in parallel and
        # pigz itself is multi-threaded via -p.
        for fq in fastq_files:
            gz_path = os.path.join(
                compressed_output_dir, os.path.basename(fq) + ".gz"
            )
            stderr_path = gz_path + ".pigz.stderr"
 
            out_fh = None
            err_fh = None
            proc = None
            prev_gz_size = 0
            started_at = time.monotonic()
 
            try:
                out_fh = open(gz_path, "wb")
                err_fh = open(stderr_path, "wb")
 
                proc = subprocess.Popen(
                    ["pigz", "-1", "-c", "-p", str(max(1, threads)), fq],
                    stdin=subprocess.DEVNULL,
                    stdout=out_fh,
                    stderr=err_fh,
                )
 
                # Bounded poll loop. Periodically samples the partial .gz
                # size to update the global throughput counter.
                while True:
                    try:
                        rc = proc.wait(timeout=1.0)
                        # Final size accounting after pigz exits.
                        try:
                            out_fh.close()
                        except Exception:
                            pass
                        out_fh = None
                        if os.path.exists(gz_path):
                            cur_gz_size = os.path.getsize(gz_path)
                            delta = cur_gz_size - prev_gz_size
                            if delta > 0:
                                with byte_counter.get_lock():
                                    byte_counter.value += delta
                        break
                    except subprocess.TimeoutExpired:
                        if (time.monotonic() - started_at) > DEADLINE_SEC:
                            logging.error(
                                f"[Converter #{job_id}] pigz exceeded "
                                f"{DEADLINE_SEC:.0f}s deadline for {fq}; "
                                f"killing process"
                            )
                            try:
                                proc.kill()
                                proc.wait(timeout=5.0)
                            except Exception as kill_exc:
                                logging.error(
                                    f"[Converter #{job_id}] kill failed: "
                                    f"{kill_exc}"
                                )
                            raise RuntimeError(
                                f"pigz hard-deadline exceeded for {fq}"
                            )
                        # Intermediate throughput sample.
                        if os.path.exists(gz_path):
                            cur_gz_size = os.path.getsize(gz_path)
                            delta = cur_gz_size - prev_gz_size
                            if delta > 0:
                                with byte_counter.get_lock():
                                    byte_counter.value += delta
                                prev_gz_size = cur_gz_size
 
                if rc != 0:
                    # Read a tail of stderr for log triage.
                    try:
                        if err_fh is not None:
                            err_fh.flush()
                    except Exception:
                        pass
                    try:
                        with open(stderr_path, "rb") as f:
                            tail = f.read()[-512:].decode(errors="replace").strip()
                    except Exception:
                        tail = "(could not read stderr file)"
                    logging.error(
                        f"[Converter #{job_id}] pigz failed (rc={rc}) for "
                        f"{fq}: {tail}"
                    )
                    # Remove the (likely truncated) .gz so a corrupt artifact
                    # never reaches the slow tier as a downstream input.
                    try:
                        if os.path.exists(gz_path):
                            os.remove(gz_path)
                    except OSError:
                        pass
                    # Also clean any sibling .gz produced earlier in this loop.
                    for prev_gz in fastq_gz_files:
                        try:
                            if os.path.exists(prev_gz):
                                os.remove(prev_gz)
                        except OSError:
                            pass
                    fastq_gz_files.clear()
                    _cleanup_dir(job_output_dir)
                    result_queue.put(
                        (sra_path, [], False, t_fasterq_start, t_fasterq_done,
                         t_pigz_start, 0.0)
                    )
                    result_emitted = True
                    return
 
                # Success for this .fastq. Record the .gz and remove source.
                if os.path.exists(gz_path):
                    fastq_gz_files.append(gz_path)
                if os.path.exists(fq):
                    try:
                        removed_size = os.path.getsize(fq)
                        os.remove(fq)
                        removed_fastq_bytes_job += removed_size
                        with removed_fastq_counter.get_lock():
                            removed_fastq_counter.value += removed_size
                        logging.info(
                            f"[Converter #{job_id}] Removed source FASTQ "
                            f"after compression: {fq} "
                            f"({_human_bytes(removed_size)})"
                        )
                    except OSError as e:
                        logging.warning(
                            f"[Converter #{job_id}] Could not remove source "
                            f"FASTQ {fq}: {e}"
                        )
 
            finally:
                if out_fh is not None:
                    try:
                        out_fh.close()
                    except Exception:
                        pass
                if err_fh is not None:
                    try:
                        err_fh.close()
                    except Exception:
                        pass
                # Remove the stderr sidecar only if pigz emitted nothing AND
                # this iteration succeeded. Leave it on failure for triage.
                try:
                    if os.path.exists(stderr_path):
                        if (proc is not None
                                and proc.returncode == 0
                                and os.path.getsize(stderr_path) == 0):
                            os.unlink(stderr_path)
                except OSError:
                    pass
 
        # All .fastq files compressed successfully.
        t_pigz_done = time.time()
        logging.info(
            f"[Converter #{job_id}] Completed {accession}: "
            f"{[os.path.basename(f) for f in fastq_gz_files]} "
            f"(written directly to slow tier: {compressed_output_dir})"
        )
        if removed_fastq_bytes_job > 0:
            logging.info(
                f"[Converter #{job_id}] Compression cleanup reclaimed "
                f"{_human_bytes(removed_fastq_bytes_job)} for {accession}"
            )
        _cleanup_dir(job_output_dir)
        result_queue.put(
            (sra_path, fastq_gz_files, True,
             t_fasterq_start, t_fasterq_done,
             t_pigz_start, t_pigz_done)
        )
        result_emitted = True
 
    except Exception as e:
        logging.error(
            f"[Converter #{job_id}] _pigz_worker fatal error for "
            f"{accession}: {e}"
        )
        import traceback
        logging.error(traceback.format_exc())
        try:
            _cleanup_dir(job_output_dir)
        except Exception:
            pass
        if not result_emitted:
            try:
                result_queue.put(
                    (sra_path, [], False, t_fasterq_start, t_fasterq_done,
                     t_pigz_start, 0.0)
                )
                result_emitted = True
            except Exception:
                pass
 
    finally:
        # CRITICAL INVARIANT: active_pigz_jobs must be decremented on every
        # exit path. If it leaks, the dispatcher's "drain active_pigz to 0"
        # loop never terminates, the benchmark hangs, and SLURM eventually
        # walltime-kills the job. This is exactly the failure mode that
        # produced the all-zero throughput tail in the original log.
        try:
            _decrement_shared_counter(active_pigz_jobs)
        except Exception as exc:
            logging.error(
                f"[Converter #{job_id}] Failed to decrement "
                f"active_pigz_jobs: {exc}"
            )
        # Also guarantee the result queue receives exactly one tuple per job.
        # The dispatcher and collector both depend on this 1:1 correspondence.
        if not result_emitted:
            try:
                result_queue.put(
                    (sra_path, [], False, t_fasterq_start, t_fasterq_done,
                     t_pigz_start, 0.0)
                )
            except Exception:
                pass



#############################
# Throughput reporter
#############################

def _report_conversion_throughput(
    byte_counter: mp.Value,
    active_jobs: mp.Value,
    active_fasterq_jobs: mp.Value,
    active_pigz_jobs: mp.Value,
    throughput_logs: deque,
    throughput_lock: Lock,
    stop_event,
    log_dir: str = "logs",
    fastq_dir: str = os.path.join(get_seqflux_work_dir(), "fastq"),
    compressed_output_dir: Optional[str] = None,
):
    """
    Logs conversion throughput (MB/s of output) once per second.
    Tracks fasterq-dump (.fastq files on fast tier) and pigz (.fastq.gz files
    on slow tier) SEPARATELY. With the FFS-direct pipeline, .fastq.gz files
    live in compressed_output_dir (slow tier), not under fastq_dir.
    Continues monitoring until all active jobs complete (not just until
    stop_event).
    """
    os.makedirs(log_dir, exist_ok=True)
    t = time.time()
    fname = os.path.join(
        log_dir,
        f"seqflux/log_conversion_{datetime.datetime.fromtimestamp(t).strftime('%Y%m%d_%H%M%S')}.csv"
    )
    
    try:
        with open(fname, "w") as f:
            f.write(
                "timestamp,elapsed_sec,convert_mbs,compress_mbs,total_mbs,"
                "active_jobs,active_fasterq_jobs,active_pigz_jobs,overlap,fastq_mb,fastqgz_mb\n"
            )

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
                fasterq_jobs = active_fasterq_jobs.value
                pigz_jobs = active_pigz_jobs.value
                overlap_active = int(fasterq_jobs > 0 and pigz_jobs > 0)

                # Measure .fastq (fasterq-dump output, on fast tier) and
                # .fastq.gz (pigz output, on slow tier) SEPARATELY.
                fastq_bytes = 0
                fastqgz_bytes = 0
                if os.path.exists(fastq_dir):
                    for root, dirs, files in os.walk(fastq_dir):
                        for f in files:
                            fpath = os.path.join(root, f)
                            try:
                                fsize = os.path.getsize(fpath)
                                if f.endswith('.fastq.gz'):
                                    # Older runs may still write .gz next to
                                    # .fastq if compressed_output_dir is unset.
                                    fastqgz_bytes += fsize
                                elif f.endswith('.fastq'):
                                    fastq_bytes += fsize
                            except OSError:
                                pass  # File might have been deleted
                if compressed_output_dir and os.path.exists(compressed_output_dir):
                    for root, dirs, files in os.walk(compressed_output_dir):
                        for f in files:
                            if not f.endswith('.fastq.gz'):
                                continue
                            fpath = os.path.join(root, f)
                            try:
                                fastqgz_bytes += os.path.getsize(fpath)
                            except OSError:
                                pass

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
                            f"(total: {fastq_mb}MB .fastq, active_fasterq: {fasterq_jobs}, "
                            f"active_pigz: {pigz_jobs})"
                        )
                    if compress_mbs > 0:
                        fasterq_state = ""
                        if fasterq_jobs > 0 and convert_mbs == 0:
                            fasterq_state = ", fasterq_active_no_fastq_growth"
                        logging.info(
                            f"pigz @{elapsed}s: {compress_mbs}MB/s "
                            f"(total: {fastqgz_mb}MB .fastq.gz, active_fasterq: {fasterq_jobs}, "
                            f"active_pigz: {pigz_jobs}{fasterq_state})"
                        )
                    # If no throughput but jobs are running, they're still processing
                    # (e.g., fasterq-dump extracting in temp space before writing output)
                    if convert_mbs == 0 and compress_mbs == 0:
                        phase_hint = "overlap active" if overlap_active else "single-stage active"
                        logging.info(
                            f"Conversion @{elapsed}s: 0MB/s ({phase_hint}, "
                            f"active_fasterq: {fasterq_jobs}, active_pigz: {pigz_jobs}, "
                            f"active_total: {jobs})"
                        )
                else:
                    # No jobs running - truly idle
                    logging.info(f"Conversion @{elapsed}s: idle")
                
                with open(fname, "a") as f:
                    f.write(
                        f"{t1},{elapsed},{convert_mbs},{compress_mbs},{total_mbs},"
                        f"{jobs},{fasterq_jobs},{pigz_jobs},{overlap_active},{fastq_mb},{fastqgz_mb}\n"
                    )
                    
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
        Completion queue — receives absolute .fastq.gz file paths after
        compression. With the FFS-direct pipeline these paths already live
        on the slow tier (compressed_output_dir); the queue is retained as a
        completion signal for downstream callers (e.g., for accounting) and
        is NOT consumed by any internal mover.
    work_dir : str
        FAST-TIER working directory. The converter creates ``work_dir/fastq``
        for fasterq-dump output and ``work_dir/tmp`` for fasterq-dump's
        ``--temp`` staging. Both should resolve to the fast-tier device
        (NVMe / tmpfs / lustre depending on the deployment).
    compressed_output_dir : str
        SLOW-TIER destination where pigz writes ``.fastq.gz`` files directly
        via ``pigz -c``. If None, defaults to ``work_dir/fastq`` for
        backward compatibility (i.e., fast-tier compression with no FFS
        separation).
    nvme_device : str
        Bare device name for /proc/diskstats, e.g. 'nvme0n1'.
    threads_per_job : int
        --threads passed to fasterq-dump and pigz.
    cpu_threshold : float
        CPU % ceiling for admission gate (default 85.0).
    nvme_threshold : float
        NVMe utilization % ceiling for admission gate (default 92.0).
    max_jobs : int | None
        Maximum concurrent fasterq-dump jobs. None → cpu_count.
    max_pigz_jobs : int | None
        Maximum concurrent pigz compression jobs. None → max_jobs.
    required_size_factor : float
        Target peak footprint factor relative to incoming SRA size.
        Admission rule uses additional runway only:
        (free_bytes - reserved_bytes) >=
        ((required_size_factor - 1.0) * sra_size + safety_margin).
        The `-1.0` accounts for the source .sra already occupying disk.
    reserve_size_factor : float
        Reservation factor charged to the shared reservation counter at job
        launch and released on completion. Example: 8.0 means reserve 8x SRA.
    pigz_reserve_factor : float
        Reservation factor applied once a job enters pigz compression.
        This should be lower than reserve_size_factor because fasterq scratch
        pressure has ended and only compressed outputs remain in-flight.
    disk_safety_margin_gb : float
        Extra free-space buffer (GB) added to each admission decision.
    shared_reserved_bytes : mp.Value | None
        Optional shared reservation counter used across pipeline stages
        (e.g., downloader + converter) to prevent cross-stage overcommit.
    shared_pending_headroom_bytes : mp.Value | None
        Optional shared signal containing the disk runway needed to keep at
        least one queued conversion job admissible while the rest of the
        deferred SRA files remain on disk.
    output_size_factor : float | None
        Legacy alias for required_size_factor. If provided, it overrides the
        default required factor unless required_size_factor is explicitly set.
    probing_sec : float
        Poll interval inside admission gate (seconds).
    """

    def __init__(
        self,
        processing_queue: mp.Queue,
        move_queue: mp.Queue,
        work_dir: str = get_seqflux_work_dir(),
        compressed_output_dir: Optional[str] = None,
        nvme_device: str = get_nvme_device(),
        threads_per_job: int = 8,
        cpu_threshold: float = 85.0,
        nvme_threshold: float = 92.0,
        max_jobs: Optional[int] = None,
        max_pigz_jobs: Optional[int] = None,
        required_size_factor: float = 12.0,
        reserve_size_factor: float = 12.0,
        pigz_reserve_factor: float = 3.5,
        disk_safety_margin_gb: float = 0.0,
        shared_reserved_bytes: Optional[mp.Value] = None,
        shared_pending_headroom_bytes: Optional[mp.Value] = None,
        output_size_factor: Optional[float] = None,
        probing_sec: float = 1.0,
    ):
        self.processing_queue = processing_queue
        self.move_queue       = move_queue
        self.work_dir         = work_dir
        self.fastq_dir        = os.path.join(work_dir, "fastq")
        self.temp_dir         = os.path.join(work_dir, "tmp")
        # Slow-tier destination for pigz output. If unset, fall back to the
        # fastq staging directory so existing callers that did not pass this
        # argument still get a (degenerate, fast-tier-only) working pipeline.
        self.compressed_output_dir = compressed_output_dir or self.fastq_dir
        self.nvme_device      = nvme_device
        self.threads_per_job  = threads_per_job
        self.cpu_threshold    = cpu_threshold
        self.nvme_threshold   = nvme_threshold
        self.max_jobs         = max_jobs or mp.cpu_count()
        self.max_pigz_jobs    = max_pigz_jobs or max(1, self.max_jobs)

        # Backward compatibility: older call sites passed output_size_factor.
        if output_size_factor is not None and required_size_factor == 10.0:
            required_size_factor = float(output_size_factor)

        self.required_size_factor = max(1.0, float(required_size_factor))
        self.reserve_size_factor = max(0.0, float(reserve_size_factor))
        self.pigz_reserve_factor = max(0.0, float(pigz_reserve_factor))
        self.disk_safety_margin_bytes = max(0, int(float(disk_safety_margin_gb) * (1024 ** 3)))
        self.probing_sec      = probing_sec

        # Shared state
        self._active_jobs     = mp.Value("i", 0)
        self._active_fasterq_jobs = mp.Value("i", 0)
        self._active_pigz_jobs = mp.Value("i", 0)
        self._byte_counter    = mp.Value("Q", 0)   # unsigned 64-bit
        self._converted_count = mp.Value("i", 0)
        self._failed_count    = mp.Value("i", 0)
        self._removed_sra_bytes = mp.Value("Q", 0)
        self._removed_fastq_bytes = mp.Value("Q", 0)

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
        self._pigz_task_queue = mp.Queue()
        self._phase_queue     = mp.Queue()

        # Throughput tracking
        self._throughput_logs = deque(maxlen=10000)
        self._throughput_lock = Lock()

        # Aggregate disk reservation tracking for conversion jobs.
        # This prevents over-admission when each individual job passes
        # free-space checks but the sum of active jobs exceeds headroom.
        self._disk_reserved_bytes = shared_reserved_bytes if shared_reserved_bytes is not None else mp.Value("Q", 0)
        self._shared_pending_headroom_bytes = shared_pending_headroom_bytes
        self._reserved_by_sra = {}
        self._reserve_lock = Lock()
        self._using_shared_reservation = shared_reserved_bytes is not None
        self._disk_deferral_counts = {}

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
        self._worker_proc_meta = {}
        self._completed_sra_paths = set()
        self._completed_lock = Lock()
        self._job_id_counter  = 0

        os.makedirs(self.fastq_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        try:
            os.makedirs(self.compressed_output_dir, exist_ok=True)
        except OSError as e:
            logging.warning(
                f"[SRAConverter] Could not create compressed_output_dir "
                f"{self.compressed_output_dir} at init: {e} (will retry per job)"
            )

    # ── Public API ──────────────────────────────────────────────────────────

    def start(self):
        """Start dispatcher, result collector, and throughput reporter threads."""
        t_dispatch = Thread(target=self._dispatcher_loop,        name="conv-dispatcher", daemon=True)
        t_pigz     = Thread(target=self._pigz_dispatcher_loop,   name="conv-pigz",       daemon=True)
        t_phase    = Thread(target=self._phase_listener,         name="conv-phase",      daemon=True)
        t_collect  = Thread(target=self._result_collector_loop,  name="conv-collector",  daemon=True)
        t_monitor  = Thread(target=self._worker_monitor_loop,    name="conv-monitor",    daemon=True)
        t_report   = Thread(
            target=_report_conversion_throughput,
            args=(
                self._byte_counter,
                self._active_jobs,
                self._active_fasterq_jobs,
                self._active_pigz_jobs,
                self._throughput_logs,
                self._throughput_lock,
                self._stop_event,
                "logs",
                self.fastq_dir,                # fast-tier directory for .fastq
                self.compressed_output_dir,    # slow-tier directory for .fastq.gz
            ),
            name="conv-reporter",
            daemon=True,
        )

        for t in (t_dispatch, t_pigz, t_phase, t_collect, t_monitor, t_report):
            t.start()
            self._threads.append(t)

        logging.info(
            f"[SRAConverter] Started — max_fasterq_jobs={self.max_jobs}, "
            f"max_pigz_jobs={self.max_pigz_jobs}, "
            f"threads_per_job={self.threads_per_job}, "
            f"nvme_device={self.nvme_device}, "
            f"cpu_threshold={self.cpu_threshold}%, "
            f"nvme_threshold={self.nvme_threshold}%, "
            f"required_size_factor={self.required_size_factor}x, "
            f"reserve_size_factor={self.reserve_size_factor}x, "
            f"pigz_reserve_factor={self.pigz_reserve_factor}x, "
            f"disk_safety_margin_gb={round(self.disk_safety_margin_bytes / (1024 ** 3), 2)}, "
            f"shared_disk_reservation={self._using_shared_reservation}, "
            f"compressed_output_dir={self.compressed_output_dir}"
        )

    def _estimate_space_targets(self, sra_path: str) -> tuple:
        """
        Estimate disk-space values for one conversion job.
        Returns (sra_size_bytes, required_bytes, reserved_bytes).
        """
        try:
            sra_size = os.path.getsize(sra_path)
        except OSError as e:
            logging.warning(
                f"[SRAConverter] Could not stat {sra_path} for disk admission: {e}. "
                "Falling back to safety margin only."
            )
            sra_size = 0

        # Only additional headroom is needed at admission time because the
        # source .sra is already occupying space on disk.
        required_growth_factor = max(0.0, self.required_size_factor - 1.0)
        required_bytes = int(sra_size * required_growth_factor) + self.disk_safety_margin_bytes
        reserved_bytes = int(sra_size * self.reserve_size_factor)
        return sra_size, required_bytes, reserved_bytes

    def _wait_for_disk_headroom(self, sra_path: str, required_bytes: int, sra_size: int):
        """
        Block until logical disk admission passes.
        Rule: (free_bytes - reserved_before) >= required_bytes.
        """
        wait_cycles = 0
        while True:
            try:
                free_bytes = shutil.disk_usage(self.work_dir).free
            except OSError as e:
                logging.warning(
                    f"[SRAConverter] disk_usage failed for {self.work_dir}: {e}; retrying"
                )
                time.sleep(self.probing_sec)
                continue

            with self._disk_reserved_bytes.get_lock():
                reserved_before = int(self._disk_reserved_bytes.value)

            effective_free = max(0, free_bytes - reserved_before)

            if effective_free >= required_bytes:
                if wait_cycles > 0:
                    logging.info(
                        f"[SRAConverter] Disk admission granted for {os.path.basename(sra_path)} "
                        f"(free-reserved {effective_free / (1024 ** 3):.2f}GB >= "
                        f"required {required_bytes / (1024 ** 3):.2f}GB; "
                        f"sra={sra_size / (1024 ** 3):.2f}GB, "
                        f"free={free_bytes / (1024 ** 3):.2f}GB, "
                        f"reserved={reserved_before / (1024 ** 3):.2f}GB)"
                    )
                return

            wait_cycles += 1
            # Emit every ~5 polling cycles to avoid log spam.
            if wait_cycles == 1 or wait_cycles % 5 == 0:
                logging.info(
                    f"[SRAConverter] Waiting for disk space for {os.path.basename(sra_path)} "
                    f"(free-reserved {effective_free / (1024 ** 3):.2f}GB < "
                    f"required {required_bytes / (1024 ** 3):.2f}GB; "
                    f"sra={sra_size / (1024 ** 3):.2f}GB, "
                    f"free={free_bytes / (1024 ** 3):.2f}GB, "
                    f"reserved={reserved_before / (1024 ** 3):.2f}GB)"
                )
            time.sleep(self.probing_sec)

    def _disk_headroom_snapshot(self, required_bytes: int) -> tuple:
        """
        Return current disk-admission state as
        (has_headroom, free_bytes, reserved_bytes, effective_free_bytes).
        """
        free_bytes = shutil.disk_usage(self.work_dir).free
        with self._disk_reserved_bytes.get_lock():
            reserved_before = int(self._disk_reserved_bytes.value)
        effective_free = max(0, free_bytes - reserved_before)
        return effective_free >= required_bytes, free_bytes, reserved_before, effective_free

    def _publish_pending_headroom(self, required_bytes: int):
        """Publish queued conversion runway requirement for download admission."""
        if self._shared_pending_headroom_bytes is None:
            return
        with self._shared_pending_headroom_bytes.get_lock():
            self._shared_pending_headroom_bytes.value = max(0, int(required_bytes))

    def _reserve_disk_for_job(self, sra_path: str, reserved_bytes: int) -> int:
        """Register reserved bytes for a job after admission, before launch."""
        with self._reserve_lock:
            self._reserved_by_sra[sra_path] = reserved_bytes
            with self._disk_reserved_bytes.get_lock():
                self._disk_reserved_bytes.value += reserved_bytes
                return int(self._disk_reserved_bytes.value)

    def _release_disk_reservation(self, sra_path: str) -> int:
        """Release reserved bytes once job result has been collected."""
        with self._reserve_lock:
            reserved_bytes = self._reserved_by_sra.pop(sra_path, 0)
            if reserved_bytes > 0:
                with self._disk_reserved_bytes.get_lock():
                    self._disk_reserved_bytes.value = max(
                        0,
                        self._disk_reserved_bytes.value - reserved_bytes,
                    )
            return int(reserved_bytes)

    def _reduce_disk_reservation_for_pigz(self, sra_path: str, sra_size: int):
        """Reduce reservation for jobs that moved from fasterq-dump to pigz."""
        new_reserved = int(max(0, sra_size) * self.pigz_reserve_factor)
        with self._reserve_lock:
            old_reserved = int(self._reserved_by_sra.get(sra_path, 0))
            delta = old_reserved - new_reserved
            if delta <= 0:
                return

            self._reserved_by_sra[sra_path] = new_reserved
            with self._disk_reserved_bytes.get_lock():
                self._disk_reserved_bytes.value = max(0, self._disk_reserved_bytes.value - delta)

        logging.info(
            f"[SRAConverter] Phase transition for {os.path.basename(sra_path)}: "
            f"reservation {old_reserved / (1024 ** 3):.2f}GB -> "
            f"{new_reserved / (1024 ** 3):.2f}GB "
            f"(released {delta / (1024 ** 3):.2f}GB)"
        )

    def _drain_phase_queue(self):
        """Drain queued phase transitions so reservation state stays consistent."""
        while True:
            try:
                tag, sra_path, sra_size = self._phase_queue.get_nowait()
                if tag == "PIGZ_START":
                    self._reduce_disk_reservation_for_pigz(sra_path, int(sra_size))
            except queue.Empty:
                break

    def _phase_listener(self):
        """Process worker phase-transition events during conversion."""
        while not self._stop_event.is_set() or self._active_jobs.value > 0:
            try:
                tag, sra_path, sra_size = self._phase_queue.get(timeout=1.0)
                if tag == "PIGZ_START":
                    self._reduce_disk_reservation_for_pigz(sra_path, int(sra_size))
            except queue.Empty:
                continue

        self._drain_phase_queue()

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

        # Process any phase events that raced with shutdown so reservation
        # accounting does not miss the final transition updates.
        self._drain_phase_queue()

        # Reap worker procs — only terminate those still alive after the wait.
        with self._procs_lock:
            for p in self._worker_procs:
                if p.is_alive():
                    if hasattr(p, "terminate"):
                        p.terminate()
                        p.join(timeout=5)
                    else:
                        logging.warning(
                            f"[SRAConverter] Worker thread {p.name} still alive at shutdown; "
                            "external tool timeout will clean it up"
                        )

        logging.info(
            f"[SRAConverter] Stopped — "
            f"converted={self._converted_count.value}, "
            f"failed={self._failed_count.value}, "
            f"cleanup_sra={_human_bytes(self._removed_sra_bytes.value)}, "
            f"cleanup_fastq={_human_bytes(self._removed_fastq_bytes.value)}"
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

    def _pigz_dispatcher_loop(self):
        """Launch pigz workers from completed fasterq tasks with independent limits."""
        while (
            not self._stop_event.is_set()
            or self._active_jobs.value > 0
            or self._active_pigz_jobs.value > 0
        ):
            if self._active_pigz_jobs.value >= self.max_pigz_jobs:
                time.sleep(0.2)
                continue

            try:
                job_id, sra_path, job_output_dir, threads, t_fasterq_start, t_fasterq_done = (
                    self._pigz_task_queue.get(timeout=1.0)
                )
            except queue.Empty:
                continue

            p = Thread(
                target=_pigz_worker,
                args=(
                    job_id,
                    sra_path,
                    job_output_dir,
                    threads,
                    t_fasterq_start,
                    t_fasterq_done,
                    self.compressed_output_dir,
                    self._result_queue,
                    self._byte_counter,
                    self._removed_fastq_bytes,
                    self._active_pigz_jobs,
                ),
                name=f"conv-pigz-worker-{job_id}",
                daemon=True,
            )

            with self._active_pigz_jobs.get_lock():
                self._active_pigz_jobs.value += 1

            try:
                p.start()
            except Exception:
                _decrement_shared_counter(self._active_pigz_jobs)
                self._result_queue.put((sra_path, [], False, t_fasterq_start, t_fasterq_done, 0.0, 0.0))
                continue

            with self._procs_lock:
                self._worker_procs.append(p)
                self._worker_proc_meta[id(p)] = {
                    "kind": "pigz",
                    "job_id": job_id,
                    "sra_path": sra_path,
                    "t_fasterq_start": t_fasterq_start,
                    "t_fasterq_done": t_fasterq_done,
                    "t_pigz_start": 0.0,
                }

            logging.info(
                f"[SRAConverter] Pigz job #{job_id} started for {os.path.basename(sra_path)} "
                f"(active pigz jobs: {self._active_pigz_jobs.value})"
            )

    def _worker_monitor_loop(self):
        """
        Watch worker processes for abnormal exits that bypass result emission.

        Normal fasterq workers exit with code 0 after handing work to pigz, and
        normal pigz workers exit with code 0 after publishing a result tuple.
        A non-zero worker exit means the process died outside the protected
        Python path; synthesize one failure result so active job accounting can
        drain instead of hanging forever.
        """
        while (
            not self._stop_event.is_set()
            or self._active_jobs.value > 0
            or self._active_fasterq_jobs.value > 0
            or self._active_pigz_jobs.value > 0
        ):
            with self._procs_lock:
                procs = list(self._worker_procs)

            for p in procs:
                if p.is_alive():
                    continue

                try:
                    p.join(timeout=0)
                except Exception:
                    pass

                with self._procs_lock:
                    meta = self._worker_proc_meta.get(id(p))

                if meta is None:
                    continue

                exitcode = getattr(p, "exitcode", 0)

                if exitcode in (0, None):
                    with self._procs_lock:
                        self._worker_proc_meta.pop(id(p), None)
                        self._worker_procs = [q for q in self._worker_procs if q is not p]
                    continue

                sra_path = meta["sra_path"]
                with self._completed_lock:
                    already_completed = sra_path in self._completed_sra_paths

                with self._procs_lock:
                    self._worker_proc_meta.pop(id(p), None)
                    self._worker_procs = [q for q in self._worker_procs if q is not p]

                if already_completed:
                    continue

                if meta["kind"] == "fasterq":
                    _decrement_shared_counter(self._active_fasterq_jobs)
                    t_fasterq_start = meta.get("t_fasterq_start", 0.0)
                    t_fasterq_done = 0.0
                    t_pigz_start = 0.0
                else:
                    _decrement_shared_counter(self._active_pigz_jobs)
                    t_fasterq_start = meta.get("t_fasterq_start", 0.0)
                    t_fasterq_done = meta.get("t_fasterq_done", 0.0)
                    t_pigz_start = meta.get("t_pigz_start", 0.0)

                logging.error(
                    f"[SRAConverter] {meta['kind']} worker #{meta['job_id']} "
                    f"for {os.path.basename(sra_path)} exited unexpectedly "
                    f"(exitcode={exitcode}); marking conversion failed"
                )
                self._result_queue.put(
                    (sra_path, [], False, t_fasterq_start, t_fasterq_done, t_pigz_start, 0.0)
                )

            time.sleep(1.0)

    def _dispatcher_loop(self):
        """
        Pulls .sra paths from processing_queue one at a time.
        Before launching each job:
                    1. Waits for active_fasterq_jobs < max_jobs (fasterq pool ceiling)
          2. Waits for AdmissionGate to grant based on CPU + NVMe utilization
                Then spawns a fasterq worker process for that job.
        Sets _dispatcher_done when it exits so stop() knows it is safe to
        raise _stop_event (Bug #6 fix).
        """
        gate = AdmissionGate(
            nvme_device=self.nvme_device,
            cpu_threshold=self.cpu_threshold,
            nvme_threshold=self.nvme_threshold,
            poll_interval=self.probing_sec,
        )

        # Once we consume the terminal sentinel from processing_queue, no new
        # conversion inputs will arrive. Keep draining deferred files without
        # requeueing the sentinel to avoid an infinite sentinel/deferred loop.
        seen_terminal_sentinel = False
        terminal_deferred_since = None
        terminal_stall_timeout_sec = max(300.0, 30.0 * float(self.probing_sec))

        while not self._stop_event.is_set():
            deferred_paths = []
            deferred_min_required = None
            deferred_min_extra_required = None
            deferred_wait = False
            selected = None

            while not self._stop_event.is_set():
                try:
                    if deferred_paths:
                        sra_path = self.processing_queue.get_nowait()
                    else:
                        sra_path = self.processing_queue.get(timeout=2.0)
                except queue.Empty:
                    break

                if sra_path is None:
                    seen_terminal_sentinel = True
                    break

                import re
                SRA_FILE_RE = re.compile(
                    r'^(?:[SED]RR\d+)$|\.(?:sra|lite\.\d+|\d+)$',
                    re.IGNORECASE,
                )

                if not SRA_FILE_RE.search(os.path.basename(sra_path)):
                    logging.warning(
                        f"[SRAConverter] Skipping non-SRA file (not convertible by fasterq-dump): {sra_path}"
                    )
                    self.move_queue.put(sra_path)
                    continue

                logging.info(f"[SRAConverter] Dequeued: {sra_path}")

                while self._active_fasterq_jobs.value >= self.max_jobs:
                    time.sleep(0.5)

                sra_size, required_bytes, reserved_bytes = self._estimate_space_targets(sra_path)
                has_headroom, free_bytes, reserved_before, effective_free = self._disk_headroom_snapshot(required_bytes)
                if not has_headroom:
                    deferred_paths.append(sra_path)
                    if deferred_min_required is None or required_bytes < deferred_min_required:
                        deferred_min_required = required_bytes
                    # required_bytes is already incremental runway beyond the
                    # source .sra footprint currently on disk.
                    extra_required = required_bytes
                    if deferred_min_extra_required is None or extra_required < deferred_min_extra_required:
                        deferred_min_extra_required = extra_required
                    deferred_wait = True
                    defer_count = self._disk_deferral_counts.get(sra_path, 0) + 1
                    self._disk_deferral_counts[sra_path] = defer_count
                    if defer_count == 1 or defer_count % 5 == 0:
                        logging.info(
                            f"[SRAConverter] Deferring {os.path.basename(sra_path)} for now "
                            f"(free-reserved {effective_free / (1024 ** 3):.2f}GB < "
                            f"required {required_bytes / (1024 ** 3):.2f}GB; "
                            f"sra={sra_size / (1024 ** 3):.2f}GB, "
                            f"free={free_bytes / (1024 ** 3):.2f}GB, "
                            f"reserved={reserved_before / (1024 ** 3):.2f}GB)"
                        )
                    continue

                self._disk_deferral_counts.pop(sra_path, None)
                selected = (sra_path, required_bytes, reserved_bytes, sra_size)
                break

            for deferred_path in deferred_paths:
                self.processing_queue.put(deferred_path)

            # Publish only incremental runway needed by the cheapest deferred
            # conversion. Deferred .sra files already occupy disk and are
            # reflected in free-space snapshots.
            self._publish_pending_headroom(deferred_min_extra_required or 0)

            if selected is None:
                if seen_terminal_sentinel and not deferred_paths:
                    logging.info("[SRAConverter] Received sentinel, dispatcher exiting")
                    self._dispatcher_done.set()
                    break

                if seen_terminal_sentinel and deferred_paths and self._active_jobs.value == 0:
                    now = time.time()
                    if terminal_deferred_since is None:
                        terminal_deferred_since = now
                        logging.warning(
                            f"[SRAConverter] Sentinel received with {len(deferred_paths)} deferred file(s) "
                            "and no active jobs; waiting for external disk relief"
                        )
                    elif (now - terminal_deferred_since) >= terminal_stall_timeout_sec:
                        with self._failed_count.get_lock():
                            self._failed_count.value += len(deferred_paths)
                        self._publish_pending_headroom(0)
                        logging.error(
                            f"[SRAConverter] Deferred queue stalled for "
                            f"{terminal_stall_timeout_sec:.0f}s after sentinel with no active jobs; "
                            f"marking {len(deferred_paths)} deferred file(s) as failed and exiting dispatcher"
                        )
                        self._dispatcher_done.set()
                        break
                else:
                    terminal_deferred_since = None

                if deferred_wait:
                    time.sleep(self.probing_sec)
                continue

            terminal_deferred_since = None

            sra_path, required_bytes, reserved_bytes, sra_size = selected

            # Guard against path disappearance between admission and launch.
            # If the source is gone, mark failed and continue instead of
            # crashing the dispatcher thread.
            if not os.path.exists(sra_path):
                with self._failed_count.get_lock():
                    self._failed_count.value += 1
                logging.error(
                    f"[SRAConverter] Source disappeared before launch: {sra_path}"
                )
                continue

            if self._active_fasterq_jobs.value > 0:
                granted = gate.admit(stop_event=None)
                if not granted:
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
            else:
                logging.info(
                    f"[SRAConverter] Admission gate bypassed (active fasterq jobs: 0) "
                    f"— launching first job for {os.path.basename(sra_path)}"
                )

            self._job_id_counter += 1
            job_id = self._job_id_counter

            p = Thread(
                target=_fasterq_worker,
                args=(
                    job_id,
                    sra_path,
                    sra_size,
                    self.fastq_dir,
                    self.temp_dir,
                    self.threads_per_job,
                    self._pigz_task_queue,
                    self._result_queue,
                    self._phase_queue,
                    self._active_fasterq_jobs,
                    self._removed_sra_bytes,
                ),
                name=f"conv-fasterq-worker-{job_id}",
                daemon=True,
            )
            reserved_after = self._reserve_disk_for_job(sra_path, reserved_bytes)
            # Publish the job as active before the child can emit a result.
            # Otherwise a fast-failing worker can be collected and decremented
            # before this increment happens, leaking _active_jobs by +1.
            with self._active_jobs.get_lock():
                self._active_jobs.value += 1
            with self._active_fasterq_jobs.get_lock():
                self._active_fasterq_jobs.value += 1
            try:
                p.start()
            except Exception:
                with self._active_jobs.get_lock():
                    self._active_jobs.value = max(0, self._active_jobs.value - 1)
                _decrement_shared_counter(self._active_fasterq_jobs)
                self._release_disk_reservation(sra_path)
                raise

            with self._procs_lock:
                self._worker_procs.append(p)
                self._worker_proc_meta[id(p)] = {
                    "kind": "fasterq",
                    "job_id": job_id,
                    "sra_path": sra_path,
                    "t_fasterq_start": time.time(),
                    "t_fasterq_done": 0.0,
                    "t_pigz_start": 0.0,
                }

            logging.info(
                f"[SRAConverter] Job #{job_id} started for {os.path.basename(sra_path)} "
                f"(active jobs: {self._active_jobs.value}, "
                f"active fasterq jobs: {self._active_fasterq_jobs.value}, "
                f"required={required_bytes / (1024 ** 3):.2f}GB, "
                f"reserved_added={reserved_bytes / (1024 ** 3):.2f}GB, "
                f"reserved_total={reserved_after / (1024 ** 3):.2f}GB)"
            )

        self._dispatcher_done.set()

    def _result_collector_loop(self):
        """
        Drains _result_queue.
        On success: pushes each fastq.gz path to move_queue.
        On failure: logs the error; for post-fasterq failures the .sra may
        already be deleted because cleanup now happens right after fasterq-dump.
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
        with self._completed_lock:
            if sra_path in self._completed_sra_paths:
                logging.warning(
                    f"[SRAConverter] Ignoring duplicate conversion result for {sra_path}"
                )
                return
            self._completed_sra_paths.add(sra_path)

        with self._active_jobs.get_lock():
            self._active_jobs.value = max(0, self._active_jobs.value - 1)

        # Release disk reservation for this job as soon as result is collected.
        released_reserved = self._release_disk_reservation(sra_path)
        if released_reserved > 0:
            with self._disk_reserved_bytes.get_lock():
                reserved_left = int(self._disk_reserved_bytes.value)
            logging.info(
                f"[SRAConverter] Released disk reservation for {os.path.basename(sra_path)}: "
                f"{released_reserved / (1024 ** 3):.2f}GB "
                f"(remaining reserved {reserved_left / (1024 ** 3):.2f}GB)"
            )

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
            retained_procs = []
            for p in self._worker_procs:
                if not p.is_alive():
                    p.join(timeout=1)
                    exitcode = getattr(p, "exitcode", 0)
                    if exitcode not in (0, None) and id(p) in self._worker_proc_meta:
                        retained_procs.append(p)
                    else:
                        self._worker_proc_meta.pop(id(p), None)
                else:
                    retained_procs.append(p)
            self._worker_procs = retained_procs

        if success:
            with self._converted_count.get_lock():
                self._converted_count.value += 1

            for gz_path in fastq_gz_files:
                # The file is already on the slow tier; push to move_queue as
                # a completion signal only (downstream drainers no longer move
                # anything, they just acknowledge arrival).
                self.move_queue.put(gz_path)
                logging.info(f"[SRAConverter] → destination (already on slow tier): {gz_path}")

        else:
            with self._failed_count.get_lock():
                self._failed_count.value += 1
            logging.error(
                f"[SRAConverter] Conversion failed for {sra_path}, "
                f"source SRA may already be removed after fasterq-dump"
            )

        logging.info(
            f"[SRAConverter] Status — active: {self._active_jobs.value}, "
            f"done: {self._converted_count.value}, "
            f"failed: {self._failed_count.value}"
        )
