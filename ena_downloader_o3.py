#!/usr/bin/env python3
import os
import shutil
import signal
import time
import sys
import warnings
import datetime
import logging as logger
import numpy as np
import multiprocessing as mp
from threading import Thread
from config_fastbiodl import configurations
from utils import available_space
from search import base_optimizer, gradient_opt_fast, exit_signal

import requests
from typing import List
import argparse

import csv
NCBI_EFETCH = (
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
)  # ?db=sra&id=<ACC>&rettype=runinfo&retmode=text

def get_ncbi_urls(acc: str, field: str = "sra_ftp") -> List[str]:
    """
    Fetch download URLs for a given SRA accession from NCBI’s efetch “runinfo” endpoint.

    Parameters
    ----------
    acc : str
        The Run/Experiment accession (e.g. “SRR1234567”).
    field : str, optional
        Which field of the runinfo CSV to extract URLs from; either
        `"sra_ftp"` (uses `download_path`) or `"fastq_ftp"`. Default is `"sra_ftp"`.

    Returns
    -------
    List[str]
        A list of one or more HTTP/HTTPS URLs. Empty if the accession is
        not found or the requested field isn’t in the CSV header.

    Raises
    ------
    HTTPError
        If the HTTP request to NCBI fails (non-200 status).

    Notes
    -----
    Performs a brief `time.sleep(0.2)` to avoid hammering NCBI in loops.
    """
    
    logger.info(f"Fetching URLs for {acc} from NCBI SRA using field '{field}'")
    # 1) ask runinfo for that accession
    r = requests.get(
        NCBI_EFETCH,
        params={"db": "sra", "id": acc, "rettype": "runinfo", "retmode": "text"},
        timeout=30,
    )
    r.raise_for_status()
    lines = [l for l in r.text.strip().splitlines() if l.strip()]
    if len(lines) < 2:
        return []                      # accession not found / private

    # 2) parse the CSV header → column index we want
    reader = csv.reader(lines)
    header = next(reader)
    data_rows = list(reader)           # usually one row per SRR
    col_map = {"sra_ftp": "download_path", "fastq_ftp": "fastq_ftp"}
    col_name = col_map.get(field, field)
    if col_name not in header:
        return []

    idx = header.index(col_name)

    # 3) collect all URLs (1 or many per row, separated by ';')
    urls: List[str] = []
    for row in data_rows:
        for u in row[idx].split(";"):
            if not u:
                continue
            if "://" not in u:         # some fastq_ftp entries are bare FTP paths
                u = "https://" + u
            urls.append(u)
    
    time.sleep(0.2)
    return urls


# Suppress FutureWarnings
warnings.filterwarnings("ignore", category=FutureWarning)

#############################
# Worker function
#############################
def download_file_worker(process_id):
    """
    Worker process that pulls (url, relative_path) tasks and downloads via HTTP,
    with pause/resume support driven by a shared `download_process_status` flag.

    Parameters
    ----------
    process_id : int
        Index into `download_process_status` that controls whether this worker
        is active (1) or paused (0).

    Side Effects
    ------------
    - Pops tasks off the global `download_tasks` list until empty.
    - Writes partial data to `download_dir/relative_path`, resuming via HTTP Range
      headers if interrupted.
    - Updates `transfer_file_offsets[relative_path]` as bytes accrue.
    - Increments `download_complete` when a file finishes.
    - Logs progress, errors, and re-queues on pause or error.

    Exceptions
    ----------
    Any exception during download is caught; on “Download paused by optimizer” it
    re-queues the task, otherwise it logs the error and re-queues if incomplete.
    """

    logger.info(f"[Download #{process_id}] Worker starting")
    while True:
        if download_process_status[process_id] == 0:
            # Worker is paused; sleep a bit and then check again.
            time.sleep(1)
            continue
        # Grab the next task or exit if none remain
        try:
            url, relative_path = download_tasks.pop(0)
        except IndexError:
            return

        local_temp = os.path.join(download_dir, relative_path)
        os.makedirs(os.path.dirname(local_temp), exist_ok=True)

        # Resume from previous offset if any
        offset = transfer_file_offsets.get(relative_path, 0)
        headers = {'Range': f'bytes={offset}-'} if offset else {}

        logger.debug(f"[Download #{process_id}] Start {relative_path} @ offset {offset}")
        fd = None
        try:
            # Initiate (or resume) HTTP download
            resp = requests.get(url, stream=True, headers=headers, timeout=30)
            resp.raise_for_status()

            # Open file descriptor and seek to offset
            fd = os.open(local_temp, os.O_CREAT | os.O_RDWR)
            os.lseek(fd, offset, os.SEEK_SET)

            for chunk in resp.iter_content(chunk_size=chunk_size):
                # Throttle if tmpfs is low on space
                _, free_now = available_space(download_dir)
                while free_now * 1024 * 1024 <= (len(chunk) + chunk_size):
                    time.sleep(0.5)
                    _, free_now = available_space(download_dir)

                # Pause if optimizer has set this worker to 0
                if download_process_status[process_id] == 0:
                    raise RuntimeError("Download paused by optimizer")

                # Write data and update offset
                os.write(fd, chunk)
                offset += len(chunk)
                transfer_file_offsets[relative_path] = offset

            # Finished download
            os.close(fd); fd = None
            with download_complete.get_lock():
                download_complete.value += 1
            logger.info(f"[Download #{process_id}] Completed {relative_path}")

        except Exception as e:
            msg = str(e)
            if msg == "Download paused by optimizer":
                # Requeue this exact task for later resume
                logger.info(f"[Download #{process_id}] Paused {relative_path} @ offset {offset}")
                download_tasks.append((url, relative_path))
            else:
                # Other error: log and requeue if incomplete
                logger.error(f"[Download #{process_id}] Error {relative_path}: {e}")
                total_len = int(resp.headers.get('Content-Length', offset))
                if offset < total_len:
                    download_tasks.append((url, relative_path))
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except:
                    pass

#############################
# Reporting throughput
#############################
def report_network_throughput():
    """
    Continuously logs per-second and cumulative throughput (Mbps) to both stdout
    and a timestamped CSV file.

    Waits for `start.value` to be set, then every ~1 s:
     - Computes current and average throughput from `transfer_file_offsets`.
     - Appends a row to `log_download_<timestamp>.csv`.
     - Stops when `transfer_done.value == 1` or 1000s of zero-throughput.

    Side Effects
    ------------
    - Writes CSV to working directory.
    - Logs info-level messages via `logger.info`.
    - Updates `transfer_done.value` to 1 if persistent zero throughput.
    """
    previous_total, previous_time = 0, 0
    t = time.time()
    fname = f'log_download_{datetime.datetime.fromtimestamp(t).strftime("%Y%m%d_%H%M%S")}.csv'
    # Wait until the start time is set
    while start.value == 0:
        time.sleep(0.1)
    start_time = start.value
    while transfer_done.value == 0:
        t1 = time.time()
        elapsed = round(t1 - start_time, 1)
        if elapsed > 1000:
            if sum(throughput_logs[-1000:]) == 0:
                transfer_done.value = 1
                break
        if elapsed >= 0.1:
            total_bytes = sum(transfer_file_offsets.values())
            thrpt = round((total_bytes * 8) / (elapsed * 1000 * 1000), 2)
            curr_total = total_bytes - previous_total
            curr_time_sec = round(elapsed - previous_time, 3) or 0.001
            curr_thrpt = round((curr_total * 8) / (curr_time_sec * 1000 * 1000), 2)
            previous_time, previous_total = elapsed, total_bytes
            throughput_logs.append(curr_thrpt)
            logger.info(f"Download Throughput @{elapsed}s: Current: {curr_thrpt}Mbps, Average: {thrpt}Mbps")
            t2 = time.time()
            with open(fname, 'a') as f:
                f.write(f"{t2}, {elapsed}, {curr_thrpt}, {sum(download_process_status)}\n")
            time.sleep(max(0, 1 - (t2 - t1)))

#############################
# Optimizer functions
#############################
def download_probing(params):
    """
    Probe function for the optimizer: toggles worker concurrency, waits, then
    computes a “score” = –(throughput / k^C) for the current concurrency.

    Parameters
    ----------
    params : List[int]
        A one-element list whose first item is the desired concurrency level.

    Returns
    -------
    int
        A negative score (so that optimizers which minimize will maximize actual utility).
    exit_signal
        If `transfer_done.value == 1`, returns the sentinel from `search.exit_signal`.

    Side Effects
    ------------
    - Sets `download_process_status[i] = 1` for the first `C` workers, else 0.
    - Sleeps for `probing_time` seconds (minus a small epsilon).
    - Reads `throughput_logs` and `configurations["K"]`.
    - Logs throughput and score.
    """
    if transfer_done.value == 1:
        return exit_signal
    params = [1 if x < 1 else int(np.round(x)) for x in params]
    logger.info("Download -- Probing Parameters: " + str(params))
    for i in range(len(download_process_status)):
        download_process_status[i] = 1 if i < params[0] else 0
    time.sleep(1)
    n_time = time.time() + probing_time - 1.05
    while time.time() < n_time and transfer_done.value == 0:
        time.sleep(0.1)
    thrpt = np.mean(throughput_logs[-(probing_time-1):]) if len(throughput_logs) > (probing_time-1) else 0
    K = float(configurations["K"])
    cc_impact_nl = K ** params[0]
    score = thrpt / cc_impact_nl if cc_impact_nl != 0 else 0
    score_value = int(np.round(score * (-1)))
    logger.info(f"Download Probing -- Throughput: {int(np.round(thrpt))}Mbps, Score: {score_value}")
    if transfer_done.value == 1:
        return exit_signal
    else:
        return score_value

def run_download_optimizer(probing_func):
    """
    Drives the selected optimization loop (gradient or Bayesian) to adjust
    concurrency parameters over time.

    Parameters
    ----------
    probing_func : Callable
        The function (e.g. `download_probing`) that the optimizer will call
        to evaluate a given concurrency setting.

    Side Effects
    ------------
    - Blocks until `start.value` is nonzero.
    - Launches either `gradient_opt_fast` or `base_optimizer` using
      `configurations["method"]`.
    - Repeatedly calls `probing_func` to refine its suggestion.
    - Logs optimizer start and progress.
    """
    while start.value == 0:
        time.sleep(0.1)
    params = [2]
    method = configurations["method"].lower()
    if method == "gradient":
        logger.info("Running Gradient Optimization for Download....")
        params = gradient_opt_fast(configurations["thread_limit"], probing_func, logger)
    else:
        logger.info("Running Bayesian Optimization for Download....")
        params = base_optimizer(configurations, probing_func, logger)
    while transfer_done.value == 0:
        probing_func(params)


#############################
# Graceful exit handler
#############################
def graceful_exit(signum=None, frame=None):
    """
    Signal handler for SIGINT/SIGTERM that stops all activity and cleans up.

    Parameters
    ----------
    signum : int, optional
        The signal number triggering the exit.
    frame : object, optional
        The current stack frame.

    Side Effects
    ------------
    - Sets `transfer_done.value = 1` to tell other threads to stop.
    - Copies `download_complete` into `move_complete`.
    - Logs any cleanup errors.
    - Calls `sys.exit(1)`.
    """
    logger.debug(f"Graceful exit triggered: signum={signum}, frame={frame}")
    logger.info(f"Graceful exit triggered: signum={signum}, frame={frame}")
    try:
        transfer_done.value = 1
        move_complete.value = download_complete.value
    except Exception as e:
        logger.error(e)
    sys.exit(1)

#############################
# Main function
#############################
if __name__ == '__main__':
    # Setup signal handlers for graceful termination
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

    # make a directory for logs if it does not exist
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Configure logging
    log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
    log_file = f'logs/receiver.{datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}.log'
    if configurations["loglevel"] == "debug":
        logger.basicConfig(
            format=log_FORMAT,
            datefmt='%m/%d/%Y %I:%M:%S %p',
            level=logger.DEBUG,
            handlers=[
                logger.FileHandler(log_file),
                logger.StreamHandler()
            ]
        )
        mp.log_to_stderr(logger.DEBUG)
    else:
        logger.basicConfig(
            format=log_FORMAT,
            datefmt='%m/%d/%Y %I:%M:%S %p',
            level=logger.INFO,
            handlers=[
                logger.FileHandler(log_file),
                logger.StreamHandler()
            ]
        )

    parser = argparse.ArgumentParser(
        description="Parallel NCBI SRA downloader"
    )
    parser.add_argument("-i","--input",   required=True,
                            help="Text file: one accession per line.")
    parser.add_argument("-o","--outdir", default=".",
                    help="Where to save downloads.")
    parser.add_argument("--fastq", action="store_true",
                            help="Use fastq_ftp instead of sra_ftp")
    args = parser.parse_args()


    # Set configuration parameters
    configurations["cpu_count"] = mp.cpu_count()
    if configurations["thread_limit"] == -1:
        configurations["thread_limit"] = configurations["cpu_count"]
    chunk_size = 1024*1024
    probing_time = configurations["probing_sec"]

    # Temporary directory – using shared memory (adjust as needed)
    download_dir = configurations["download_dir"]
    try:
        os.makedirs(download_dir, exist_ok=True)
    except Exception as e:
        logger.error(e)
        sys.exit(1)

    # Shared counters and structures
    download_complete = mp.Value("i", 0)
    move_complete = mp.Value("i", 0)
    transfer_done = mp.Value("i", 0)

    transfer_file_offsets = mp.Manager().dict()  # Bytes downloaded per file.
    throughput_logs = mp.Manager().list() # Download throughput logs.
    download_tasks = mp.Manager().list() # List of FTP download tasks.

    # Read accessions and build download_tasks from NCBI:
    with open(args.input) as f:
        accs = [l.strip() for l in f if l.strip()]
    field = "fastq_ftp" if args.fastq else "sra_ftp"
    for acc in accs:
        try:
            urls = get_ncbi_urls(acc, field)
        except Exception as e:
            logger.error(f"NCBI lookup failed for {acc}: {e}")
            continue
        for url in urls:
            # task: (url, filename)
            download_tasks.append((url, os.path.basename(url)))

    initial_task_count = len(download_tasks)

    logger.info(f"Total files to download: {len(download_tasks)}: {download_tasks}")
    
    num_workers = len(download_tasks)
    # Two sets of process status arrays:
    download_process_status = mp.Array("i", [0 for _ in range(num_workers)])

    # Start download workers.
    download_workers = [mp.Process(target=download_file_worker, args=(i,)) for i in range(num_workers)]
    for p in download_workers:
        p.daemon = True
        p.start()

    # A shared start time for throughput measurement.
    start = mp.Value("d", time.time())
    # Start throughput reporting threads.
    network_report_thread = Thread(target=report_network_throughput)
    network_report_thread.start()
    download_optimizer_thread = Thread(target=run_download_optimizer, args=(download_probing,))
    download_optimizer_thread.start()

    # Wait until all download tasks have been handled.
    while initial_task_count != download_complete.value:
        time.sleep(0.1)
    # Mark downloads as done.
    transfer_done.value = 1
    logger.info("Download Tasks Completed!")
    time.sleep(1)

    # Terminate download worker processes if still alive.
    for p in download_workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)

    logger.info("Transfer Completed!")
    sys.exit(0)
