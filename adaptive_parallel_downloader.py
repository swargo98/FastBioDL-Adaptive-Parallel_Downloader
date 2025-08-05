#!/usr/bin/env python3
import os
import shutil
import signal
import mmap
import time
import ftplib
import sys
import warnings
import datetime
import logging as logger
import numpy as np
import multiprocessing as mp
from threading import Thread
from config_apd import configurations
from utils import available_space
from search import base_optimizer, gradient_opt_fast, exit_signal

# Suppress FutureWarnings
warnings.filterwarnings("ignore", category=FutureWarning)

#############################
# FTP helper function
#############################
def ftp_connect(host, username, password, port=21):
    """
    Connect to the FTP server and return an FTP connection object.
    """
    try:
        ftp = ftplib.FTP()
        ftp.connect(host, port)
        ftp.login(username, password)
        ftp.set_pasv(True)
        logger.info(f"Connected to FTP server: {host} as {username}")
        return ftp
    except ftplib.all_errors as e:
        logger.error("FTP error: " + str(e))
        sys.exit(1)

#############################
# Worker functions
#############################
def download_file_worker(process_id):
    """
    Download worker: Each process connects to the FTP server and downloads files 
    (one task at a time) from the shared download_tasks list, using a raw data
    socket (via transfercmd) instead of retrbinary().  Supports pause/resume,
    free-space throttling, and offset‐based restarts.
    """
    # Establish initial FTP connection
    ftp = ftp_connect(
        configurations["ftp"]["host"],
        configurations["ftp"]["username"],
        configurations["ftp"]["password"],
        configurations["ftp"].get("port", 21)
    )
    ftp.set_pasv(True)

    while True:
        # 1) pause if optimizer says so
        if download_process_status[process_id] == 0:
            time.sleep(0.1)
            continue

        # 2) get next task, or exit if none remain
        try:
            remote_file, relative_path = download_tasks.pop(0)
        except IndexError:
            break

        local_temp = os.path.join(download_dir, relative_path)
        os.makedirs(os.path.dirname(local_temp), exist_ok=True)

        # 3) determine resume offset
        offset = transfer_file_offsets.get(relative_path, 0)

        # open and seek
        fd = os.open(local_temp, os.O_CREAT | os.O_RDWR)
        os.lseek(fd, offset, os.SEEK_SET)

        try:
            # 4) open the data connection, telling the server to REST to 'offset'
            cmd = f"RETR {remote_file}"
            ftp.sendcmd('TYPE I')
            data_sock = ftp.transfercmd(cmd, rest=offset)

            # 5) read/write loop
            while True:
                chunk = data_sock.recv(chunk_size)
                if not chunk:
                    break  # EOF

                # 5a) throttle on low tmpfs space
                _, free_now = available_space(download_dir)
                while free_now*1024*1024 <= len(chunk) + chunk_size:
                    time.sleep(0.5)
                    _, free_now = available_space(download_dir)

                # 5b) pause if optimizer tells us to
                if download_process_status[process_id] == 0:
                    data_sock.close()
                    raise Exception("Download paused by optimizer")

                # 5c) write to disk & update offset
                os.write(fd, chunk)
                offset += len(chunk)
                transfer_file_offsets[relative_path] = offset

            # 6) cleanly close data socket and consume final FTP response
            data_sock.close()
            ftp.voidresp()

            # 7) mark complete
            os.close(fd)
            with transfer_complete.get_lock():
                transfer_complete.value += 1

            completed_tasks.append((remote_file, relative_path))
            logger.info(f"[Download #{process_id}] Completed {relative_path}")

        except Exception as e:
            # ensure sockets and fds are closed
            try: data_sock.close()
            except: pass
            try: os.close(fd)
            except: pass

            if str(e) == "Download paused by optimizer":
                # requeue for resume
                logger.info(f"[Download #{process_id}] Paused {relative_path} @ offset {offset}")
                download_tasks.append((remote_file, relative_path))

                # restart FTP connection (to avoid a “poisoned” socket)
                try: ftp.quit()
                except: ftp.close()

                ftp = ftp_connect(
                    configurations["ftp"]["host"],
                    configurations["ftp"]["username"],
                    configurations["ftp"]["password"],
                    configurations["ftp"].get("port", 21)
                )
                ftp.set_pasv(True)

            else:
                # other error: log and requeue if incomplete
                logger.error(f"[Download #{process_id}] Error {relative_path}: {e}")
                # try to requeue only if file isn’t fully fetched
                try:
                    total_len = int(ftp.size(remote_file))
                except:
                    total_len = offset
                if offset < total_len:
                    download_tasks.append((remote_file, relative_path))

        # loop back for next task or resume

    # once out of the loop, clean up
    time.sleep(0.1)
    try:
        ftp.quit()
    except ftplib.error_temp:
        ftp.close()

#############################
# Reporting throughput
#############################
def report_network_throughput():
    previous_total, previous_time = 0, 0
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
            fname = 'timed_log_download.csv'
            with open(fname, 'a') as f:
                f.write(f"{t2}, {elapsed}, {curr_thrpt}, {sum(download_process_status)}\n")
            time.sleep(max(0, 1 - (t2 - t1)))

#############################
# Optimizer functions
#############################
def download_probing(params):
    # Probing for download concurrency. Uses network throughput from downloaded bytes.
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
    thrpt = np.mean(throughput_logs[-2:]) if len(throughput_logs) > 2 else 0
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
    logger.debug(f"Graceful exit triggered: signum={signum}, frame={frame}")
    try:
        transfer_done.value = 1
        move_complete.value = transfer_complete.value
        shutil.rmtree(download_dir, ignore_errors=True)
    except Exception as e:
        logger.error(e)
    sys.exit(1)

#############################
# FTP recursive listing
#############################
def list_ftp_files(ftp, curr_dir, relative_dir):
    """
    Recursively list files in the remote directory tree.
    Returns a list of tuples: (remote_file, relative_path)
    """
    tasks = []
    try:
        ftp.cwd(curr_dir)
    except ftplib.error_perm as e:
        logger.error(f"Cannot access remote directory {curr_dir}: {e}")
        return tasks
    try:
        items = ftp.nlst()
    except ftplib.error_perm as e:
        logger.error(f"Error listing directory {curr_dir}: {e}")
        return tasks

    for item in items:
        # Try to change to the item to detect a directory.
        try:
            ftp.cwd(item)
            ftp.cwd('..')
            logger.info(f"Found directory: {item}")
            new_remote_dir = os.path.join(curr_dir, item)
            new_relative_dir = os.path.join(relative_dir, item)
            tasks.extend(list_ftp_files(ftp, new_remote_dir, new_relative_dir))
        except ftplib.error_perm:
            remote_file = os.path.join(curr_dir, item)
            relative_path = os.path.join(relative_dir, item)
            tasks.append((remote_file, relative_path))
    return tasks

#############################
# Main function
#############################
if __name__ == '__main__':
    # Setup signal handlers for graceful termination
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

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

    # Set configuration parameters
    configurations["cpu_count"] = mp.cpu_count()
    if configurations["thread_limit"] == -1:
        configurations["thread_limit"] = configurations["cpu_count"]
    chunk_size = 1024*1024
    probing_time = configurations["probing_sec"]

    download_dir = configurations["download_dir"]
    try:
        os.makedirs(download_dir, exist_ok=True)
    except Exception as e:
        logger.error(e)
        sys.exit(1)
    _, free = available_space(download_dir)
    memory_limit = min(50, free / 2)

    # Shared counters and structures
    transfer_complete = mp.Value("i", 0)
    move_complete = mp.Value("i", 0)
    transfer_done = mp.Value("i", 0)

    transfer_file_offsets = mp.Manager().dict()  # Bytes downloaded per file.
    throughput_logs = mp.Manager().list()           # Download throughput logs.
    download_tasks = mp.Manager().list() # List of FTP download tasks.
    completed_tasks = mp.Manager().list() # List of FTP download tasks.

    # Prepare download tasks by listing the remote FTP directory recursively.
    ftp_listing = ftp_connect(configurations["ftp"]["host"],
                              configurations["ftp"]["username"],
                              configurations["ftp"]["password"],
                              configurations["ftp"].get("port", 21))
    all_tasks = list_ftp_files(ftp_listing, configurations["ftp"]["remote_dir"], "")
    ftp_listing.quit()
    for task in all_tasks:
        download_tasks.append(task)
    logger.info(f"Total files to download: {len(download_tasks)}: {download_tasks}")
    
    num_workers = len(download_tasks)
    # Two sets of process status arrays:
    download_process_status = mp.Array("i", [0 for _ in range(num_workers)])
    io_process_status = mp.Array("i", [0 for _ in range(num_workers)])

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
    while len(all_tasks) != len(completed_tasks):
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

    shutil.rmtree(download_dir, ignore_errors=True)
    logger.info("Transfer Completed!")
    sys.exit(0)
