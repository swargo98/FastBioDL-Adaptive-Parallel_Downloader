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
from config_receiver import configurations
from utils import available_space, get_dir_size, run
from search import base_optimizer, hill_climb, cg_opt, gradient_opt_fast, exit_signal

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
    (one task at a time) from the shared download_tasks list. Download progress 
    (file offset) is stored so that if the process is paused (via download_process_status)
    it can resume later from the last saved offset. Additionally, if there is insufficient 
    space in tmpfs_dir, the worker will sleep to allow move_file workers to clear space.
    """
    fname = f'log_{process_id}.csv'
    ftp = ftp_connect(configurations["ftp"]["host"],
                      configurations["ftp"]["username"],
                      configurations["ftp"]["password"],
                      configurations["ftp"].get("port", 21))
    ftp.set_pasv(True)
    # Loop until there are no tasks
    while True:
        # Check if the optimizer has paused this worker.
        if download_process_status[process_id] == 0:
            # Worker is paused; sleep a bit and then check again.
            time.sleep(0.1)
            continue
        # print(process_id)

        # Check if there is enough free space in tmpfs_dir.
        # available_space(tmpfs_dir) should return (used, free)
        _, free_space = available_space(tmpfs_dir)
        # print(free_space*1024*1024, chunk_size)
        # If free space is below an arbitrary threshold (e.g., 10 times the chunk_size),
        # then sleep to allow move_file workers to free up space.
        while free_space*1024*1024 <= (chunk_size * 10):
            # print('line 130')
            # print(free_space*1024*1024, chunk_size)
            logger.debug(f"[Download #{process_id}] Not enough space in tmpfs_dir ({free_space} bytes free). Sleeping.")
            time.sleep(0.5)
            continue
        # print(process_id)
        # print(download_tasks)

        # Retrieve a new task if available; task is a tuple (remote_file, relative_path)
        try:
            if len(download_tasks) == 0:
                with open(fname, 'a') as f:
                    f.write(f"BREAK\n")
                break  # No more download tasks remain.
            task = download_tasks.pop(0)
        except IndexError:
            time.sleep(0.1)
            continue

        # print(task)
        remote_file, relative_path = task
        local_temp_file = os.path.join(tmpfs_dir, relative_path)
        os.makedirs(os.path.dirname(local_temp_file), exist_ok=True)
        # print(f'Started Downloading {remote_file}')

        # Check for a stored offset; if the file was partially downloaded, resume from that offset.
        current_offset = transfer_file_offsets.get(relative_path, 0)

        try:
            # Open the file for reading/writing. The file pointer is set to the previously
            # stored offset so that the download can be resumed.
            fd = os.open(local_temp_file, os.O_CREAT | os.O_RDWR)
            os.lseek(fd, current_offset, os.SEEK_SET)
            offset = current_offset  # Local variable to track the updated offset.

            def callback(data):
                nonlocal offset
                # Check if there is enough free space before writing.
                with open(fname, 'a') as f:
                    f.write(f"{local_temp_file}, {offset}\n")
                _, free_now = available_space(tmpfs_dir)
                while free_now*1024*1024 <= (len(data) + chunk_size):
                    # print('line 167')
                    # print(free_now*1024*1024, len(data) + chunk_size)
                    logger.debug(f"[Download #{process_id}] Low space during download of {relative_path}. Pausing write.")
                    time.sleep(0.5)
                while download_process_status[process_id] == 0:
                    # Worker is paused; sleep a bit and then check again.
                    time.sleep(0.1)
                os.write(fd, data)
                offset += len(data) 
                with open(fname, 'a') as f:
                    f.write(f"{local_temp_file}, {offset}\n")
                transfer_file_offsets[relative_path] = offset
                # Check if the optimizer has now paused this worker. If so, abort the download. (Redundant???)
                if download_process_status[process_id] == 0:
                    with open(fname, 'a') as f:
                        f.write(f"PAUSED\n")
                    print(f"{local_temp_file}, PAUSED")
                    raise Exception("Download paused by optimizer.")

            logger.debug(f"[Download #{process_id}] Downloading {remote_file} -> {local_temp_file} starting at offset {offset}")
            # Resume the download from the saved offset using the 'rest' parameter.
            ftp.retrbinary(f"RETR {remote_file}", callback, blocksize=chunk_size, rest=offset)
            os.close(fd)

            # Mark the file as completely downloaded.
            with transfer_complete.get_lock():
                transfer_complete.value += 1
                with open(fname, 'a') as f:
                    f.write(f"DONE\n")

            # Queue the file for moving and record that the task is completed.
            mQueue.append(relative_path)
            completed_tasks.append(task)
            logger.info(f"[Download #{process_id}] Completed {relative_path}")
            with open(fname, 'a') as f:
                    f.write(f"BREAK 146\n")
            break

        except Exception as e:
            # If the exception is due to a pause, simply log and leave the offset for resuming.
            if str(e) == "Download paused by optimizer.":
                logger.info(f"[Download #{process_id}] Paused download of {relative_path} at offset {transfer_file_offsets[relative_path]}")
                download_tasks.append(task)
                try:
                    ftp.quit()
                except Exception:
                    ftp.close()
                ftp = ftp_connect(configurations["ftp"]["host"],
                      configurations["ftp"]["username"],
                      configurations["ftp"]["password"],
                      configurations["ftp"].get("port", 21))
                ftp.set_pasv(True)
            else:
                logger.info(f"[Download #{process_id}] Error downloading {remote_file}: {e}")
            try:
                os.close(fd)
            except Exception:
                pass

    # Allow for a slight delay before terminating the FTP connection.
    time.sleep(0.2)
    try:
        ftp.quit()
    except ftplib.error_temp as e:
        logger.error(f"[Download #{process_id}] ftp.quit() error (ignored): {e}")
        ftp.close()

def move_file(process_id):
    """
    Move worker: waits for files to appear in the shared mQueue then reads the file
    from tmpfs_dir in chunks and writes it to the final destination (root_dir).
    """
    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        if io_process_status[process_id] != 0 and len(mQueue) > 0:
            logger.debug(f"[Move #{process_id}] Starting File Mover")
            try:
                # Pop a file (relative path) from the moving queue
                fname = mQueue.pop()
                # print(f"[Move #{process_id}] Starting File #{fname}")
                src_path = os.path.join(tmpfs_dir, fname)
                dst_path = os.path.join(root_dir, fname)
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                fd_dst = os.open(dst_path, os.O_CREAT | os.O_RDWR)
                current_block_size = chunk_size

                # Optional throttling based on an I/O limit
                if io_limit > 0:
                    target, factor = io_limit, 8
                    max_speed = (target * 1024 * 1024) / 8
                    second_target = int(max_speed / factor)
                    second_data_count = 0
                    timer100ms = time.time()

                with open(src_path, "rb") as ff:
                    offset = 0
                    # Check if a previous move attempt left an offset recorded
                    if fname in io_file_offsets:
                        offset = int(io_file_offsets[fname])
                    ff.seek(offset)
                    chunk = ff.read(current_block_size)

                    while chunk and io_process_status[process_id] != 0:
                        os.lseek(fd_dst, offset, os.SEEK_SET)
                        os.write(fd_dst, chunk)
                        offset += len(chunk)
                        io_file_offsets[fname] = offset
                        if io_limit > 0:
                            second_data_count += len(chunk)
                            if second_data_count >= second_target:
                                second_data_count = 0
                                while timer100ms + (1 / factor) > time.time():
                                    pass
                                timer100ms = time.time()
                        chunk = ff.read(current_block_size)

                    # If the move is incomplete, requeue the file.
                    if io_file_offsets.get(fname, 0) < transfer_file_offsets.get(fname, 0):
                        mQueue.append(fname)
                    else:
                        with move_complete.get_lock():
                            move_complete.value += 1
                            # print(f"[Move #{process_id}] complete for {fname}. {move_complete.value}")
                        logger.debug(f"[Move #{process_id}] Moved {fname}")
                        os.remove(src_path)
                        logger.debug(f"[Move #{process_id}] Cleanup complete for {fname}")
                        _, free_now = available_space(tmpfs_dir)
                        # print(f"[Move #{process_id}] Cleanup complete for {fname}. Free: {free_now}")
                os.close(fd_dst)
            except IndexError:
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"[Move #{process_id}] {e}")
                time.sleep(0.1)
            logger.debug(f"[Move #{process_id}] Exiting File Mover cycle")
        else:
            time.sleep(0.1)


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


def report_io_throughput():
    previous_total, previous_time = 0, 0
    while start.value == 0:
        time.sleep(0.1)
    start_time = start.value
    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        t1 = time.time()
        elapsed = round(t1 - start_time, 1)
        if elapsed > 1000:
            if sum(io_throughput_logs[-1000:]) == 0:
                transfer_done.value = 1
                move_complete.value = transfer_complete.value
                break
        if elapsed >= 0.1:
            total_bytes = sum(io_file_offsets.values())
            thrpt = round((total_bytes * 8) / (elapsed * 1000 * 1000), 2)
            curr_total = total_bytes - previous_total
            curr_time_sec = round(elapsed - previous_time, 3) or 0.001
            curr_thrpt = round((curr_total * 8) / (curr_time_sec * 1000 * 1000), 2)
            previous_time, previous_total = elapsed, total_bytes
            io_throughput_logs.append(curr_thrpt)
            logger.info(f"I/O Throughput @{elapsed}s: Current: {curr_thrpt}Mbps, Average: {thrpt}Mbps")
            t2 = time.time()
            fname = 'timed_log_io.csv'
            with open(fname, 'a') as f:
                f.write(f"{t2}, {elapsed}, {curr_thrpt}, {sum(io_process_status)}\n")
            time.sleep(max(0, 1 - (t2 - t1)))


#############################
# Optimizer functions
#############################
def io_probing(params):
    # If transfers are complete, signal termination.
    if transfer_done.value == 1 and move_complete.value >= transfer_complete.value:
        return exit_signal
    params = [1 if x < 1 else int(np.round(x)) for x in params]
    logger.info("I/O -- Probing Parameters: " + str(params))
    for i in range(len(io_process_status)):
        io_process_status[i] = 1 if i < params[0] else 0
    time.sleep(1)
    n_time = time.time() + probing_time - 1.05
    while time.time() < n_time and (transfer_done.value == 0 or move_complete.value < transfer_complete.value):
        time.sleep(0.1)
    thrpt = np.mean(io_throughput_logs[-2:]) if len(io_throughput_logs) > 2 else 0
    K = float(configurations["K"])
    cc_impact_nl = K ** params[0]
    score = thrpt / cc_impact_nl if cc_impact_nl != 0 else 0
    score_value = int(np.round(score * (-1)))
    used = get_dir_size(logger, tmpfs_dir)
    logger.info(f"Shared Memory -- Used: {used}GB")
    logger.info(f"I/O Probing -- Throughput: {int(np.round(thrpt))}Mbps, Score: {score_value}")
    if transfer_done.value == 1 and move_complete.value >= transfer_complete.value:
        return exit_signal
    else:
        return score_value


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


def run_io_optimizer(probing_func):
    while start.value == 0:
        time.sleep(0.1)
    params = [2]
    method = configurations["method"].lower()
    if method == "hill_climb":
        logger.info("Running Hill Climb Optimization for I/O....")
        params = hill_climb(configurations["thread_limit"], probing_func, logger)
    elif method == "gradient":
        logger.info("Running Gradient Optimization for I/O....")
        params = gradient_opt_fast(configurations["thread_limit"], probing_func, logger)
    elif method == "cg":
        logger.info("Running Conjugate Optimization for I/O....")
        params = cg_opt(False, probing_func)
    elif method == "probe":
        logger.info("Running fixed configuration Probing for I/O....")
        params = [configurations["fixed_probing"]["thread"]]
    else:
        logger.info("Running Bayesian Optimization for I/O....")
        params = base_optimizer(configurations, probing_func, logger)
    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        probing_func(params)


def run_download_optimizer(probing_func):
    while start.value == 0:
        time.sleep(0.1)
    params = [2]
    method = configurations["method"].lower()
    if method == "hill_climb":
        logger.info("Running Hill Climb Optimization for Download....")
        params = hill_climb(configurations["thread_limit"], probing_func, logger)
    elif method == "gradient":
        logger.info("Running Gradient Optimization for Download....")
        params = gradient_opt_fast(configurations["thread_limit"], probing_func, logger)
    elif method == "cg":
        logger.info("Running Conjugate Optimization for Download....")
        params = cg_opt(False, probing_func)
    elif method == "probe":
        logger.info("Running fixed configuration Probing for Download....")
        params = [configurations["fixed_probing"]["thread"]]
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
    print(f"Graceful exit triggered: signum={signum}, frame={frame}")
    try:
        transfer_done.value = 1
        move_complete.value = transfer_complete.value
        shutil.rmtree(tmpfs_dir, ignore_errors=True)
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
    # Use the provided "data_dir" as the final destination directory.
    root_dir = configurations["data_dir"]
    chunk_size = 1024*1024
    probing_time = configurations["probing_sec"]
    io_limit = int(configurations["io_limit"]) if ("io_limit" in configurations and configurations["io_limit"] is not None) else -1

    # Temporary directory – using shared memory (adjust as needed)
    tmpfs_dir = f"/dev/shm/data{os.getpid()}/"
    tmpfs_dir = "/mnt/nvme0n1/dest"
    try:
        os.makedirs(tmpfs_dir, exist_ok=True)
    except Exception as e:
        logger.error(e)
        sys.exit(1)
    _, free = available_space(tmpfs_dir)
    memory_limit = min(50, free / 2)
    # print(memory_limit)

    # Shared counters and structures
    transfer_complete = mp.Value("i", 0)
    move_complete = mp.Value("i", 0)
    transfer_done = mp.Value("i", 0)

    transfer_file_offsets = mp.Manager().dict()  # Bytes downloaded per file.
    io_file_offsets = mp.Manager().dict()         # Bytes moved per file.
    throughput_logs = mp.Manager().list()           # Download throughput logs.
    io_throughput_logs = mp.Manager().list()        # I/O throughput logs.
    mQueue = mp.Manager().list()         # Queue for files ready to be moved.
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

    # Start move workers.
    move_workers = [mp.Process(target=move_file, args=(i,)) for i in range(num_workers)]
    for p in move_workers:
        p.daemon = True
        p.start()

    # A shared start time for throughput measurement.
    start = mp.Value("d", time.time())
    # Start throughput reporting threads.
    network_report_thread = Thread(target=report_network_throughput)
    network_report_thread.start()
    io_report_thread = Thread(target=report_io_throughput)
    io_report_thread.start()

    # Start optimizer threads for I/O and Download concurrency.
    io_optimizer_thread = Thread(target=run_io_optimizer, args=(io_probing,))
    io_optimizer_thread.start()
    download_optimizer_thread = Thread(target=run_download_optimizer, args=(download_probing,))
    download_optimizer_thread.start()

    # Wait until all download tasks have been handled.
    while len(all_tasks) != len(completed_tasks):
        time.sleep(0.1)
    # Mark downloads as done.
    transfer_done.value = 1
    logger.info("Download Tasks Completed!")
    time.sleep(1)

    # print(f"Download Tasks Completed! Move complete: {move_complete.value}; Transfer Complete: {transfer_complete.value}")

    # print(f"609 Move complete: {move_complete.value}; Transfer Complete: {transfer_complete.value}")
    # Wait until all moved files match the count of completed downloads.
    while move_complete.value < transfer_complete.value:
        # print(f"612 Move complete: {move_complete.value}; Transfer Complete: {transfer_complete.value}")
        time.sleep(0.1)
    time.sleep(1)
    # Terminate download worker processes if still alive.
    for p in download_workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)
    for p in move_workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=0.1)

    shutil.rmtree(tmpfs_dir, ignore_errors=True)
    logger.info("Transfer Completed!")
    sys.exit(0)
