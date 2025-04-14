import os
import shutil
import signal
import mmap
import time
import ftplib # Changed from socket
import sys # Added for ftp error exit
import warnings
import datetime
import logging as logger
import numpy as np
import multiprocessing as mp
from threading import Thread
# Assuming config_receiver.py now contains FTP details as well
from config_receiver import configurations
from utils import available_space, get_dir_size, run
# Keep the search optimizers
from search import base_optimizer, hill_climb, cg_opt, gradient_opt_fast, exit_signal

warnings.filterwarnings("ignore", category=FutureWarning)

# --- Core Logic from Code 1 (Modified) ---

def move_file(process_id, tmpfs_dir, root_dir, chunk_size, io_limit, mQueue, io_process_status, move_complete, io_file_offsets, download_complete, total_files_to_download):
    """
    Moves files from temporary storage (tmpfs_dir) to final destination (root_dir).
    Optimized by io_probing function adjusting io_process_status.
    """
    logger.debug(f'Starting File Mover Thread: {process_id}')
    while download_complete.value < total_files_to_download.value or move_complete.value < download_complete.value:
        if io_process_status[process_id] != 0 and len(mQueue) > 0: # Check queue length directly
            try:
                fname = mQueue.pop(0) # Use list pop(0) for FIFO queue behavior
                logger.debug(f'Mover {process_id} picked {fname}')
                local_tmp_path = os.path.join(tmpfs_dir, fname)
                local_final_path = os.path.join(root_dir, fname)

                # Ensure the destination directory exists (especially if fname includes subdirs)
                final_dir = os.path.dirname(local_final_path)
                if not os.path.exists(final_dir):
                    try:
                        os.makedirs(final_dir, exist_ok=True)
                        logger.debug(f"Created destination directory: {final_dir}")
                    except OSError as e:
                        logger.error(f"Failed to create destination directory {final_dir}: {e}")
                        # Decide how to handle: skip file, retry, put back in queue?
                        # For now, log error and potentially lose the file if dir fails
                        continue # Skip this file

                # Open destination file
                try:
                    fd = os.open(local_final_path, os.O_CREAT | os.O_RDWR)
                except Exception as e:
                    logger.error(f"Failed to open destination file {local_final_path}: {e}")
                    continue # Skip this file

                block_size = chunk_size
                # I/O Throttling Logic (if io_limit is set)
                if io_limit > 0:
                    target, factor = io_limit, 8
                    max_speed = (target * 1024 * 1024) / 8
                    second_target, second_data_count = int(max_speed / factor), 0
                    block_size = min(block_size, second_target)
                    timer100ms = time.time()

                try:
                    with open(local_tmp_path, "rb") as ff:
                        offset = 0
                        # Check if we were already processing this file (e.g., after restart/error)
                        # For simplicity now, we assume starting fresh for each move.
                        # If resuming moves is needed, io_file_offsets needs careful management.

                        chunk = ff.read(block_size)
                        while chunk and io_process_status[process_id] != 0:
                            os.lseek(fd, offset, os.SEEK_SET)
                            bytes_written = os.write(fd, chunk)
                            if bytes_written is None or bytes_written <= 0:
                                logger.warning(f"Write returned {bytes_written} for {fname}, stopping move.")
                                break # Stop processing this file chunk

                            offset += bytes_written
                            # io_file_offsets[fname] = offset # Update progress primarily for reporting

                            # Throttling delay
                            if io_limit > 0:
                                second_data_count += bytes_written
                                if second_data_count >= second_target:
                                    second_data_count = 0
                                    while timer100ms + (1 / factor) > time.time():
                                        time.sleep(0.001) # Small sleep to prevent busy-waiting
                                    timer100ms = time.time()

                            ff.seek(offset) # Move read pointer
                            chunk = ff.read(block_size)

                    # Finished reading or interrupted
                    if io_process_status[process_id] == 0:
                        logger.warning(f"Mover {process_id} interrupted while processing {fname}. Re-queueing.")
                        mQueue.append(fname) # Put it back if interrupted
                    else:
                        # Move seems complete
                        move_complete.value += 1
                        io_file_offsets[fname] = offset # Store final size moved for reporting
                        logger.info(f'I/O Move Complete :: {fname} ({offset / (1024*1024):.2f} MB)')
                        # Safely close descriptor before removing source
                        os.close(fd)
                        try:
                            run(f'rm "{local_tmp_path}"', logger) # Use run util, quote path
                            logger.debug(f'Cleanup :: {fname}')
                        except Exception as e_rm:
                             logger.error(f"Failed to remove temporary file {local_tmp_path}: {e_rm}")

                except FileNotFoundError:
                     logger.error(f"Temporary file {local_tmp_path} not found for moving. Already moved or deleted?")
                     # If file is gone, maybe it was already moved? Increment anyways?
                     # Or maybe the download failed? Need careful state management.
                     # For now, log error. If this happens often, check download logic.
                     # Make sure fd is closed if opened
                     try: os.close(fd)
                     except: pass
                except Exception as e_move:
                    logger.exception(f"Error moving file {fname}: {e_move}")
                    mQueue.append(fname) # Re-queue on generic error
                    try: os.close(fd)
                    except: pass

            except IndexError:
                # This happens if mQueue becomes empty between checking length and popping
                time.sleep(0.05)
            except Exception as e:
                logger.exception(f"Unexpected error in move_file loop (Process {process_id}): {e}")
                time.sleep(0.1)
        else:
            # Wait if not active or queue is empty
            time.sleep(0.1)
    logger.debug(f'Exiting File Mover Thread: {process_id}')

# --- New FTP Download Logic (Inspired by Code 2) ---

def ftp_connect(host, username, password, port, retries=3, delay=5):
    """ Connect to the FTP server with retries. """
    ftp = ftplib.FTP()
    last_exception = None
    for attempt in range(retries):
        try:
            logger.debug(f"FTP connect attempt {attempt + 1}/{retries} to {host}:{port}")
            ftp.connect(host, port, timeout=30)  # Add timeout
            ftp.login(username, password)
            ftp.set_pasv(True) # Use passive mode
            logger.info(f"FTP connected to {host} as {username}")
            return ftp
        except ftplib.all_errors as e:
            logger.warning(f"FTP connection attempt {attempt + 1} failed: {e}")
            last_exception = e
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logger.error("FTP connection failed after multiple retries.")
                raise ConnectionError(f"FTP connection failed: {last_exception}") from last_exception
        except Exception as e: # Catch other potential errors like socket errors
            logger.warning(f"FTP connection attempt {attempt + 1} failed with non-FTP error: {e}")
            last_exception = e
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                 logger.error("FTP connection failed after multiple retries (non-FTP error).")
                 raise ConnectionError(f"FTP connection failed: {last_exception}") from last_exception
    return None # Should not be reached if exception is raised


def ftp_download_worker(process_id, ftp_host, ftp_port, ftp_user, ftp_pass, remote_base_dir, tmpfs_dir, chunk_size, ftp_download_queue, mQueue, download_complete, download_bytes_tracker, io_file_offsets, download_process_status):
    """
    Worker process to download files listed in ftp_download_queue from FTP server.
    """
    logger.debug(f"Starting FTP Download Worker {process_id}")
    ftp = None
    try:
        ftp = ftp_connect(ftp_host, ftp_user, ftp_pass, ftp_port)
        ftp.cwd(remote_base_dir) # Change to base directory once
        logger.info(f"FTP Worker {process_id} successfully connected and changed to {remote_base_dir}")
    except Exception as e:
        logger.error(f"FTP Worker {process_id} failed to connect or cwd: {e}. Worker terminating.")
        download_process_status[process_id] = -1 # Mark as errored
        return # Cannot proceed without connection

    download_process_status[process_id] = 1 # Mark as active

    while True:
        try:
            # Check if the main process signaled an error or completion needed (optional)
            # if some_global_stop_flag.value == 1: break

            remote_fname_rel = ftp_download_queue.pop(0) # Get relative path from queue
            logger.debug(f"Downloader {process_id} picked {remote_fname_rel}")

            remote_full_path = remote_fname_rel # Assuming nlst gives path relative to cwd
            local_tmp_path = os.path.join(tmpfs_dir, remote_fname_rel)

            # Ensure local temporary directory exists (if fname includes subdirs)
            local_tmp_subdir = os.path.dirname(local_tmp_path)
            if not os.path.exists(local_tmp_subdir):
                try:
                    os.makedirs(local_tmp_subdir, exist_ok=True)
                except OSError as e:
                    logger.error(f"Downloader {process_id} failed to create temp directory {local_tmp_subdir}: {e}")
                    # Decide handling: skip, maybe put back in queue? For now, skip.
                    continue

            total_bytes_downloaded = 0
            try:
                # Download the file
                with open(local_tmp_path, 'wb') as f:
                    def callback(data):
                        nonlocal total_bytes_downloaded
                        f.write(data)
                        bytes_downloaded = len(data)
                        total_bytes_downloaded += bytes_downloaded
                        # Update shared counter for overall throughput
                        with download_bytes_tracker.get_lock():
                             download_bytes_tracker.value += bytes_downloaded

                    # Use RETR command
                    ftp.retrbinary(f'RETR {remote_full_path}', callback, blocksize=chunk_size)

                logger.info(f"FTP Download complete: {remote_fname_rel} -> {local_tmp_path} ({total_bytes_downloaded / (1024*1024):.2f} MB)")

                # If successful, add to the move queue and update counters
                io_file_offsets[remote_fname_rel] = 0 # Initialize offset for mover
                mQueue.append(remote_fname_rel) # Add to move queue
                download_complete.value += 1

            except ftplib.error_perm as e:
                logger.error(f"FTP Worker {process_id} failed to download {remote_fname_rel}: Permission error {e}")
                # Remove potentially incomplete local file
                if os.path.exists(local_tmp_path):
                    try: os.remove(local_tmp_path)
                    except: pass
            except ftplib.error_temp as e:
                 logger.error(f"FTP Worker {process_id} failed to download {remote_fname_rel}: Temporary error {e}. Skipping.")
                 if os.path.exists(local_tmp_path):
                    try: os.remove(local_tmp_path)
                    except: pass
            except Exception as e_dl:
                logger.error(f"FTP Worker {process_id} failed during download of {remote_fname_rel}: {e_dl}")
                if os.path.exists(local_tmp_path):
                    try: os.remove(local_tmp_path)
                    except: pass


        except IndexError:
            # Queue is empty, worker can potentially exit if discovery is done
            # For robust handling, might need a flag indicating discovery complete
            logger.info(f"FTP Download Worker {process_id} found queue empty. Exiting.")
            break # Exit loop when queue is empty
        except ftplib.all_errors as e_ftp:
            logger.error(f"FTP Worker {process_id} encountered FTP error: {e_ftp}. Attempting reconnect...")
             # Simple handling: try to reconnect and continue loop (might retry same file if pop failed or happened before error)
            try:
                if ftp: ftp.quit()
            except: pass
            try:
                ftp = ftp_connect(ftp_host, ftp_user, ftp_pass, ftp_port)
                ftp.cwd(remote_base_dir)
                logger.info(f"FTP Worker {process_id} reconnected.")
            except Exception as e_reconnect:
                logger.error(f"FTP Worker {process_id} failed to reconnect: {e_reconnect}. Worker terminating.")
                download_process_status[process_id] = -1 # Mark as errored
                break # Exit loop if reconnect fails
        except Exception as e_outer:
             logger.exception(f"Unexpected error in ftp_download_worker loop (Process {process_id}): {e_outer}")
             time.sleep(1) # Avoid busy-looping on unexpected errors

    # Cleanup for this worker
    if ftp:
        try:
            ftp.quit()
            logger.debug(f"FTP Worker {process_id} quit FTP connection.")
        except Exception as e:
            logger.warning(f"FTP Worker {process_id} error quitting FTP: {e}")
    download_process_status[process_id] = 0 # Mark as finished (or -1 if errored out)
    logger.debug(f"Exiting FTP Download Worker {process_id}")


# --- Functions from Code 1 (Mostly Unchanged, but check dependencies) ---

def io_probing(params, tmpfs_dir, io_process_status, io_throughput_logs, probing_time, K_val, download_complete, move_complete, total_files_to_download):
    """ Probes I/O performance for the optimizer. """
    # Check if all work is done
    if download_complete.value >= total_files_to_download.value and move_complete.value >= download_complete.value:
        logger.info("I/O Probing: All downloads and moves complete. Sending exit signal.")
        return exit_signal # Use the imported signal value

    num_io_workers = len(io_process_status)
    params = [1 if x < 1 else int(np.round(x)) for x in params]
    # Ensure params[0] doesn't exceed the number of actual IO workers
    active_workers = min(max(1, params[0]), num_io_workers)
    logger.info(f"I/O Probing -- Setting Active Workers: {active_workers} / {num_io_workers}")

    for i in range(num_io_workers):
        io_process_status[i] = 1 if i < active_workers else 0

    # Wait for probing duration, but check frequently if work is done
    n_time = time.time() + probing_time
    while time.time() < n_time:
        if download_complete.value >= total_files_to_download.value and move_complete.value >= download_complete.value:
            logger.info("I/O Probing: Work completed during probing wait.")
            # Return a neutral score or exit signal if appropriate
            # Returning 0 might be safer than exit_signal if optimizer expects a number
            return 0 # Indicate neutral score as work finished
        time.sleep(0.1)

    # Calculate score based on recent IO throughput
    # Use a window size appropriate for probing_time
    probing_window = int(probing_time) + 1 # Get slightly more than probing time seconds of logs
    recent_logs = io_throughput_logs[-probing_window:]
    thrpt = np.mean(recent_logs) if len(recent_logs) > 0 else 0

    # Score calculation (penalize by number of active workers)
    cc_impact_nl = K_val ** active_workers # Using K^P penalty factor
    score = thrpt / cc_impact_nl if cc_impact_nl > 0 else thrpt # Avoid division by zero
    score_value = np.round(score * (-1)) # Optimizer minimizes score

    # Logging
    try:
        used_gb = get_dir_size(logger, tmpfs_dir, units='GB')
        logger.info(f"Shared Memory -- Used: {used_gb:.2f} GB")
    except Exception as e:
        logger.warning(f"Could not get tmpfs directory size: {e}")

    logger.info(f"I/O Probing -- Workers: {active_workers}, Throughput: {thrpt:.2f} Mbps, Score: {score_value}")

    # Check again if work is done *after* probing
    if download_complete.value >= total_files_to_download.value and move_complete.value >= download_complete.value:
         logger.info("I/O Probing: All downloads and moves complete after probing. Sending exit signal.")
         return exit_signal
    else:
        return score_value

def run_optimizer(probing_func, method, thread_limit, fixed_params, bayes_config, start_signal, download_complete, move_complete, total_files_to_download):
    """ Runs the chosen optimization algorithm. """
    while start_signal.value == 0:
        time.sleep(0.1)

    # Initial parameters guess (e.g., start with 2 workers)
    # The optimizer functions should define their own starting points if needed.
    # Let's pass the thread_limit which might be used by optimizers.
    initial_guess = [min(2, thread_limit)] # Start with 2 or max allowed
    bounds = [(1, thread_limit)] # Bounds for number of workers

    logger.info(f"Starting I/O Optimizer (Method: {method}, Max Workers: {thread_limit})")

    final_params = initial_guess # Default if optimization skipped or fails

    try:
        if method.lower() == "hill_climb":
            logger.info("Running Hill Climb Optimization .... ")
            # Assuming hill_climb takes (bounds, objective_function, logger)
            # Need to adapt hill_climb or this call if signature differs
            # Using a lambda to pass fixed args to probing_func
            objective = lambda p: probing_func(p)
            final_params = hill_climb(bounds, objective, logger)

        elif method.lower() == "gradient":
            logger.info("Running Gradient Optimization .... ")
             # Assuming gradient_opt_fast takes (bounds, objective_function, logger)
            objective = lambda p: probing_func(p)
            final_params = gradient_opt_fast(bounds, objective, logger)

        elif method.lower() == "cg":
            logger.info("Running Conjugate Gradient Optimization .... ")
             # Assuming cg_opt takes (bounds, objective_function) - requires modification maybe
            objective = lambda p: probing_func(p)
            # Need to check cg_opt signature and adapt call
            final_params = cg_opt(bounds, objective) # Assuming it takes bounds now

        elif method.lower() == "probe":
            logger.info("Running a fixed configuration Probing .... ")
            final_params = [fixed_params["thread"]]
            # Run probe once to set the fixed worker count
            probing_func(final_params)

        elif method.lower() == "bayesian":
            logger.info("Running Bayesian Optimization .... ")
            # Assuming base_optimizer takes (bounds, objective_function, config, logger)
            objective = lambda p: probing_func(p)
            # Pass relevant parts of bayes_config if needed
            final_params = base_optimizer(bounds, objective, bayes_config, logger)
        else:
             logger.warning(f"Unknown optimization method: {method}. Running with default params: {final_params}")
             # Run with default params once
             probing_func(final_params)


        logger.info(f"Optimization finished. Optimal parameters found: {final_params}")
        # Keep applying the best found parameters until all work is done
        while download_complete.value < total_files_to_download.value or move_complete.value < download_complete.value:
            result = probing_func(final_params)
            if result == exit_signal:
                break
            # No need to sleep here, probing_func has waits/sleeps
            # Add a small sleep if probing_func could return very quickly
            time.sleep(1) # Check every second after optimization finishes

    except Exception as e:
        logger.exception(f"Error during optimization execution: {e}")
        # Fallback: Run with a default setting if optimizer crashes
        logger.warning("Optimizer failed. Running with default parameters.")
        default_params = [min(2, thread_limit)]
        while download_complete.value < total_files_to_download.value or move_complete.value < download_complete.value:
             result = probing_func(default_params)
             if result == exit_signal:
                 break
             time.sleep(1)

    logger.info("Optimizer thread exiting.")


def report_ftp_throughput(start_signal, download_bytes_tracker, download_complete, total_files_to_download):
    """ Reports overall FTP download throughput. """
    global ftp_throughput_logs # Use a global or pass a Manager list
    previous_total_bytes, previous_time = 0, 0

    while start_signal.value == 0:
        time.sleep(0.1)

    start_time = start_signal.value
    logger.info("FTP Throughput reporting started.")

    while download_complete.value < total_files_to_download.value:
        t1 = time.time()
        time_since_beginning = np.round(t1 - start_time, 1)

        if time_since_beginning <= 0.1: # Avoid division by zero at start
             time.sleep(0.5)
             continue

        # Read total bytes downloaded from shared counter
        with download_bytes_tracker.get_lock():
            total_bytes = download_bytes_tracker.value

        # Calculate average throughput
        avg_thrpt_mbps = np.round((total_bytes * 8) / (time_since_beginning * 1000 * 1000), 2)

        # Calculate current interval throughput
        curr_bytes = total_bytes - previous_total_bytes
        curr_time_sec = np.round(time_since_beginning - previous_time, 3)
        curr_thrpt_mbps = 0
        if curr_time_sec > 0:
             curr_thrpt_mbps = np.round((curr_bytes * 8) / (curr_time_sec * 1000 * 1000), 2)

        previous_time, previous_total_bytes = time_since_beginning, total_bytes
        ftp_throughput_logs.append(curr_thrpt_mbps) # Log current throughput

        logger.info(f"FTP Throughput @{time_since_beginning:.1f}s: Current: {curr_thrpt_mbps} Mbps, Average: {avg_thrpt_mbps} Mbps")

        # Check for stall (optional, but good practice)
        stall_check_duration = 15 # seconds
        if time_since_beginning > stall_check_duration:
            recent_logs = ftp_throughput_logs[-stall_check_duration:]
            if len(recent_logs) == stall_check_duration and sum(recent_logs) == 0:
                logger.warning("FTP download stalled for 15 seconds. Investigate workers.")
                # Potentially add logic here to signal an error or try to recover

        # Sleep until the next reporting interval (e.g., 1 second)
        t2 = time.time()
        sleep_time = max(0, 1.0 - (t2 - t1))
        time.sleep(sleep_time)

    logger.info("FTP Throughput reporting finished.")


def report_io_throughput(start_signal, io_file_offsets, io_throughput_logs, download_complete, move_complete, total_files_to_download):
    """ Reports I/O move throughput. """
    previous_total_bytes, previous_time = 0, 0

    while start_signal.value == 0:
        time.sleep(0.1)

    start_time = start_signal.value
    logger.info("I/O Throughput reporting started.")

    # Continue reporting as long as there are files being moved or potentially waiting to be moved
    while move_complete.value < total_files_to_download.value:
        t1 = time.time()
        time_since_beginning = np.round(t1 - start_time, 1)

        if time_since_beginning <= 0.1:
             time.sleep(0.5)
             continue

        # Sum bytes moved (using final values stored in io_file_offsets when move completes)
        # This might slightly lag if files are in transit, but reflects completed work.
        # Alternative: Sum current offsets during the move_file loop (more complex state sharing).
        # Let's sum the values in the dict for an estimate of total moved bytes.
        # Note: This assumes io_file_offsets stores the *final* size after move for completed files.
        # If io_file_offsets tracks live progress, this sum is accurate. Let's assume move_file updates it finally.
        # We need a way to sum bytes *actually* moved in the last second.
        # Let's use the final values in io_file_offsets associated with files whose move is complete.

        # This calculation needs refinement. io_file_offsets might contain in-progress values
        # if move_file updates it live, or final values. Let's calculate based on move_complete count
        # assuming average file size, or sum offsets directly understanding the limitation.
        # A simpler approach: Sum all values in io_file_offsets. This reflects bytes *written*
        # by the movers up to this point (either in progress or completed).
        total_bytes = sum(io_file_offsets.values())

        # Calculate average throughput
        avg_thrpt_mbps = np.round((total_bytes * 8) / (time_since_beginning * 1000 * 1000), 2)

        # Calculate current interval throughput
        curr_bytes = total_bytes - previous_total_bytes
        curr_time_sec = np.round(time_since_beginning - previous_time, 3)
        curr_thrpt_mbps = 0
        if curr_time_sec > 0:
             curr_thrpt_mbps = np.round((curr_bytes * 8) / (curr_time_sec * 1000 * 1000), 2)

        previous_time, previous_total_bytes = time_since_beginning, total_bytes
        io_throughput_logs.append(curr_thrpt_mbps) # Log current throughput

        logger.info(f"I/O Move Throughput @{time_since_beginning:.1f}s: Current: {curr_thrpt_mbps} Mbps, Average: {avg_thrpt_mbps} Mbps ({move_complete.value}/{total_files_to_download.value} files)")

         # Check for stall (e.g., if no files moved recently but downloads are done)
        stall_check_duration = 15
        if time_since_beginning > stall_check_duration and download_complete.value >= total_files_to_download.value :
            recent_logs = io_throughput_logs[-stall_check_duration:]
            if len(recent_logs) == stall_check_duration and sum(recent_logs) == 0 and move_complete.value < total_files_to_download.value:
                logger.warning(f"I/O Move stalled for {stall_check_duration}s while downloads appear complete. Investigate movers.")

        # Sleep until the next reporting interval
        t2 = time.time()
        sleep_time = max(0, 1.0 - (t2 - t1))
        time.sleep(sleep_time)

    logger.info("I/O Throughput reporting finished.")

# --- Global Variables / Shared State ---
# Use Manager for shared lists/dicts between processes
manager = mp.Manager()
mQueue = manager.list() # Queue for files downloaded, ready to be moved
ftp_download_queue = manager.list() # Queue for files needing download
io_file_offsets = manager.dict() # Tracks bytes moved per file (updated by mover)
ftp_throughput_logs = manager.list() # Stores current FTP throughput samples
io_throughput_logs = manager.list() # Stores current I/O throughput samples

# Use Value/Array for counters and status flags
download_complete = mp.Value("i", 0) # Count of files successfully downloaded
move_complete = mp.Value("i", 0) # Count of files successfully moved
total_files_to_download = mp.Value("i", 0) # Total files found on FTP
start_signal = mp.Value("i", 0) # Timestamp when process starts, triggers reporting/optimizer

# --- Main Execution ---

def graceful_exit(signum=None, frame=None):
    """ Attempts to clean up resources on termination signal. """
    logger.warning(f"Received signal {signum}. Initiating graceful shutdown...")
    # Set flags to signal workers to stop (if they check for it)
    # This is complex with multiprocessing; termination is often forceful.

    # Try to clean up tmpfs directory
    # Note: Workers might still be writing to it. Termination is safer.
    # Set a global flag maybe?
    global shutting_down
    shutting_down = True
    time.sleep(1) # Give workers a moment to potentially notice (if designed to)

    try:
        tmpfs_dir_path = f"/dev/shm/data{os.getpid()}/" # Reconstruct path if not global
        if os.path.exists(tmpfs_dir_path):
            logger.info(f"Attempting to remove temporary directory: {tmpfs_dir_path}")
            shutil.rmtree(tmpfs_dir_path, ignore_errors=True)
    except Exception as e:
        logger.error(f"Error during temporary directory cleanup: {e}")

    logger.warning("Exiting.")
    # Exit forcefully - relying on daemon=True for worker cleanup is common
    sys.exit(1)

shutting_down = False # Global flag for shutdown signal

if __name__ == '__main__':
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

    # --- Configuration Loading ---
    # Ensure these are in your config_receiver.py or configurations dict
    log_level = configurations.get("loglevel", "info").upper()
    log_file = f'logs/ftp_receiver.{datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}.log'
    os.makedirs("logs", exist_ok=True) # Ensure logs directory exists

    ftp_host = configurations.get("ftp", {}).get("host", "127.0.0.1") # Example default
    ftp_port = configurations.get("ftp", {}).get("port", 21)
    ftp_user = configurations.get("ftp", {}).get("user", "anonymous")
    ftp_pass = configurations.get("ftp", {}).get("password", "")
    ftp_remote_dir = configurations.get("ftp", {}).get("remote_dir", "/") # Directory to download from

    root_dir = configurations.get("data_dir", "received_data/") # Final destination
    os.makedirs(root_dir, exist_ok=True) # Ensure final destination exists

    tmpfs_dir = f"/dev/shm/data{os.getpid()}/"
    probing_time = configurations.get("probing_sec", 5) # IO probing duration
    K_val = float(configurations.get("K", 1.1)) # Penalty factor for IO probing score

    # Worker counts
    # Use separate configurations for download and I/O workers
    num_download_workers = configurations.get("num_download_workers", 4)
    num_io_workers = configurations.get("num_io_workers", 4) # Max IO workers for optimizer

    # Optimizer settings
    optimizer_method = configurations.get("method", "bayesian").lower()
    optimizer_fixed_params = configurations.get("fixed_probing", {"thread": 2})
    optimizer_bayes_config = configurations.get("bayesian_config", {}) # Pass relevant bayes config

    chunk_size = configurations.get("chunk_size", 1024*1024) # 1MB default
    io_limit = configurations.get("io_limit", -1) # MBytes/sec, -1 for unlimited

    # --- Logging Setup ---
    log_FORMAT = '%(created)f -- %(levelname)s -- %(processName)s: %(message)s'
    log_level_enum = getattr(logger, log_level, logger.INFO)

    logger.basicConfig(
        format=log_FORMAT,
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=log_level_enum,
        handlers=[
            logger.FileHandler(log_file, mode='w'),
            logger.StreamHandler(sys.stdout) # Log to stdout as well
        ]
    )
    if log_level_enum == logger.DEBUG:
         # Enable multiprocessing logging to stderr if debug
         # Be careful, this can be very verbose
         # mp.log_to_stderr(logger.DEBUG) # Commented out by default
         pass
    logger.info("-------------------- Configuration --------------------")
    logger.info(f"Log Level: {log_level}")
    logger.info(f"FTP Host: {ftp_host}:{ftp_port}, User: {ftp_user}, Remote Dir: {ftp_remote_dir}")
    logger.info(f"Destination Dir: {root_dir}")
    logger.info(f"Temp Dir: {tmpfs_dir}")
    logger.info(f"Download Workers: {num_download_workers}, I/O Workers (Max): {num_io_workers}")
    logger.info(f"I/O Optimizer Method: {optimizer_method}")
    logger.info(f"Chunk Size: {chunk_size / (1024*1024)} MB, I/O Limit: {io_limit if io_limit > 0 else 'Unlimited'} MB/s")
    logger.info(f"I/O Probing Time: {probing_time}s, K Factor: {K_val}")
    logger.info("-------------------------------------------------------")


    # --- Temp Directory Setup ---
    try:
        os.makedirs(tmpfs_dir, exist_ok=True)
        logger.info(f"Created temporary directory: {tmpfs_dir}")
        _, free_space_gb = available_space(tmpfs_dir)
        logger.info(f"Available space in {tmpfs_dir}: {free_space_gb:.2f} GB")
        # Add check for available space vs expected data size if possible
    except Exception as e:
        logger.exception(f"Failed to create temporary directory {tmpfs_dir}: {e}")
        sys.exit(1)

    # --- List files from FTP ---
    files_to_download = []
    try:
        logger.info("Connecting to FTP to list files...")
        ftp_main = ftp_connect(ftp_host, ftp_user, ftp_pass, ftp_port)
        ftp_main.cwd(ftp_remote_dir)
        # Use nlst for simpler listing, assuming flat directory or want recursive download handled differently later
        # If recursive download is needed, need a more complex listing function here
        raw_list = ftp_main.nlst()
        # Filter out '.' and '..' and potentially directories if only files are wanted
        files_to_download = [item for item in raw_list if item not in ('.', '..')]
        # Add filtering logic here if needed (e.g., check if item is file or directory using ftp.dir() or ftp.mlsd())
        # For now, assume nlst gives files or items we want to try downloading.
        ftp_main.quit()
        logger.info(f"Found {len(files_to_download)} items to potentially download from {ftp_remote_dir}")
        if not files_to_download:
             logger.warning("No files found on FTP server in the specified directory. Exiting.")
             shutil.rmtree(tmpfs_dir, ignore_errors=True)
             sys.exit(0)
        # Populate the download queue
        for fname in files_to_download:
             ftp_download_queue.append(fname)
        total_files_to_download.value = len(files_to_download)
    except Exception as e:
        logger.exception(f"Failed to list files from FTP: {e}")
        try: ftp_main.quit()
        except: pass
        shutil.rmtree(tmpfs_dir, ignore_errors=True)
        sys.exit(1)


    # --- Initialize Shared State ---
    io_process_status = mp.Array("i", [0] * num_io_workers) # Status for IO workers (controlled by optimizer)
    download_process_status = mp.Array("i", [0] * num_download_workers) # Status for download workers
    download_bytes_tracker = mp.Value("L", 0) # Shared counter for total bytes downloaded (unsigned long long)

    # --- Start Workers ---
    # Download Workers
    download_workers = []
    for i in range(num_download_workers):
        p = mp.Process(target=ftp_download_worker, name=f"FTP-Downloader-{i}", args=(
            i, ftp_host, ftp_port, ftp_user, ftp_pass, ftp_remote_dir, tmpfs_dir,
            chunk_size, ftp_download_queue, mQueue, download_complete,
            download_bytes_tracker, io_file_offsets, download_process_status
        ))
        p.daemon = True # Ensure they exit if main process exits
        download_workers.append(p)
        p.start()
        logger.debug(f"Started FTP Download Worker {i}")

    # I/O Move Workers
    io_workers = []
    for i in range(num_io_workers):
         p = mp.Process(target=move_file, name=f"IO-Mover-{i}", args=(
             i, tmpfs_dir, root_dir, chunk_size, io_limit, mQueue,
             io_process_status, move_complete, io_file_offsets,
             download_complete, total_files_to_download # Pass download status for termination checks
         ))
         p.daemon = True
         io_workers.append(p)
         p.start()
         logger.debug(f"Started I/O Mover Worker {i}")

    # --- Start Reporting & Optimization Threads ---
    start_signal.value = int(time.time()) # Signal threads to start monitoring/optimizing

    ftp_report_thread = Thread(target=report_ftp_throughput, name="FTP-Report", args=(
         start_signal, download_bytes_tracker, download_complete, total_files_to_download
    ), daemon=True)
    ftp_report_thread.start()

    io_report_thread = Thread(target=report_io_throughput, name="IO-Report", args=(
         start_signal, io_file_offsets, io_throughput_logs, download_complete, move_complete, total_files_to_download
    ), daemon=True)
    io_report_thread.start()

    # Create the lambda for the probing function with its necessary arguments
    probing_func_with_args = lambda p: io_probing(
        p, tmpfs_dir, io_process_status, io_throughput_logs, probing_time, K_val,
        download_complete, move_complete, total_files_to_download
    )

    io_optimizer_thread = Thread(target=run_optimizer, name="IO-Optimizer", args=(
        probing_func_with_args, optimizer_method, num_io_workers,
        optimizer_fixed_params, optimizer_bayes_config, start_signal,
        download_complete, move_complete, total_files_to_download
    ), daemon=True)
    io_optimizer_thread.start()


    # --- Main Wait Loop ---
    logger.info("Main process waiting for tasks to complete...")
    while move_complete.value < total_files_to_download.value:
        if shutting_down: # Check if graceful exit was triggered
             logger.warning("Shutdown signal received, exiting main loop.")
             break

        # Check worker health (optional but recommended)
        # Check download_process_status for -1 (error)
        if -1 in download_process_status[:]:
             logger.error("One or more download workers encountered critical errors. Stopping.")
             # Add more robust error handling: signal others, attempt cleanup etc.
             break # Exit main loop

        # Check if all download workers finished but moves are stuck
        all_downloads_done = download_complete.value >= total_files_to_download.value
        all_download_workers_inactive = all(s <= 0 for s in download_process_status[:]) # 0=finished, -1=error

        if all_downloads_done and all_download_workers_inactive and move_complete.value < total_files_to_download.value:
            # Downloads finished, but moves aren't progressing. Log maybe?
            # The I/O reporter already has stall detection logic.
            pass

        time.sleep(1) # Check completion status every second


    # --- Cleanup ---
    logger.info("All tasks appear complete or process is exiting.")
    final_downloaded = download_complete.value
    final_moved = move_complete.value
    final_total = total_files_to_download.value
    logger.info(f"Completion Status: Downloaded={final_downloaded}/{final_total}, Moved={final_moved}/{final_total}")


    # Allow optimizer/reporters a moment to finish logging/final state
    time.sleep(2)

    # Terminate any remaining worker processes (should be daemons, but helps ensure cleanup)
    logger.debug("Terminating worker processes if still alive...")
    for p in download_workers + io_workers:
        if p.is_alive():
            try:
                p.terminate()
                p.join(timeout=0.5) # Give a short time to terminate
                logger.debug(f"Terminated worker: {p.name}")
            except Exception as e:
                 logger.warning(f"Error terminating worker {p.name}: {e}")

    # Final cleanup of temporary directory
    try:
        logger.info(f"Removing temporary directory: {tmpfs_dir}")
        shutil.rmtree(tmpfs_dir, ignore_errors=True)
    except Exception as e:
        logger.error(f"Error during final temporary directory cleanup: {e}")

    logger.info("Script finished.")
    sys.exit(0) # Exit cleanly