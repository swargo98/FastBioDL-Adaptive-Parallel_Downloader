import os
import shutil
import signal
import mmap
import time
import warnings
import datetime
import logging as logger
import numpy as np
import multiprocessing as mp
from threading import Thread
from config_fastbiodl import configurations
from utils import available_space, get_dir_size
from search import base_optimizer,gradient_opt_fast, exit_signal
warnings.filterwarnings("ignore", category=FutureWarning)

# ── Output-directory configuration ──────────────────────────────────────────
# For production runs both tmpfs_dir (staging) and root_dir (final) were
# historically set to NVMe paths.  For benchmarking we want the final
# destination on a *spinning-disk* mount so all three tools are measured on
# the same I/O tier.  Set BENCHMARK_ROOT_DIR to your disk mount and pass it
# as `root_dir` when constructing FileMover (or via fastbiodl_upgrade.py's
# --outdir flag).
#
#   PRODUCTION : root_dir = "/mnt/raid0/fastbiodl/staging/"
#   BENCHMARK  : root_dir = "/mnt/disk/benchmark/fastbiodl/output/"
BENCHMARK_ROOT_DIR = "fastbiodl/output/"


def _human_bytes(num_bytes):
    """Format byte counts for readable logging."""
    value = float(max(0, num_bytes))
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if value < 1024.0 or unit == "PB":
            return f"{value:.2f}{unit}"
        value /= 1024.0


def move_file(process_id):
    block_size = chunk_size
    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        if io_process_status[process_id] != 0 and mQueue:
            logger.debug(f'Starting File Mover Thread: {process_id}')

            if io_limit > 0:
                target, factor = io_limit, 8
                max_speed = (target * 1024 * 1024)/8
                second_target, second_data_count = int(max_speed/factor), 0
                block_size = min(block_size, second_target)
                timer100ms = time.time()

            try:
                fname = mQueue.pop()
                source_path = os.path.join(tmpfs_dir_g, fname)
                dest_path = os.path.join(root_dir_g, fname)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                fd = os.open(dest_path, os.O_CREAT | os.O_RDWR | os.O_TRUNC)

                with open(source_path, "rb") as ff:
                    chunk, offset = ff.read(block_size), 0
                    if fname in io_file_offsets:
                        offset = int(io_file_offsets[fname])

                    while chunk and io_process_status[process_id] != 0:
                        os.lseek(fd, offset, os.SEEK_SET)
                        os.write(fd, chunk)
                        offset += len(chunk)
                        io_file_offsets[fname] = offset
                        # logger.debug((fname, offset))
                        if io_limit > 0:
                            second_data_count += len(chunk)
                            if second_data_count >= second_target:
                                second_data_count = 0
                                time.sleep(max(0.0, timer100ms + (1.0 / factor) - time.time()))
                                timer100ms = time.time()

                                timer100ms = time.time()

                        ff.seek(offset)
                        chunk = ff.read(block_size)

                    if io_file_offsets[fname] < transfer_file_offsets[fname]:
                        mQueue.append(fname)
                    else:
                        with move_complete.get_lock():
                            move_complete.value += 1
                        logger.debug(f'I/O :: {fname}')
                        try:
                            removed_size = os.path.getsize(source_path)
                            os.remove(source_path)
                            with removed_compressed_bytes.get_lock():
                                removed_compressed_bytes.value += removed_size
                                removed_total = removed_compressed_bytes.value
                            logger.info(
                                f"[move_file #{process_id}] Removed staged compressed file: {source_path} "
                                f"({_human_bytes(removed_size)}, total {_human_bytes(removed_total)})"
                            )
                        except OSError as e:
                            logger.warning(f"[move_file #{process_id}] Could not remove source {source_path}: {e}")

                os.close(fd)

            except IndexError:
                time.sleep(0.1)

            except Exception as e:
                logger.exception(f"[move_file #{process_id}] Unexpected error on {fname!r}, requeueing: {e}")
                try:
                    mQueue.append(fname)   # put it back so it isn't lost
                except Exception:
                    pass
                time.sleep(0.1)

            logger.debug(f'Exiting File Mover Thread: {process_id}')
        else:
            time.sleep(0.1)

def io_probing(params):
    global io_throughput_logs
    if transfer_done.value == 1 and move_complete.value >= transfer_complete.value:
        return exit_signal

    params = [1 if x<1 else int(np.round(x)) for x in params]
    logger.info("I/O -- Probing Parameters: {0}".format(params))

    for i in range(len(io_process_status)):
        io_process_status[i] = 1 if i < params[0] else 0

    time.sleep(1)
    n_time = time.time() + probing_time - 1.05
    # time.sleep(n_time)
    while (time.time() < n_time) and (transfer_done.value == 0 or move_complete.value < transfer_complete.value):
        time.sleep(0.1)

    thrpt = np.mean(io_throughput_logs[-2:]) if len(io_throughput_logs) > 2 else 0
    K = float(configurations["K"])
    # score = thrpt
    # cc_impact_lin = (K-1) * num_transfer_workers.value
    # score = thrpt * (1-cc_impact_lin)
    cc_impact_nl = K**params[0]
    score = thrpt/cc_impact_nl
    score_value = np.round(score * (-1))
    used = get_dir_size(logger, tmpfs_dir_g)
    logger.info(f"Shared Memory -- Used: {used}GB")
    logger.info("I/O Probing -- Throughput: {0}Mbps, Score: {1}".format(
        np.round(thrpt), score_value))

    if transfer_done.value == 1 and move_complete.value >= transfer_complete.value:
        return exit_signal
    else:
        return score_value


def run_optimizer(probing_func):
    while _start_time.value == 0:
        time.sleep(0.1)

    params = [2]
    if configurations["method"].lower() == "gradient":
        logger.info("Running Gradient Optimization .... ")
        params = gradient_opt_fast(max(1, (transfer_complete.value - move_complete.value)), probing_func, logger)

    elif configurations["method"].lower() == "probe":
        logger.info("Running a fixed configurations Probing .... ")
        params = [configurations["fixed_probing"]["thread"]]

    else:
        logger.info("Running Bayesian Optimization .... ")
        params = base_optimizer(configurations, probing_func, logger)

    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        probing_func(params)


def report_io_throughput():
    global io_throughput_logs
    previous_total, previous_time = 0, 0

    while _start_time.value == 0:
        time.sleep(0.1)

    start_time = _start_time.value
    while transfer_done.value == 0 or move_complete.value < transfer_complete.value:
        t1 = time.time()
        time_since_begining = np.round(t1-start_time, 1)

        # FIX [Mover #1]: The original stall-detection fired after 15 s of zero
        # I/O, forcibly setting transfer_done=1 and move_complete=transfer_complete=0.
        # In the FastBioDL pipeline conversions take several MINUTES, so this
        # shortcut killed all move workers long before any file was ready,
        # leaving mover.stop() to wait out the full 7200-s timeout.
        #
        # New logic: only consider the "zero-throughput stall" a real completion
        # after the feeder sentinel has been received (transfer_done==1) AND all
        # registered files have been moved (move_complete>=transfer_complete>0).
        # For the ordinary "nothing to do yet" case we simply continue waiting.
        if time_since_begining > 15 and sum(io_throughput_logs[-15:]) == 0:
            if (transfer_done.value == 1
                    and transfer_complete.value > 0
                    and move_complete.value >= transfer_complete.value):
                break
            # else: conversions are still running — keep waiting, do NOT exit

        if time_since_begining >= 0.1:
            total_bytes = np.sum(io_file_offsets.values())
            thrpt = np.round((total_bytes*8)/(time_since_begining*1000*1000), 2)
            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3) or 0.001
            curr_thrpt = np.round((curr_total*8)/(curr_time_sec*1000*1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            io_throughput_logs.append(curr_thrpt)

            logger.info("I/O Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps".format(
                time_since_begining, curr_thrpt, thrpt))

            t2 = time.time()
            time.sleep(max(0, 1 - (t2-t1)))


def graceful_exit(signum=None, frame=None):
    logger.debug((signum, frame))
    try:
        transfer_done.value  = 1
        move_complete.value = transfer_complete.value
        # time.sleep()
        # shutil.rmtree(tmpfs_dir, ignore_errors=True)
    except Exception as e:
        logger.debug(e)

    exit(1)

class FileMover:
    """
    Consumes completed .fastq.gz paths from move_queue (written by SRAConverter),
    then copies them from /dev/shm to root_dir using the same I/O optimizer as Marlin.
    """

    def __init__(self, move_queue: mp.Queue, tmpfs_dir: str, root_dir: str, config: dict):
        self.move_queue = move_queue
        self.config = config

        # Inject module-level globals expected by the existing worker functions
        global tmpfs_dir_g, root_dir_g
        global transfer_complete, move_complete, transfer_done
        global io_process_status, transfer_file_offsets, io_file_offsets
        global io_throughput_logs, mQueue, _start_time, chunk_size
        global removed_compressed_bytes
        global probing_time, io_limit

        tmpfs_dir_g  = tmpfs_dir
        root_dir_g   = root_dir
        chunk_size   = 1024 * 1024
        probing_time = config.get("probing_sec", 5)
        io_limit     = int(config.get("io_limit", -1))

        num_workers = config["thread_limit"]
        mgr = mp.Manager()

        transfer_complete       = mp.Value("i", 0)
        move_complete           = mp.Value("i", 0)
        transfer_done           = mp.Value("i", 0)
        removed_compressed_bytes = mp.Value("Q", 0)
        io_process_status       = mp.Array("i", [0] * num_workers)
        transfer_file_offsets   = mgr.dict()
        io_file_offsets         = mgr.dict()
        io_throughput_logs      = mgr.list()
        mQueue                  = mgr.list()
        _start_time = mp.Value("d", 0.0)

        self._num_workers = num_workers
        self._threads     = []
        self._io_workers  = []

    # ── internal ────────────────────────────────────────────────────────────

    def _queue_feeder(self):
        """
        Drains move_queue and registers each file for the I/O worker pool.
        A None sentinel signals that no more files will arrive.
        """
        while True:
            path = self.move_queue.get()          # blocks until item available
            if path is None:                      # sentinel from fastbiodl shutdown
                transfer_done.value = 1
                logger.info("[FileMover] Feeder received sentinel — no more files")
                break

            fname     = os.path.relpath(path, tmpfs_dir_g)
            file_size = os.path.getsize(path)

            transfer_file_offsets[fname] = file_size
            io_file_offsets[fname]       = 0
            mQueue.append(fname)

            with transfer_complete.get_lock():    # atomic increment
                transfer_complete.value += 1

            logger.info(f"[FileMover] Registered: {fname}  ({file_size} bytes)")

    # ── public API ───────────────────────────────────────────────────────────

    def begin(self):
        # Feeder thread
        feeder = Thread(target=self._queue_feeder, name="mover-feeder", daemon=True)
        feeder.start()
        self._threads.append(feeder)

        # I/O worker pool (same as Marlin receiver)
        self._io_workers = [
            mp.Process(target=move_file, args=(i,))
            for i in range(self._num_workers)
        ]
        for p in self._io_workers:
            p.daemon = True
            p.start()

        # Reporter + optimizer threads
        reporter = Thread(target=report_io_throughput, name="mover-reporter", daemon=True)
        optimizer = Thread(target=run_optimizer, args=(io_probing,),
                           name="mover-optimizer", daemon=True)
        reporter.start()
        optimizer.start()
        self._threads += [reporter, optimizer]

        _start_time.value = time.time()    # unblocks reporter and optimizer (they poll _start_time.value)
        logger.info("[FileMover] Started")

    def stop(self, timeout: float = 7200.0):
        """Block until all registered files have been moved, then clean up."""
        deadline = time.time() + timeout
        while move_complete.value < transfer_complete.value or transfer_done.value == 0:
            if time.time() > deadline:
                logger.warning("[FileMover] stop() timed out")
                break
            time.sleep(0.5)

        for p in self._io_workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=1)

        logger.info(
            f"[FileMover] Done — moved {move_complete.value}/{transfer_complete.value} files, "
            f"cleanup_compressed={_human_bytes(removed_compressed_bytes.value)}"
        )