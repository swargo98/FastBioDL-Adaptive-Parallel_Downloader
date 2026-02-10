#!/usr/bin/env python3
import os
import signal
import time
import sys
import warnings
import datetime
import logging
import numpy as np
import multiprocessing as mp
import asyncio
import aiohttp
from threading import Thread, Lock
from collections import deque
from config_fastbiodl import configurations
from utils import available_space
from search import base_optimizer, gradient_opt_fast, exit_signal

from typing import List, Tuple, Optional, Dict, Set
import argparse
import csv
import json

NCBI_EFETCH = (
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
)

# Suppress FutureWarnings
warnings.filterwarnings("ignore", category=FutureWarning)

#############################
# NCBI URL fetching
#############################
def get_ncbi_urls(acc: str, field: str = "sra_ftp") -> List[Tuple[str, str]]:
    """
    Fetch download URLs for a given SRA accession from NCBI's efetch "runinfo" endpoint.
    Returns list of (url, accession) tuples to preserve source context.
    """
    import requests
    
    logging.info(f"Fetching URLs for {acc} from NCBI SRA using field '{field}'")
    r = requests.get(
        NCBI_EFETCH,
        params={"db": "sra", "id": acc, "rettype": "runinfo", "retmode": "text"},
        timeout=30,
    )
    r.raise_for_status()
    lines = [l for l in r.text.strip().splitlines() if l.strip()]
    if len(lines) < 2:
        return []

    reader = csv.reader(lines)
    header = next(reader)
    data_rows = list(reader)
    col_map = {"sra_ftp": "download_path", "fastq_ftp": "fastq_ftp"}
    col_name = col_map.get(field, field)
    if col_name not in header:
        return []

    idx = header.index(col_name)
    url_acc_pairs: List[Tuple[str, str]] = []
    for row in data_rows:
        for u in row[idx].split(";"):
            if not u:
                continue
            if "://" not in u:
                u = "https://" + u
            url_acc_pairs.append((u, acc))
    
    time.sleep(0.2)
    return url_acc_pairs


#############################
# File metadata and segmentation
#############################
class FileMetadata:
    """Stores metadata about a file to be downloaded."""
    def __init__(self, url: str, local_path: str, file_size: int, 
                 supports_ranges: bool, segments: List[Tuple[int, int]]):
        self.url = url
        self.local_path = local_path
        self.file_size = file_size
        self.supports_ranges = supports_ranges
        self.segments = segments  # List of (start, end) tuples
        self.part_path = local_path + ".part"
        self.meta_path = local_path + ".part.meta"


async def probe_file(session: aiohttp.ClientSession, url: str) -> Tuple[Optional[int], bool]:
    """
    Probe for file size and Range support.
    Returns (file_size, supports_ranges).
    """
    try:
        # First try HEAD to get file size
        async with session.head(url, allow_redirects=True) as resp:
            if resp.status == 200:
                content_length = resp.headers.get('Content-Length')
                file_size = int(content_length) if content_length else None
            else:
                file_size = None
        
        # Probe with tiny Range request to confirm support
        headers = {'Range': 'bytes=0-0'}
        async with session.get(url, headers=headers) as resp:
            if resp.status == 206:
                supports_ranges = True
                # Try to get file size from Content-Range if we don't have it
                if file_size is None:
                    content_range = resp.headers.get('Content-Range', '')
                    if '/' in content_range:
                        try:
                            file_size = int(content_range.split('/')[-1])
                        except:
                            pass
            else:
                supports_ranges = False
        
        return file_size, supports_ranges
        
    except Exception as e:
        logging.debug(f"Range probe failed for {url}: {e}")
        return None, False


def calculate_segments(file_size: int, segment_size: int, min_file_size: int) -> List[Tuple[int, int]]:
    """
    Calculate segment ranges for parallel downloading.
    Returns list of (start_byte, end_byte) tuples.
    No max_segments limit - we'll distribute across all workers.
    """
    # Don't segment small files
    if file_size < min_file_size:
        return [(0, file_size - 1)]
    
    # Calculate number of segments (no artificial cap)
    num_segments = max(1, file_size // segment_size)
    
    segment_list = []
    bytes_per_segment = file_size // num_segments
    
    for i in range(num_segments):
        start = i * bytes_per_segment
        # Last segment gets any remainder bytes
        end = file_size - 1 if i == num_segments - 1 else (i + 1) * bytes_per_segment - 1
        segment_list.append((start, end))
    
    return segment_list


#############################
# Segment metadata management
#############################
def read_segment_metadata(meta_path: str) -> Optional[Dict]:
    """Read segment completion metadata from .meta file."""
    if not os.path.exists(meta_path):
        return None
    try:
        with open(meta_path, 'r') as f:
            return json.load(f)
    except:
        return None


def write_segment_metadata(meta_path: str, file_size: int, segments: List[Tuple[int, int]], 
                           completed: Set[int]):
    """Write segment completion metadata to .meta file with atomic write."""
    metadata = {
        'file_size': file_size,
        'segments': [[s, e] for s, e in segments],  # Store as lists for JSON compatibility
        'completed_indices': list(completed)
    }
    # Atomic write: write to temp file, then rename
    meta_tmp = meta_path + '.tmp'
    with open(meta_tmp, 'w') as f:
        json.dump(metadata, f)
        f.flush()
        os.fsync(f.fileno())
    os.rename(meta_tmp, meta_path)


def mark_segment_complete(meta_path: str, file_size: int, segments: List[Tuple[int, int]], 
                          segment_idx: int, segment_completion_lock: Lock):
    """
    Thread-safe: mark a segment as complete and update metadata.
    """
    with segment_completion_lock:
        metadata = read_segment_metadata(meta_path)
        if metadata:
            completed = set(metadata.get('completed_indices', []))
        else:
            completed = set()
        
        completed.add(segment_idx)
        write_segment_metadata(meta_path, file_size, segments, completed)


#############################
# Segment Downloader
#############################
async def download_segment(
    session: aiohttp.ClientSession,
    url: str,
    start: int,
    end: int,
    part_path: str,
    process_id: int,
    process_counter: mp.Value,
    active_connections: mp.Value,
    max_retries: int = 3
) -> int:
    """
    Download a single segment and write directly to file at correct offset.
    Returns bytes_written.
    """
    chunk_size = 128 * 1024
    bytes_written = 0
    current_offset = start
    local_bytes_accumulated = 0
    flush_threshold = 4 * 1024 * 1024
    
    # Update active connections
    if active_connections is not None:
        with active_connections.get_lock():
            active_connections.value += 1
    
    try:
        # Retry logic with exponential backoff
        for attempt in range(max_retries):
            # Track the requested range for THIS attempt
            req_start = current_offset
            req_end = end
            expected_bytes = req_end - req_start + 1
            
            headers = {'Range': f'bytes={req_start}-{req_end}'}
            
            try:
                async with session.get(url, headers=headers) as resp:
                    # STRICT: Require 206 for Range requests
                    if resp.status != 206:
                        raise Exception(
                            f"Expected 206 Partial Content for Range request, got {resp.status}"
                        )
                    
                    # Validate Content-Range header
                    content_range = resp.headers.get('Content-Range', '')
                    if not content_range:
                        raise Exception("Server returned 206 but no Content-Range header")
                    
                    if not content_range.startswith('bytes '):
                        raise Exception(f"Invalid Content-Range format: {content_range}")
                    
                    # Validate returned range matches what we asked for THIS attempt
                    try:
                        range_part = content_range.split()[1]
                        returned_range = range_part.split('/')[0]
                        returned_start, returned_end = map(int, returned_range.split('-'))
                        if returned_start != req_start or returned_end != req_end:
                            raise Exception(
                                f"Server returned different range: asked {req_start}-{req_end}, got {returned_start}-{returned_end}"
                            )
                    except Exception as e:
                        raise Exception(f"Failed to validate Content-Range '{content_range}': {e}")
                    
                    # Open file for this segment
                    fd = os.open(part_path, os.O_CREAT | os.O_RDWR)
                    try:
                        async for chunk in resp.content.iter_chunked(chunk_size):
                            # Check if paused
                            if download_process_status[process_id] == 0:
                                raise asyncio.CancelledError("Download paused by optimizer")
                            
                            # Check available space
                            _, free_now = available_space(download_dir)
                            while free_now * 1024 * 1024 <= (len(chunk) + chunk_size):
                                await asyncio.sleep(0.5)
                                _, free_now = available_space(download_dir)
                            
                            # Write directly to file at correct offset using pwrite
                            os.pwrite(fd, chunk, current_offset)
                            chunk_len = len(chunk)
                            current_offset += chunk_len
                            bytes_written += chunk_len
                            
                            # Accumulate locally, flush periodically
                            local_bytes_accumulated += chunk_len
                            if local_bytes_accumulated >= flush_threshold:
                                if process_counter is not None:
                                    with process_counter.get_lock():
                                        process_counter.value += local_bytes_accumulated
                                local_bytes_accumulated = 0
                    finally:
                        os.close(fd)
                
                # Validate we got all expected bytes for THIS attempt
                bytes_received = current_offset - req_start
                if bytes_received != expected_bytes:
                    raise Exception(
                        f"Incomplete segment: expected {expected_bytes} bytes, got {bytes_received}"
                    )
                
                # Success - flush any remaining bytes
                if local_bytes_accumulated > 0 and process_counter is not None:
                    with process_counter.get_lock():
                        process_counter.value += local_bytes_accumulated
                
                return bytes_written
                
            except asyncio.CancelledError:
                if local_bytes_accumulated > 0 and process_counter is not None:
                    with process_counter.get_lock():
                        process_counter.value += local_bytes_accumulated
                raise
            except Exception as e:
                logging.warning(f"Segment attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    if local_bytes_accumulated > 0 and process_counter is not None:
                        with process_counter.get_lock():
                            process_counter.value += local_bytes_accumulated
                    raise
        
        return bytes_written
        
    finally:
        # Decrement active connections
        if active_connections is not None:
            with active_connections.get_lock():
                active_connections.value -= 1


async def download_single_connection(
    session: aiohttp.ClientSession,
    url: str,
    part_path: str,
    local_path: str,
    process_id: int,
    process_counter: mp.Value,
    active_connections: mp.Value,
    max_retries: int = 3
) -> bool:
    """
    Fallback single-connection download for files without Range support.
    Returns success status.
    """
    chunk_size = 128 * 1024
    local_bytes_accumulated = 0
    flush_threshold = 4 * 1024 * 1024
    
    # Update active connections
    if active_connections is not None:
        with active_connections.get_lock():
            active_connections.value += 1
    
    try:
        for attempt in range(max_retries):
            try:
                os.makedirs(os.path.dirname(part_path), exist_ok=True)
                
                async with session.get(url) as resp:
                    if resp.status != 200:
                        raise Exception(f"Expected 200 OK, got {resp.status}")
                    
                    fd = os.open(part_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
                    
                    try:
                        async for chunk in resp.content.iter_chunked(chunk_size):
                            if download_process_status[process_id] == 0:
                                raise asyncio.CancelledError("Download paused")
                            
                            _, free_now = available_space(download_dir)
                            while free_now * 1024 * 1024 <= (len(chunk) + chunk_size):
                                await asyncio.sleep(0.5)
                                _, free_now = available_space(download_dir)
                            
                            os.write(fd, chunk)
                            
                            local_bytes_accumulated += len(chunk)
                            if local_bytes_accumulated >= flush_threshold:
                                if process_counter is not None:
                                    with process_counter.get_lock():
                                        process_counter.value += local_bytes_accumulated
                                local_bytes_accumulated = 0
                        
                        # Success - flush remaining bytes
                        if local_bytes_accumulated > 0 and process_counter is not None:
                            with process_counter.get_lock():
                                process_counter.value += local_bytes_accumulated
                        
                        os.close(fd)
                        fd = None
                        
                        # Atomic rename
                        os.rename(part_path, local_path)
                        
                        logging.info(f"[Download #{process_id}] Completed {os.path.basename(local_path)}")
                        return True
                        
                    finally:
                        if fd is not None:
                            os.close(fd)
                    
            except asyncio.CancelledError:
                if local_bytes_accumulated > 0 and process_counter is not None:
                    with process_counter.get_lock():
                        process_counter.value += local_bytes_accumulated
                logging.info(f"[Download #{process_id}] Paused {os.path.basename(local_path)}")
                raise
            except Exception as e:
                logging.warning(f"Single-connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logging.error(f"[Download #{process_id}] Failed {os.path.basename(local_path)}: {e}")
                    return False
        
        return False
        
    finally:
        # Decrement active connections
        if active_connections is not None:
            with active_connections.get_lock():
                active_connections.value -= 1


#############################
# Async Worker
#############################
async def segment_download_worker(
    process_id: int,
    segment_queue: mp.Queue,
    failed_queue: mp.Queue,
    file_completion_dict: Dict,
    file_completion_lock: Lock,
    process_counter: mp.Value,
    active_connections: mp.Value,
    max_retries: int = 3
):
    """
    Async worker that processes segment download tasks from a queue.
    Each task is a single segment from potentially any file.
    """
    logging.info(f"[Worker #{process_id}] Starting segment download worker")
    
    # Create persistent session with connection pooling
    timeout = aiohttp.ClientTimeout(total=3600, connect=60, sock_read=300)
    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=100,
        ttl_dns_cache=300,
        enable_cleanup_closed=True
    )
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={'User-Agent': 'fastbiodl/4.0-segmented'}
    ) as session:
        
        while True:
            # Check if worker is paused
            if download_process_status[process_id] == 0:
                await asyncio.sleep(1)
                continue
            
            # Get next segment task from queue
            try:
                task_data = segment_queue.get(timeout=0.1)
                (url, file_path, segment_idx, start, end, file_size, 
                 total_segments, supports_ranges, retry_count) = task_data
            except:
                # No tasks available or timeout
                if transfer_done.value == 1:
                    break
                await asyncio.sleep(0.1)
                continue
            
            part_path = file_path + ".part"
            meta_path = file_path + ".part.meta"
            
            try:
                # Handle single-connection fallback for non-range files
                if not supports_ranges:
                    success = await download_single_connection(
                        session, url, part_path, file_path,
                        process_id, process_counter, active_connections, max_retries
                    )
                    
                    if success:
                        with download_complete.get_lock():
                            download_complete.value += 1
                    else:
                        failed_queue.put((url, file_path))
                        with failed_count.get_lock():
                            failed_count.value += 1
                    
                    segment_queue.task_done()
                    continue
                
                # Download the segment
                os.makedirs(os.path.dirname(part_path), exist_ok=True)
                
                # Ensure .part file exists
                if not os.path.exists(part_path):
                    fd = os.open(part_path, os.O_CREAT | os.O_RDWR)
                    os.close(fd)
                
                bytes_written = await download_segment(
                    session, url, start, end, part_path,
                    process_id, process_counter, active_connections, max_retries
                )
                
                # Mark segment complete
                all_segments = calculate_segments(
                    file_size, 
                    configurations.get("segment_size", 10 * 1024 * 1024),
                    configurations.get("min_file_size", 5 * 1024 * 1024)
                )
                mark_segment_complete(meta_path, file_size, all_segments, segment_idx, file_completion_lock)
                
                logging.debug(f"[Worker #{process_id}] Completed segment {segment_idx}/{total_segments-1} of {os.path.basename(file_path)}")
                
                # Check if all segments of this file are complete
                metadata = read_segment_metadata(meta_path)
                if metadata:
                    completed_segments = set(metadata.get('completed_indices', []))
                    if len(completed_segments) == total_segments:
                        # All segments complete - atomic rename and cleanup
                        os.rename(part_path, file_path)
                        if os.path.exists(meta_path):
                            os.remove(meta_path)
                        
                        with download_complete.get_lock():
                            download_complete.value += 1
                        
                        logging.info(f"[Worker #{process_id}] ✓ Completed file: {os.path.basename(file_path)}")
                
                segment_queue.task_done()
                
            except asyncio.CancelledError:
                # Paused - requeue with same retry count
                logging.debug(f"Re-queueing segment {segment_idx} after pause")
                segment_queue.put((url, file_path, segment_idx, start, end, 
                                 file_size, total_segments, supports_ranges, retry_count))
                segment_queue.task_done()
            except Exception as e:
                # Segment failed - retry or fail
                new_retry_count = retry_count + 1
                if new_retry_count <= max_retries:
                    logging.info(f"Re-queueing segment {segment_idx} after failure (retry {new_retry_count}/{max_retries})")
                    segment_queue.put((url, file_path, segment_idx, start, end, 
                                     file_size, total_segments, supports_ranges, new_retry_count))
                else:
                    logging.error(f"Segment {segment_idx} of {os.path.basename(file_path)} failed after {max_retries} retries")
                    failed_queue.put((url, file_path))
                    with failed_count.get_lock():
                        failed_count.value += 1
                
                segment_queue.task_done()
    
    logging.info(f"[Worker #{process_id}] Segment download worker finished")


def segment_worker_wrapper(
    process_id: int,
    segment_queue: mp.Queue,
    failed_queue: mp.Queue,
    file_completion_dict: Dict,
    file_completion_lock: Lock,
    process_counter: mp.Value,
    active_connections: mp.Value
):
    """Wrapper to run async worker in a sync process."""
    max_retries = configurations.get("max_retries", 3)
    
    asyncio.run(segment_download_worker(
        process_id,
        segment_queue,
        failed_queue,
        file_completion_dict,
        file_completion_lock,
        process_counter,
        active_connections,
        max_retries
    ))


#############################
# Reporting throughput
#############################
def report_network_throughput(process_counters: List[mp.Value], active_connections: mp.Value, 
                              throughput_logs: deque, throughput_lock: Lock):
    """
    Continuously logs per-second and cumulative throughput (Mbps).
    """
    previous_total, previous_time = 0, 0
    t = time.time()
    fname = f'log_download_{datetime.datetime.fromtimestamp(t).strftime("%Y%m%d_%H%M%S")}.csv'
    
    # Write CSV header
    with open(fname, 'w') as f:
        f.write("timestamp,elapsed_sec,current_mbps,avg_mbps,active_workers,est_connections\n")
    
    while start.value == 0:
        time.sleep(0.1)
    start_time = start.value
    
    while transfer_done.value == 0:
        t1 = time.time()
        elapsed = round(t1 - start_time, 1)
        
        if elapsed > 1000:
            with throughput_lock:
                if len(throughput_logs) >= 1000 and sum(list(throughput_logs)[-1000:]) == 0:
                    transfer_done.value = 1
                    break
        
        if elapsed >= 0.1:
            # Sum all process counters efficiently
            total_bytes = sum(pc.value for pc in process_counters)
            
            thrpt = round((total_bytes * 8) / (elapsed * 1000 * 1000), 2)
            curr_total = total_bytes - previous_total
            curr_time_sec = round(elapsed - previous_time, 3) or 0.001
            curr_thrpt = round((curr_total * 8) / (curr_time_sec * 1000 * 1000), 2)
            previous_time, previous_total = elapsed, total_bytes
            
            with throughput_lock:
                throughput_logs.append(curr_thrpt)
            
            # Get active connection count
            active_workers = sum(download_process_status)
            est_connections = active_connections.value
            
            logging.info(
                f"Download @{elapsed}s: Current: {curr_thrpt}Mbps, "
                f"Avg: {thrpt}Mbps, Workers: {active_workers}, "
                f"Connections: {est_connections}"
            )
            
            t2 = time.time()
            with open(fname, 'a') as f:
                f.write(f"{t2},{elapsed},{curr_thrpt},{thrpt},{active_workers},{est_connections}\n")
            
            time.sleep(max(0, 1 - (t2 - t1)))


#############################
# Optimizer functions
#############################
def download_probing(params, throughput_logs: deque, throughput_lock: Lock):
    """
    Probe function for the optimizer: toggles worker concurrency.
    """
    if transfer_done.value == 1:
        return exit_signal
    
    params = [1 if x < 1 else int(np.round(x)) for x in params]
    logging.info("Download -- Probing Parameters: " + str(params))
    
    for i in range(len(download_process_status)):
        download_process_status[i] = 1 if i < params[0] else 0
    
    time.sleep(1)
    n_time = time.time() + probing_time - 1.05
    
    while time.time() < n_time and transfer_done.value == 0:
        time.sleep(0.1)
    
    need = probing_time - 1
    with throughput_lock:
        recent_logs = list(throughput_logs)[-need:]
    thrpt = float(np.mean(recent_logs)) if len(recent_logs) >= need else 0.0
    K = float(configurations["K"])
    cc_impact_nl = K ** params[0]
    score = thrpt / cc_impact_nl if cc_impact_nl != 0 else 0
    score_value = int(np.round(score * (-1)))
    
    logging.info(
        f"Download Probing -- Throughput: {int(np.round(thrpt))}Mbps, "
        f"Score: {score_value}, Workers: {params[0]}, "
        f"Connections: {active_connections.value}"
    )
    
    if transfer_done.value == 1:
        return exit_signal
    else:
        return score_value


def run_download_optimizer(probing_func, throughput_logs: deque, throughput_lock: Lock):
    """
    Drives the optimization loop to adjust concurrency.
    """
    while start.value == 0:
        time.sleep(0.1)
    
    params = [2]
    method = configurations["method"].lower()
    
    if method == "gradient":
        logging.info("Running Gradient Optimization for Download....")
        params = gradient_opt_fast(configurations["thread_limit"], lambda p: probing_func(p, throughput_logs, throughput_lock), logging)
    else:
        logging.info("Running Bayesian Optimization for Download....")
        params = base_optimizer(configurations, lambda p: probing_func(p, throughput_logs, throughput_lock), logging)
    
    while transfer_done.value == 0:
        probing_func(params, throughput_logs, throughput_lock)


#############################
# Graceful exit handler
#############################
def graceful_exit(signum=None, frame=None):
    """Signal handler for SIGINT/SIGTERM."""
    logging.info(f"Graceful exit triggered: signum={signum}")
    try:
        transfer_done.value = 1
        move_complete.value = download_complete.value
    except Exception as e:
        logging.error(e)
    sys.exit(1)


#############################
# Main function
#############################
if __name__ == '__main__':
    # Set multiprocessing start method to 'fork' for Linux/HPC
    try:
        mp.set_start_method('fork')
    except RuntimeError:
        pass
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

    # Make log directory
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Configure logging
    log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
    log_file = f'logs/receiver.{datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}.log'
    
    if configurations.get("loglevel") == "debug":
        logging.basicConfig(
            format=log_FORMAT,
            datefmt='%m/%d/%Y %I:%M:%S %p',
            level=logging.DEBUG,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        mp.log_to_stderr(logging.DEBUG)
    else:
        logging.basicConfig(
            format=log_FORMAT,
            datefmt='%m/%d/%Y %I:%M:%S %p',
            level=logging.INFO,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    parser = argparse.ArgumentParser(
        description="Segment-based parallel NCBI SRA downloader"
    )
    parser.add_argument("-i", "--input", required=True,
                        help="Text file: one accession per line.")
    parser.add_argument("-o", "--outdir", default=".",
                        help="Where to save downloads.")
    parser.add_argument("--fastq", action="store_true",
                        help="Use fastq_ftp instead of sra_ftp")
    parser.add_argument("--segment-size", type=int, default=10,
                        help="Segment size in MB (default: 10)")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="Max retry attempts per segment (default: 3)")
    args = parser.parse_args()

    # Set configuration parameters
    configurations["cpu_count"] = mp.cpu_count()
    if configurations.get("thread_limit", -1) == -1:
        configurations["thread_limit"] = configurations["cpu_count"]
    
    configurations["segment_size"] = args.segment_size * 1024 * 1024
    configurations["min_file_size"] = 5 * 1024 * 1024
    configurations["max_retries"] = args.max_retries
    probing_time = configurations.get("probing_sec", 5)

    # Download directory
    download_dir = args.outdir
    
    try:
        os.makedirs(download_dir, exist_ok=True)
    except Exception as e:
        logging.error(f"Failed to create download directory: {e}")
        sys.exit(1)

    # Shared counters and structures
    download_complete = mp.Value("i", 0)
    failed_count = mp.Value("i", 0)
    move_complete = mp.Value("i", 0)
    transfer_done = mp.Value("i", 0)
    active_connections = mp.Value("i", 0)

    # Use deque instead of Manager list for better performance
    throughput_logs = deque(maxlen=10000)
    throughput_lock = Lock()
    
    # Segment queue and file completion tracking
    segment_queue = mp.JoinableQueue()
    failed_queue = mp.Queue()
    file_completion_dict = mp.Manager().dict()
    file_completion_lock = mp.Manager().Lock()

    # Read accessions and probe files
    logging.info("Phase 1: Probing files and creating segment tasks...")
    
    with open(args.input) as f:
        accs = [l.strip() for l in f if l.strip()]
    
    field = "fastq_ftp" if args.fastq else "sra_ftp"
    
    # Collect all file URLs first
    file_urls = []
    for acc in accs:
        try:
            url_acc_pairs = get_ncbi_urls(acc, field)
            for url, source_acc in url_acc_pairs:
                filename = os.path.basename(url)
                relative_path = os.path.join(source_acc, filename)
                local_path = os.path.join(download_dir, relative_path)
                file_urls.append((url, local_path))
        except Exception as e:
            logging.error(f"NCBI lookup failed for {acc}: {e}")
            continue
    
    if not file_urls:
        logging.error("No files to download!")
        sys.exit(1)
    
    logging.info(f"Found {len(file_urls)} files, probing for segmentation...")
    
    # Probe all files and create segment tasks
    async def probe_and_queue_all():
        timeout = aiohttp.ClientTimeout(total=60, connect=30)
        connector = aiohttp.TCPConnector(limit=50)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            total_segments = 0
            files_processed = 0
            
            for url, local_path in file_urls:
                # Check if already complete
                if os.path.exists(local_path):
                    file_size, _ = await probe_file(session, url)
                    if file_size and os.path.getsize(local_path) == file_size:
                        logging.info(f"Already complete: {os.path.basename(local_path)}")
                        download_complete.value += 1
                        files_processed += 1
                        continue
                
                # Probe file
                file_size, supports_ranges = await probe_file(session, url)
                
                if file_size is None:
                    logging.warning(f"Could not determine size for {url}, will try single connection")
                    # Queue as single-connection task
                    segment_queue.put((url, local_path, 0, 0, 0, 0, 1, False, 0))
                    total_segments += 1
                    files_processed += 1
                    continue
                
                # Check for existing partial download and resume
                part_path = local_path + ".part"
                meta_path = local_path + ".part.meta"
                
                segments = calculate_segments(
                    file_size,
                    configurations["segment_size"],
                    configurations["min_file_size"]
                )
                
                completed_segments = set()
                metadata = read_segment_metadata(meta_path)
                
                if metadata:
                    # Validate metadata matches
                    if (metadata.get('file_size') == file_size and 
                        metadata.get('segments') == [[s, e] for s, e in segments]):
                        completed_segments = set(metadata.get('completed_indices', []))
                        logging.info(
                            f"Resume: {len(completed_segments)}/{len(segments)} segments complete for {os.path.basename(local_path)}"
                        )
                    else:
                        # Mismatch - clean up
                        if os.path.exists(meta_path):
                            os.remove(meta_path)
                        if os.path.exists(part_path):
                            os.remove(part_path)
                
                # Queue remaining segments
                for idx, (start, end) in enumerate(segments):
                    if idx not in completed_segments:
                        segment_queue.put((
                            url, local_path, idx, start, end,
                            file_size, len(segments), supports_ranges, 0
                        ))
                        total_segments += 1
                
                # If all segments already complete, mark file done
                if len(completed_segments) == len(segments):
                    if os.path.exists(part_path):
                        os.rename(part_path, local_path)
                    if os.path.exists(meta_path):
                        os.remove(meta_path)
                    download_complete.value += 1
                    logging.info(f"Already complete (from resume): {os.path.basename(local_path)}")
                
                files_processed += 1
                if files_processed % 10 == 0:
                    logging.info(f"Probed {files_processed}/{len(file_urls)} files...")
            
            return total_segments, files_processed
    
    total_segments, files_processed = asyncio.run(probe_and_queue_all())
    initial_file_count = len(file_urls)
    
    logging.info(f"Phase 2: Downloading {total_segments} segments across {files_processed} files...")
    
    if total_segments == 0:
        logging.info("All files already complete!")
        sys.exit(0)
    
    # Start workers
    num_workers = min(configurations["thread_limit"], total_segments)
    download_process_status = mp.Array("i", [1 for _ in range(num_workers)])
    
    # Per-process byte counters
    process_counters = [mp.Value('Q', 0) for _ in range(num_workers)]

    # Start segment download workers
    download_workers = [
        mp.Process(
            target=segment_worker_wrapper,
            args=(i, segment_queue, failed_queue, file_completion_dict,
                  file_completion_lock, process_counters[i], active_connections)
        )
        for i in range(num_workers)
    ]
    for p in download_workers:
        p.daemon = True
        p.start()

    # Start reporting and optimization
    start = mp.Value("d", time.time())
    
    network_report_thread = Thread(
        target=report_network_throughput,
        args=(process_counters, active_connections, throughput_logs, throughput_lock)
    )
    network_report_thread.start()
    
    download_optimizer_thread = Thread(
        target=run_download_optimizer,
        args=(download_probing, throughput_logs, throughput_lock)
    )
    download_optimizer_thread.start()

    # Wait for completion
    while (download_complete.value + failed_count.value) < initial_file_count and transfer_done.value == 0:
        time.sleep(0.5)
    
    transfer_done.value = 1
    logging.info(f"Download Tasks Completed! Success: {download_complete.value}, Failed: {failed_count.value}")
    
    # Report failed downloads
    failed_list = []
    while not failed_queue.empty():
        failed_list.append(failed_queue.get())
    
    if failed_list:
        logging.warning(f"Failed to download {len(failed_list)} files:")
        failed_log = f'failed_downloads_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        with open(failed_log, 'w') as f:
            for url, path in failed_list:
                logging.warning(f"  - {path}")
                f.write(f"{url}\t{path}\n")
        logging.warning(f"Failed downloads written to: {failed_log}")
    
    time.sleep(2)

    # Cleanup
    for p in download_workers:
        if p.is_alive():
            p.terminate()
            p.join(timeout=1)

    logging.info("Transfer Completed!")
    sys.exit(0)