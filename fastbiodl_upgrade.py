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
from converter import SRAConverter
from mover import FileMover
import queue

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
# Async Download Workers
#############################
class SegmentedDownloader:
    """
    Handles multi-segment downloading with direct streaming to disk offsets.
    Uses .part files and proper resume logic.
    """
    
    def __init__(
        self, 
        session: aiohttp.ClientSession,
        url: str,
        local_path: str,
        segment_size: int = 10 * 1024 * 1024,  # 10MB segments
        min_file_size_for_segmentation: int = 5 * 1024 * 1024,  # 5MB minimum
        max_segments: int = 8,
        process_id: int = 0,
        process_counter: mp.Value = None,
        active_connections: mp.Value = None,
        max_retries: int = 3
    ):
        self.session = session
        self.url = url
        self.local_path = local_path
        self.part_path = local_path + ".part"
        self.meta_path = local_path + ".part.meta"
        self.segment_size = segment_size
        self.min_file_size = min_file_size_for_segmentation
        self.max_segments = max_segments
        self.process_id = process_id
        self.process_counter = process_counter
        self.active_connections = active_connections
        self.max_retries = max_retries
        self.total_size = 0
        self.segments: List[Tuple[int, int]] = []
        
        # Batched counter updates to reduce lock contention
        self.local_bytes_accumulated = 0
        self.flush_threshold = 4 * 1024 * 1024  # Flush every 4MB
    
    def read_metadata(self) -> Optional[Dict]:
        """Read segment completion metadata from .meta file."""
        if not os.path.exists(self.meta_path):
            return None
        try:
            with open(self.meta_path, 'r') as f:
                return json.load(f)
        except:
            return None
    
    def write_metadata(self, file_size: int, segments: List[Tuple[int, int]], completed: Set[int]):
        """Write segment completion metadata to .meta file with atomic write."""
        metadata = {
            'file_size': file_size,
            'segments': [[s, e] for s, e in segments],  # Store as lists for JSON compatibility
            'completed_indices': list(completed)
        }
        # Atomic write: write to temp file, then rename
        meta_tmp = self.meta_path + '.tmp'
        with open(meta_tmp, 'w') as f:
            json.dump(metadata, f)
            f.flush()
            os.fsync(f.fileno())
        os.rename(meta_tmp, self.meta_path)
    
    def mark_segment_complete(self, segment_idx: int, file_size: int, segments: List[Tuple[int, int]], completed: Set[int]):
        """Mark a segment as complete and update metadata."""
        completed.add(segment_idx)
        self.write_metadata(file_size, segments, completed)
        
    def flush_counter(self, force=False):
        """Flush accumulated bytes to shared counter."""
        if self.local_bytes_accumulated > 0 and (force or self.local_bytes_accumulated >= self.flush_threshold):
            if self.process_counter is not None:
                with self.process_counter.get_lock():
                    self.process_counter.value += self.local_bytes_accumulated
            self.local_bytes_accumulated = 0
    
    async def probe_range_support(self) -> Tuple[Optional[int], bool]:
        """
        Probe for Range support using a small Range GET request.
        More reliable than checking Accept-Ranges header.
        Returns (file_size, supports_ranges).
        """
        try:
            # First try HEAD to get file size
            async with self.session.head(self.url, allow_redirects=True) as resp:
                if resp.status == 200:
                    content_length = resp.headers.get('Content-Length')
                    file_size = int(content_length) if content_length else None
                else:
                    file_size = None
            
            # Probe with tiny Range request to confirm support
            headers = {'Range': 'bytes=0-0'}
            async with self.session.get(self.url, headers=headers) as resp:
                if resp.status == 206:
                    # Server supports ranges
                    supports_ranges = True
                    
                    # Try to get file size from Content-Range if we don't have it
                    if file_size is None:
                        content_range = resp.headers.get('Content-Range', '')
                        # Format: bytes 0-0/1234
                        if '/' in content_range:
                            try:
                                file_size = int(content_range.split('/')[-1])
                            except:
                                pass
                else:
                    supports_ranges = False
            
            return file_size, supports_ranges
            
        except Exception as e:
            logging.debug(f"Range probe failed for {self.url}: {e}")
            return None, False
    
    def calculate_segments(self, file_size: int) -> List[Tuple[int, int]]:
        """
        Calculate segment ranges for parallel downloading.
        Returns list of (start_byte, end_byte) tuples.
        """
        # Don't segment small files
        if file_size < self.min_file_size:
            return [(0, file_size - 1)]
        
        # Calculate number of segments
        num_segments = min(
            self.max_segments,
            max(1, file_size // self.segment_size)
        )
        
        segment_list = []
        bytes_per_segment = file_size // num_segments
        
        for i in range(num_segments):
            start = i * bytes_per_segment
            # Last segment gets any remainder bytes
            end = file_size - 1 if i == num_segments - 1 else (i + 1) * bytes_per_segment - 1
            segment_list.append((start, end))
        
        return segment_list
    
    async def download_segment_streaming(
        self, 
        segment_id: int,
        start: int, 
        end: int,
        fd: int
    ) -> Tuple[int, int]:
        """
        Download a single segment and write directly to file descriptor at correct offset.
        Returns (segment_id, bytes_written).
        Validates that returned range matches request and all bytes received.
        """
        chunk_size = 1024 * 1024  # 128KB chunks
        bytes_written = 0
        current_offset = start
        
        # Retry logic with exponential backoff
        for attempt in range(self.max_retries):
            # Track the requested range for THIS attempt
            req_start = current_offset
            req_end = end
            expected_bytes = req_end - req_start + 1
            
            headers = {'Range': f'bytes={req_start}-{req_end}'}
            
            try:
                async with self.session.get(self.url, headers=headers) as resp:
                    # STRICT: Require 206 for Range requests
                    if resp.status != 206:
                        raise Exception(
                            f"Expected 206 Partial Content for Range request, got {resp.status}. "
                            f"Server may not support ranges or is ignoring Range header."
                        )
                    
                    # Validate Content-Range header
                    content_range = resp.headers.get('Content-Range', '')
                    if not content_range:
                        raise Exception("Server returned 206 but no Content-Range header")
                    
                    # Parse and validate Content-Range: bytes start-end/total
                    if not content_range.startswith('bytes '):
                        raise Exception(f"Invalid Content-Range format: {content_range}")
                    
                    # Validate returned range matches what we asked for THIS attempt
                    try:
                        range_part = content_range.split()[1]  # "start-end/total"
                        returned_range = range_part.split('/')[0]  # "start-end"
                        returned_start, returned_end = map(int, returned_range.split('-'))
                        if returned_start != req_start or returned_end != req_end:
                            raise Exception(
                                f"Server returned different range: asked {req_start}-{req_end}, got {returned_start}-{returned_end}"
                            )
                    except Exception as e:
                        raise Exception(f"Failed to validate Content-Range '{content_range}': {e}")
                    
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        # Check if paused
                        if download_process_status[self.process_id] == 0:
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
                        self.local_bytes_accumulated += chunk_len
                        self.flush_counter()
                
                # Validate we got all expected bytes for THIS attempt
                bytes_received = current_offset - req_start
                if bytes_received != expected_bytes:
                    raise Exception(
                        f"Incomplete segment: expected {expected_bytes} bytes, got {bytes_received}"
                    )
                
                # Success - flush any remaining bytes and return
                self.flush_counter(force=True)
                return segment_id, bytes_written
                
            except asyncio.CancelledError:
                self.flush_counter(force=True)
                logging.info(f"Segment {segment_id} paused at offset {current_offset}")
                raise
            except Exception as e:
                logging.warning(f"Segment {segment_id} attempt {attempt + 1}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries - 1:
                    # Exponential backoff
                    await asyncio.sleep(2 ** attempt)
                    # current_offset is already updated to where we left off
                    # Next iteration will use it as req_start
                else:
                    # Final attempt failed
                    self.flush_counter(force=True)
                    raise
        
        self.flush_counter(force=True)
        return segment_id, bytes_written
    
    async def download_with_resume(self) -> Tuple[bool, bool, int]:
        """
        Download file with resume support using .part files.
        Returns (success, was_paused, num_connections) tuple.
        """
        # Check if final file already exists and is complete
        if os.path.exists(self.local_path):
            file_size, _ = await self.probe_range_support()
            if file_size and os.path.getsize(self.local_path) == file_size:
                logging.info(f"[Download #{self.process_id}] Already complete: {os.path.basename(self.local_path)}")
                if self.process_counter is not None:
                    with self.process_counter.get_lock():
                        self.process_counter.value += file_size
                return True, False, 0
        
        # Get file info via Range probe
        file_size, supports_ranges = await self.probe_range_support()
        
        if file_size is None:
            logging.warning(f"Could not determine file size for {self.url}, attempting direct download")
            return await self.download_single_connection(supports_ranges=False)
        
        # Check existing partial download
        existing_size = 0
        if os.path.exists(self.part_path):
            existing_size = os.path.getsize(self.part_path)
            
            # Partial download is corrupted if larger than expected
            if existing_size > file_size:
                logging.warning(f"Partial file larger than expected, restarting: {self.part_path}")
                os.remove(self.part_path)
                existing_size = 0
        
        # Decide on strategy
        if not supports_ranges:
            logging.debug(f"Server doesn't support ranges for {self.url}, using single connection")
            return await self.download_single_connection(supports_ranges=False, resume_from=existing_size)
        
        # Use segmented download
        return await self.download_segmented(file_size)
    
    async def download_segmented(self, file_size: int) -> Tuple[bool, bool, int]:
        """
        Download file in multiple segments, streaming directly to disk.
        Uses .meta file to track completed segments (not file size).
        Returns (success, was_paused, num_connections) tuple.
        """
        segments = self.calculate_segments(file_size)
        
        # Load metadata to see which segments are already complete
        metadata = self.read_metadata()
        completed_segments: Set[int] = set()
        
        if metadata:
            # Validate metadata matches current file/segments
            if (metadata.get('file_size') == file_size and 
                metadata.get('segments') == [[s, e] for s, e in segments]):  # Compare as lists
                completed_segments = set(metadata.get('completed_indices', []))
                logging.info(
                    f"[Download #{self.process_id}] Resume: {len(completed_segments)}/{len(segments)} "
                    f"segments already complete for {os.path.basename(self.local_path)}"
                )
            else:
                logging.warning(
                    f"[Download #{self.process_id}] Metadata mismatch, restarting: {os.path.basename(self.local_path)}"
                )
                # Metadata doesn't match - delete stale files and start fresh
                if os.path.exists(self.meta_path):
                    os.remove(self.meta_path)
                if os.path.exists(self.part_path):
                    os.remove(self.part_path)
                completed_segments = set()
        
        # Determine remaining segments
        remaining_segments = [
            (i, start, end) 
            for i, (start, end) in enumerate(segments) 
            if i not in completed_segments
        ]
        
        if not remaining_segments:
            # Already complete
            logging.info(f"[Download #{self.process_id}] All segments complete: {os.path.basename(self.local_path)}")
            # Rename .part to final and clean up metadata
            if os.path.exists(self.part_path):
                os.rename(self.part_path, self.local_path)
            if os.path.exists(self.meta_path):
                os.remove(self.meta_path)
            return True, False, 0  # No connections needed
        
        logging.info(
            f"[Download #{self.process_id}] Downloading {os.path.basename(self.local_path)} "
            f"in {len(remaining_segments)}/{len(segments)} segments ({file_size} bytes)"
        )
        
        os.makedirs(os.path.dirname(self.part_path), exist_ok=True)
        
        # Track actual connections we'll use
        num_connections = len(remaining_segments)
        
        # Update active connections counter
        if self.active_connections is not None:
            with self.active_connections.get_lock():
                self.active_connections.value += num_connections
        
        fd = None
        
        # NOTE: We do NOT use ftruncate here because it breaks resume logic.
        # ftruncate would make getsize() return file_size immediately (file full of holes),
        # which would make us think the download is complete when it's not.
        # We accept potential fragmentation instead of silent corruption.
        
        try:
            # Open file descriptor for .part file
            fd = os.open(self.part_path, os.O_CREAT | os.O_RDWR)
            # Download all remaining segments concurrently
            tasks = [
                self.download_segment_streaming(i, start, end, fd)
                for i, start, end in remaining_segments
            ]
            
            # Use gather with return_exceptions to handle partial completion
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # First collect all successful segments (don't bail early)
            paused = any(isinstance(r, asyncio.CancelledError) for r in results)
            for result in results:
                if isinstance(result, tuple):
                    seg_id, bytes_written = result
                    completed_segments.add(seg_id)
            
            # Check if paused and save progress before raising
            if paused:
                self.write_metadata(file_size, segments, completed_segments)
                return False, True, num_connections  # Not successful, but was paused
            
            # Check if all segments completed
            if len(completed_segments) == len(segments):
                # All segments complete - atomic rename and cleanup
                os.close(fd)
                fd = None
                os.rename(self.part_path, self.local_path)
                if os.path.exists(self.meta_path):
                    os.remove(self.meta_path)
                
                logging.info(f"[Download #{self.process_id}] Completed {os.path.basename(self.local_path)}")
                return True, False, num_connections
            else:
                # Some segments failed
                self.write_metadata(file_size, segments, completed_segments)
                raise Exception(f"Not all segments completed: {len(completed_segments)}/{len(segments)}")
            
        except asyncio.CancelledError:
            # Save progress to metadata before exiting
            self.write_metadata(file_size, segments, completed_segments)
            logging.info(f"[Download #{self.process_id}] Paused {os.path.basename(self.local_path)}")
            return False, True, num_connections  # Not successful, but was paused (not failed)
        except Exception as e:
            # Save whatever progress we have
            self.write_metadata(file_size, segments, completed_segments)
            logging.error(f"[Download #{self.process_id}] Failed {os.path.basename(self.local_path)}: {e}")
            return False, False, num_connections  # Failed, not paused
        finally:
            if fd is not None:
                os.close(fd)
            # Decrement active connections
            if self.active_connections is not None:
                with self.active_connections.get_lock():
                    self.active_connections.value -= num_connections
    
    async def download_single_connection(
        self, 
        supports_ranges: bool = True,
        resume_from: int = 0
    ) -> Tuple[bool, bool, int]:
        """
        Fallback single-connection download with resume support.
        Returns (success, was_paused, num_connections) tuple.
        """
        chunk_size = 1024 * 1024
        num_connections = 1
        
        # Update active connections counter
        if self.active_connections is not None:
            with self.active_connections.get_lock():
                self.active_connections.value += num_connections
        
        try:
            # Retry logic
            for attempt in range(self.max_retries):
                try:
                    headers = {}
                    if resume_from > 0 and supports_ranges:
                        headers['Range'] = f'bytes={resume_from}-'
                    
                    os.makedirs(os.path.dirname(self.part_path), exist_ok=True)
                    
                    async with self.session.get(self.url, headers=headers) as resp:
                        # Handle Range request response
                        if resume_from > 0 and supports_ranges:
                            if resp.status == 206:
                                # Proper partial content - resume
                                initial_offset = resume_from
                            elif resp.status == 200:
                                # Server ignored Range header - restart from beginning
                                logging.warning(
                                    f"Server returned 200 instead of 206 for Range request, "
                                    f"restarting download from beginning"
                                )
                                initial_offset = 0
                                if os.path.exists(self.part_path):
                                    os.remove(self.part_path)
                            else:
                                raise Exception(f"Unexpected status {resp.status} for Range request")
                        else:
                            if resp.status != 200:
                                raise Exception(f"Expected 200 OK, got {resp.status}")
                            initial_offset = 0
                        
                        fd = None
                        current_offset = initial_offset
                        
                        try:
                            fd = os.open(self.part_path, os.O_CREAT | os.O_RDWR)
                            os.lseek(fd, initial_offset, os.SEEK_SET)
                            async for chunk in resp.content.iter_chunked(chunk_size):
                                if download_process_status[self.process_id] == 0:
                                    raise asyncio.CancelledError("Download paused")
                                
                                _, free_now = available_space(download_dir)
                                while free_now * 1024 * 1024 <= (len(chunk) + chunk_size):
                                    await asyncio.sleep(0.5)
                                    _, free_now = available_space(download_dir)
                                
                                os.write(fd, chunk)
                                current_offset += len(chunk)
                                
                                # Batched counter updates
                                self.local_bytes_accumulated += len(chunk)
                                self.flush_counter()
                            
                            # Success
                            self.flush_counter(force=True)
                            os.close(fd)
                            fd = None
                            
                            # Atomic rename
                            os.rename(self.part_path, self.local_path)
                            
                            logging.info(f"[Download #{self.process_id}] Completed {os.path.basename(self.local_path)}")
                            return True, False, num_connections  # Single connection
                            
                        finally:
                            if fd is not None:
                                os.close(fd)
                        
                except asyncio.CancelledError:
                    self.flush_counter(force=True)
                    logging.info(f"[Download #{self.process_id}] Paused {os.path.basename(self.local_path)}")
                    return False, True, num_connections  # Not successful, but was paused
                except Exception as e:
                    logging.warning(f"Single-connection attempt {attempt + 1}/{self.max_retries} failed: {e}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        logging.error(f"[Download #{self.process_id}] Failed {os.path.basename(self.local_path)}: {e}")
                        return False, False, num_connections
            
            return False, False, num_connections  # Failed after all retries
        finally:
            # Decrement active connections
            if self.active_connections is not None:
                with self.active_connections.get_lock():
                    self.active_connections.value -= num_connections


async def download_worker_async(
    process_id: int,
    task_queue: mp.Queue,
    failed_queue: mp.Queue,
    failed_count: mp.Value,
    process_counter: mp.Value,
    active_connections: mp.Value,
    segment_size: int = 10 * 1024 * 1024,
    max_segments: int = 8,
    max_retries: int = 3,
    processing_queue: mp.Queue = None
):
    """
    Async worker that processes download tasks from a queue with connection pooling.
    """
    logging.info(f"[Download #{process_id}] Async worker starting")
    
    # Create persistent session with connection pooling
    timeout = aiohttp.ClientTimeout(total=3600, connect=60, sock_read=300)
    connector = aiohttp.TCPConnector(
        limit=max_segments,
        limit_per_host=max_segments,
        ttl_dns_cache=300,
        enable_cleanup_closed=True
    )
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={'User-Agent': 'fastbiodl/3.0'}
    ) as session:
        
        while True:
            # Check if worker is paused
            if download_process_status[process_id] == 0:
                await asyncio.sleep(1)
                continue
            
            # Get next task from queue with timeout to avoid spinning
            try:
                # Use blocking get with timeout (can't use async, so use get_nowait with better sleep)
                task_data = task_queue.get(timeout=0.1)
                url, relative_path, retry_count = task_data
            except queue.Empty:
                if transfer_done.value == 1:
                    break
                await asyncio.sleep(0.1)
                continue
            except Exception as e:
                logging.error(f"Worker {process_id} unexpected error: {e}")
                await asyncio.sleep(0.1)
                continue
            
            local_path = os.path.join(download_dir, relative_path)
            
            try:
                # Create downloader and execute
                downloader = SegmentedDownloader(
                    session=session,
                    url=url,
                    local_path=local_path,
                    segment_size=segment_size,
                    max_segments=max_segments,
                    process_id=process_id,
                    process_counter=process_counter,
                    active_connections=active_connections,
                    max_retries=max_retries
                )
                
                success, was_paused, _ = await downloader.download_with_resume()
                
                if success:
                    with download_complete.get_lock():
                        download_complete.value += 1
                    processing_queue.put(local_path)   # ← hand off to conversion stage
                    logging.info(f"[Download #{process_id}] Queued for processing: {local_path}")
                    task_queue.task_done()
                elif was_paused:
                    # Paused by optimizer - requeue with SAME retry count
                    logging.debug(f"Re-queueing {relative_path} after pause (retry {retry_count}/{max_retries})")
                    task_queue.put((url, relative_path, retry_count))  # Don't increment!
                    task_queue.task_done()
                else:
                    # Actually failed - increment retry count
                    new_retry_count = retry_count + 1
                    if new_retry_count <= max_retries:
                        logging.info(f"Re-queueing {relative_path} after failure (retry {new_retry_count}/{max_retries})")
                        task_queue.put((url, relative_path, new_retry_count))
                    else:
                        logging.error(f"Max retries exceeded for {relative_path}, adding to failed list")
                        failed_queue.put((url, relative_path))
                        with failed_count.get_lock():
                            failed_count.value += 1
                    task_queue.task_done()
            
            except Exception as e:
                logging.error(f"Worker {process_id} error on {relative_path}: {e}")
                task_queue.task_done()
    
    logging.info(f"[Download #{process_id}] Async worker finished")


def download_file_worker(
    process_id: int,
    task_queue: mp.Queue,
    failed_queue: mp.Queue,
    failed_count: mp.Value,
    process_counter: mp.Value,
    active_connections: mp.Value,
    processing_queue
):
    """
    Wrapper to run async worker in a sync process.
    """
    segment_size = configurations.get("segment_size", 10 * 1024 * 1024)
    max_segments = configurations.get("max_segments", 8)
    max_retries = configurations.get("max_retries", 3)
    
    asyncio.run(download_worker_async(
        process_id, 
        task_queue,
        failed_queue,
        failed_count,
        process_counter,
        active_connections,
        segment_size, 
        max_segments,
        max_retries,
        processing_queue
    ))


#############################
# Reporting throughput
#############################
def report_network_throughput(process_counters: List[mp.Value], active_connections: mp.Value, throughput_logs: deque, throughput_lock: Lock):
    """
    Continuously logs per-second and cumulative throughput (Mbps).
    Reads from per-process counters instead of expensive Manager dict.
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
    Now aware of actual connection counts.
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
    
    with throughput_lock:
        recent_logs = list(throughput_logs)[-(probing_time-1):] if len(throughput_logs) > 0 else []
    need = probing_time - 1
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
        params = gradient_opt_fast(max(1, (files_to_download.value - download_complete.value)), lambda p: probing_func(p, throughput_logs, throughput_lock), logging)
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
    # This allows child processes to inherit global state
    # Note: On macOS this may cause issues; use 'spawn' if needed
    try:
        mp.set_start_method('fork')
    except RuntimeError:
        # Already set, ignore
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
        description="Production-grade parallel NCBI SRA downloader"
    )
    parser.add_argument("-i", "--input", required=True,
                        help="Text file: one accession per line.")
    parser.add_argument("-o", "--outdir", default="/mnt/nvme0n1/fastbiodl/staging/",
                        help="Where to save downloads.")
    parser.add_argument("--fastq", action="store_true",
                        help="Use fastq_ftp instead of sra_ftp")
    parser.add_argument("--segment-size", type=int, default=512,
                        help="Segment size in MB (default: 10)")
    parser.add_argument("--max-segments", type=int, default=8,
                        help="Max segments per file (default: 8)")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="Max retry attempts per task (default: 3)")
    args = parser.parse_args()

    # Set configuration parameters
    configurations["cpu_count"] = mp.cpu_count()
    if configurations.get("thread_limit", -1) == -1:
        configurations["thread_limit"] = configurations["cpu_count"]
    
    configurations["segment_size"] = args.segment_size * 1024 * 1024
    configurations["max_segments"] = args.max_segments
    configurations["max_retries"] = args.max_retries
    probing_time = configurations.get("probing_sec", 5)

    # Download directory - use outdir from args
    # REPLACE this block:
    download_dir = args.outdir

    # WITH:
    tmpfs_dir    = f"/mnt/nvme0n1/fastbiodl_{os.getpid()}/"
    download_dir = tmpfs_dir                # downloads land here
    root_dir     = args.outdir              # final destination (NVMe / lustre / etc.)
    os.makedirs(tmpfs_dir, exist_ok=True)
    
    try:
        os.makedirs(download_dir, exist_ok=True)
    except Exception as e:
        logging.error(f"Failed to create download directory: {e}")
        sys.exit(1)

    # Shared counters and structures
    download_complete = mp.Value("i", 0)
    failed_count = mp.Value("i", 0)  # Track failed downloads
    move_complete = mp.Value("i", 0)
    transfer_done = mp.Value("i", 0)
    active_connections = mp.Value("i", 0)  # Track actual connection count

    # Use deque instead of Manager list for better performance
    throughput_logs = deque(maxlen=10000)
    throughput_lock = Lock()
    
    # Use JoinableQueue for tasks
    download_queue = mp.JoinableQueue()
    failed_queue = mp.Queue()  # Track failed downloads
    processing_queue = mp.Queue()
    move_queue = mp.Queue()

    # Read accessions and build download tasks
    with open(args.input) as f:
        accs = [l.strip() for l in f if l.strip()]
    
    field = "fastq_ftp" if args.fastq else "sra_ftp"
    task_count = 0
    
    for acc in accs:
        try:
            url_acc_pairs = get_ncbi_urls(acc, field)
        except Exception as e:
            logging.error(f"NCBI lookup failed for {acc}: {e}")
            continue
        
        for url, source_acc in url_acc_pairs:
            # Create accession-specific subdirectory to avoid collisions
            filename = os.path.basename(url)
            relative_path = os.path.join(source_acc, filename)
            # Queue format: (url, relative_path, retry_count)
            download_queue.put((url, relative_path, 0))
            task_count += 1

    initial_task_count = task_count
    files_to_download = mp.Value("i", task_count)
    logging.info(f"Total files to download: {initial_task_count}")
    
    if initial_task_count == 0:
        logging.error("No files to download!")
        sys.exit(1)
    
    num_workers = min(initial_task_count, configurations["thread_limit"])
    download_process_status = mp.Array("i", [0 for _ in range(num_workers)])
    
    # Per-process byte counters (no Manager overhead)
    process_counters = [mp.Value('Q', 0) for _ in range(num_workers)]

    # Start download workers
    download_workers = [
        mp.Process(
            target=download_file_worker, 
            args=(i, download_queue, failed_queue, failed_count, process_counters[i], active_connections, processing_queue)
        ) 
        for i in range(num_workers)
    ]
    for p in download_workers:
        p.daemon = True
        p.start()

    converter = SRAConverter(
        processing_queue=processing_queue,
        move_queue=move_queue,
        work_dir=tmpfs_dir,
        nvme_device="nvme0n1",
        threads_per_job=configurations.get("conversion_threads", 4),
        cpu_threshold=configurations.get("cpu_threshold", 85.0),
        nvme_threshold=configurations.get("nvme_threshold", 80.0),
        max_jobs=configurations.get("max_conversion_jobs", None),
    )
    converter.start()

    configurations["thread_limit"] = configurations.get("max_cc", mp.cpu_count())
    mover = FileMover(
        move_queue=move_queue,
        tmpfs_dir=tmpfs_dir,
        root_dir=root_dir,
        config=configurations,
    )
    mover.start()

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

    # Wait for completion (success or failure)
    while (download_complete.value + failed_count.value) < initial_task_count and transfer_done.value == 0:
        time.sleep(0.5)
    
    transfer_done.value = 1
    logging.info(f"Download Tasks Completed! Success: {download_complete.value}, Failed: {failed_count.value}")
    processing_queue.put(None)       # ← sentinel to unblock dispatcher
    converter.stop(timeout=7200.0)     # ← wait for in-flight jobs to finish
    logging.info(f"Conversion complete: {converter.converted_count} ok, {converter.failed_count} failed")
    
    # Drain any stale sentinels or leftovers from processing_queue.
    # With the Bug #6 fix in converter.py (dispatcher now exits via sentinel
    # before stop() sets _stop_event), this should always be empty.  Log a
    # critical error if anything is found so it is visible without silently
    # shipping corrupt data downstream.
    unprocessed_count = 0
    while True:
        try:
            leftover = processing_queue.get_nowait()
            if leftover is not None:  # sentinel is None, skip it
                logging.critical(
                    f"[Bug #6] Unprocessed .sra still in queue after converter.stop(): "
                    f"{leftover} — NOT moved (would be raw/unconverted). "
                    f"File left on disk for manual inspection."
                )
                unprocessed_count += 1
        except queue.Empty:
            break

    if unprocessed_count > 0:
        logging.critical(
            f"[Bug #6] {unprocessed_count} file(s) were NOT converted. "
            f"This indicates a bug — please report."
        )

    move_queue.put(None)          # sentinel → FileMover feeder exits, sets transfer_done
    mover.stop(timeout=7200.0)   # waits for all .fastq.gz to land on root_dir
    logging.info("All files moved to final destination.")
    
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