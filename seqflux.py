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
from config_seqflux import configurations, get_seqflux_tmpfs_dir
from storage_config import get_nvme_device
from utils import available_space, available_space_bytes
from search import base_optimizer, gradient_opt_fast, exit_signal

from typing import List, Tuple, Optional, Dict, Set
import argparse
import json
from converter import SRAConverter
import queue
from ncbi_lookup import get_ncbi_urls as shared_get_ncbi_urls

# Suppress FutureWarnings
warnings.filterwarnings("ignore", category=FutureWarning)

RUN_LOG_DIR = os.path.join("logs", "seqflux")


def _accession_group_name(input_path: str) -> str:
    """Derive a safe folder name from the accession input filename."""
    raw_name = os.path.splitext(os.path.basename(input_path))[0] or "accessions"
    safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in raw_name)
    safe_name = safe_name.strip("._-")
    return safe_name or "accessions"

#############################
# NCBI URL fetching
#############################
def get_ncbi_urls(acc: str, field: str = "sra_ftp") -> List[Tuple[str, str]]:
    """Compatibility wrapper around shared NCBI lookup implementation."""
    return shared_get_ncbi_urls(
        acc,
        field=field,
        max_attempts=int(configurations.get("ncbi_lookup_retries", 5)),
        backoff_base=float(configurations.get("ncbi_lookup_backoff_base", 1.0)),
        timeout=int(configurations.get("ncbi_lookup_timeout", 30)),
        max_rps=float(configurations.get("ncbi_lookup_rps", 2.0)),
        user_agent=str(configurations.get("ncbi_user_agent", "seqflux/3.0 (+https://github.com/)")),
        tool_name=str(configurations.get("ncbi_tool_name", "seqflux")),
        email=os.environ.get("NCBI_EMAIL", configurations.get("ncbi_email", "")),
        api_key=os.environ.get("NCBI_API_KEY", configurations.get("ncbi_api_key", "")),
        logger=logging,
    )


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
        disk_reserved_bytes: mp.Value = None,
        min_pending_conversion_bytes: mp.Value = None,
        disk_safety_margin_bytes: int = 0,
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
        self.disk_reserved_bytes = disk_reserved_bytes
        self.min_pending_conversion_bytes = min_pending_conversion_bytes
        self.disk_safety_margin_bytes = max(0, int(disk_safety_margin_bytes))
        self.max_retries = max_retries
        self.total_size = 0
        self.segments: List[Tuple[int, int]] = []
        self.current_reserved_bytes = 0
        
        # Batched counter updates to reduce lock contention
        self.local_bytes_accumulated = 0
        self.flush_threshold = 256 * 1024  # Flush every 256 KB — keeps metrics accurate during TCP slow-start
    
    def read_metadata(self) -> Optional[Dict]:
        """Read segment completion metadata from .meta file."""
        if not os.path.exists(self.meta_path):
            return None
        try:
            with open(self.meta_path, 'r') as f:
                return json.load(f)
        except:
            return None
    
    def write_metadata(
        self,
        file_size: int,
        segments: List[Tuple[int, int]],
        completed: Set[int],
        partial_offsets: Optional[Dict[int, int]] = None,
    ):
        """Write segment completion + partial resume metadata using atomic replace."""
        normalized_offsets: Dict[str, int] = {}
        if partial_offsets:
            for seg_idx, offset in partial_offsets.items():
                if seg_idx in completed:
                    continue
                if seg_idx < 0 or seg_idx >= len(segments):
                    continue
                seg_start, seg_end = segments[seg_idx]
                clamped = max(seg_start, min(int(offset), seg_end + 1))
                if clamped > seg_start:
                    normalized_offsets[str(seg_idx)] = clamped

        metadata = {
            'file_size': file_size,
            'segments': [[s, e] for s, e in segments],  # Store as lists for JSON compatibility
            'completed_indices': list(completed),
            'partial_offsets': normalized_offsets,
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

    def _load_partial_offsets(
        self,
        metadata: Dict,
        segments: List[Tuple[int, int]],
        completed: Set[int],
    ) -> Dict[int, int]:
        """Load and validate partial segment resume offsets from metadata."""
        raw_offsets = metadata.get('partial_offsets', {})
        if not isinstance(raw_offsets, dict):
            return {}

        offsets: Dict[int, int] = {}
        for key, value in raw_offsets.items():
            try:
                seg_idx = int(key)
                offset = int(value)
            except (TypeError, ValueError):
                continue

            if seg_idx in completed:
                continue
            if seg_idx < 0 or seg_idx >= len(segments):
                continue

            seg_start, seg_end = segments[seg_idx]
            clamped = max(seg_start, min(offset, seg_end + 1))
            if clamped >= seg_end + 1:
                completed.add(seg_idx)
                continue
            if clamped > seg_start:
                offsets[seg_idx] = clamped

        return offsets
        
    def flush_counter(self, force=False):
        """Flush accumulated bytes to shared counter."""
        if self.local_bytes_accumulated > 0 and (force or self.local_bytes_accumulated >= self.flush_threshold):
            if self.process_counter is not None:
                with self.process_counter.get_lock():
                    self.process_counter.value += self.local_bytes_accumulated
            self.local_bytes_accumulated = 0

    async def reserve_disk_space(self, required_bytes: int):
        """
        Reserve download bytes before starting a task so workers cannot overcommit
        disk capacity concurrently.
        """
        required_bytes = max(0, int(required_bytes))
        if required_bytes == 0 or self.disk_reserved_bytes is None:
            return

        if self.current_reserved_bytes > 0:
            return

        while True:
            pending_headroom = 0
            if self.min_pending_conversion_bytes is not None:
                with self.min_pending_conversion_bytes.get_lock():
                    pending_headroom = int(self.min_pending_conversion_bytes.value)

            needed_total = required_bytes + self.disk_safety_margin_bytes + pending_headroom
            with self.disk_reserved_bytes.get_lock():
                free_now = available_space_bytes(download_dir)
                effective_free = free_now - self.disk_reserved_bytes.value
                if effective_free >= needed_total:
                    self.disk_reserved_bytes.value += required_bytes
                    self.current_reserved_bytes = required_bytes
                    logging.debug(
                        f"[Download #{self.process_id}] Reserved {required_bytes} bytes "
                        f"(shared reserved={self.disk_reserved_bytes.value}, free={free_now}, "
                        f"pending_conversion_headroom={pending_headroom})"
                    )
                    return

            if transfer_done.value == 1:
                raise asyncio.CancelledError("Transfer stopping while waiting for disk space")

            await asyncio.sleep(0.5)

    def release_disk_space(self):
        """Release previously reserved download bytes."""
        if self.current_reserved_bytes <= 0 or self.disk_reserved_bytes is None:
            return

        with self.disk_reserved_bytes.get_lock():
            self.disk_reserved_bytes.value = max(
                0,
                self.disk_reserved_bytes.value - self.current_reserved_bytes
            )
        self.current_reserved_bytes = 0
    
    async def probe_range_support(self) -> Tuple[Optional[int], bool]:
        """
        Probe for Range support using a single Range GET request.
        A 206 response confirms range support and Content-Range gives the full
        file size, eliminating the separate HEAD round-trip.
        Returns (file_size, supports_ranges).
        """
        try:
            headers = {'Range': 'bytes=0-0'}
            async with self.session.get(self.url, headers=headers, allow_redirects=True) as resp:
                if resp.status == 206:
                    # Server supports ranges; extract total size from Content-Range
                    file_size = None
                    content_range = resp.headers.get('Content-Range', '')
                    # Format: bytes 0-0/1234
                    if '/' in content_range:
                        try:
                            file_size = int(content_range.split('/')[-1])
                        except Exception:
                            pass
                    return file_size, True
                elif resp.status == 200:
                    # Server returned the full file instead of a partial response.
                    # Use Content-Length for size, but ranges are not supported.
                    content_length = resp.headers.get('Content-Length')
                    file_size = int(content_length) if content_length else None
                    return file_size, False
                else:
                    return None, False
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
        fd: int,
        progress_offsets: Dict[int, int],
    ) -> Tuple[int, int]:
        """
        Download a single segment and write directly to file descriptor at correct offset.
        Returns (segment_id, bytes_written).
        Validates that returned range matches request and all bytes received.
        """
        chunk_size = 1024 * 1024  # 128KB chunks
        bytes_written = 0
        current_offset = start
        progress_offsets[segment_id] = start
        
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
                        free_now = available_space_bytes(download_dir)
                        while free_now <= (len(chunk) + chunk_size):
                            await asyncio.sleep(0.5)
                            free_now = available_space_bytes(download_dir)
                        
                        # Write directly to file at correct offset using pwrite
                        os.pwrite(fd, chunk, current_offset)
                        chunk_len = len(chunk)
                        current_offset += chunk_len
                        bytes_written += chunk_len
                        progress_offsets[segment_id] = current_offset
                        
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
                progress_offsets[segment_id] = end + 1
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
        # Fresh downloads do not need a separate preflight step. Probe range
        # support inside download_segmented() and begin scheduling segments
        # immediately after the first 206 response confirms support.
        if not os.path.exists(self.local_path) and not os.path.exists(self.part_path):
            return await self.download_segmented()

        # Fix #3: single range probe reused for both the already-complete check
        # and the download path, eliminating a redundant network round-trip.
        file_size, supports_ranges = await self.probe_range_support()

        # Check if final file already exists and is complete
        if os.path.exists(self.local_path):
            if file_size and os.path.getsize(self.local_path) == file_size:
                logging.info(f"[Download #{self.process_id}] Already complete: {os.path.basename(self.local_path)}")
                if self.process_counter is not None:
                    with self.process_counter.get_lock():
                        self.process_counter.value += file_size
                return True, False, 0

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
            remaining_bytes = max(0, file_size - existing_size)
            return await self.download_single_connection(
                supports_ranges=False,
                resume_from=existing_size,
                expected_remaining_bytes=remaining_bytes
            )
        
        # Use segmented download
        return await self.download_segmented(file_size)
    
    async def download_segmented(self, file_size: Optional[int] = None) -> Tuple[bool, bool, int]:
        """
        Download file in multiple segments, streaming directly to disk.
        Uses .meta file to track completed segments (not file size).
        Returns (success, was_paused, num_connections) tuple.
        """
        if file_size is None:
            try:
                headers = {'Range': 'bytes=0-0'}
                async with self.session.get(self.url, headers=headers, allow_redirects=True) as resp:
                    if resp.status == 206:
                        content_range = resp.headers.get('Content-Range', '')
                        if '/' in content_range:
                            try:
                                file_size = int(content_range.split('/')[-1])
                            except Exception:
                                file_size = None
                        if file_size is None:
                            raise Exception(f"Could not determine file size from Content-Range: {content_range}")
                    elif resp.status == 200:
                        logging.debug(f"Server doesn't support ranges for {self.url}, using single connection")
                        content_length = resp.headers.get('Content-Length')
                        file_size = int(content_length) if content_length else None
                        return await self.download_single_connection(
                            supports_ranges=False,
                            expected_remaining_bytes=file_size if file_size else 0
                        )
                    else:
                        raise Exception(f"Unexpected status {resp.status} during range probe")
            except Exception as e:
                logging.warning(f"Could not determine range support for {self.url}, attempting direct download: {e}")
                return await self.download_single_connection(supports_ranges=False)

        segments = self.calculate_segments(file_size)
        
        # Load metadata to see which segments are already complete
        metadata = self.read_metadata()
        completed_segments: Set[int] = set()
        partial_offsets: Dict[int, int] = {}
        
        if metadata:
            # Validate metadata matches current file/segments
            if (metadata.get('file_size') == file_size and 
                metadata.get('segments') == [[s, e] for s, e in segments]):  # Compare as lists
                completed_segments = set(metadata.get('completed_indices', []))
                partial_offsets = self._load_partial_offsets(metadata, segments, completed_segments)
                resumed_bytes = sum(
                    max(0, partial_offsets.get(i, start) - start)
                    for i, (start, end) in enumerate(segments)
                    if i not in completed_segments
                )
                logging.info(
                    f"[Download #{self.process_id}] Resume: {len(completed_segments)}/{len(segments)} "
                    f"segments already complete, partial credit {resumed_bytes} bytes "
                    f"for {os.path.basename(self.local_path)}"
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
                partial_offsets = {}
        
        # Determine remaining segments
        remaining_segments = [
            (i, partial_offsets.get(i, start), end)
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

        remaining_bytes = sum((end - start + 1) for _, start, end in remaining_segments)
        await self.reserve_disk_space(remaining_bytes)
        
        logging.info(
            f"[Download #{self.process_id}] Downloading {os.path.basename(self.local_path)} "
            f"in {len(remaining_segments)}/{len(segments)} segments ({file_size} bytes)"
        )
        
        os.makedirs(os.path.dirname(self.part_path), exist_ok=True)
        
        # Track actual connections we'll use
        num_connections = len(remaining_segments)
        progress_offsets: Dict[int, int] = {
            i: start for i, start, _ in remaining_segments
        }
        
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
                self.download_segment_streaming(i, start, end, fd, progress_offsets)
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

            latest_partial_offsets: Dict[int, int] = {}
            for seg_id, seg_start, seg_end in remaining_segments:
                if seg_id in completed_segments:
                    continue
                latest = progress_offsets.get(seg_id, seg_start)
                latest = max(seg_start, min(latest, seg_end + 1))
                if latest >= seg_end + 1:
                    completed_segments.add(seg_id)
                    continue
                if latest > seg_start:
                    latest_partial_offsets[seg_id] = latest
            
            # Check if paused and save progress before raising
            if paused:
                self.write_metadata(file_size, segments, completed_segments, latest_partial_offsets)
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
                self.write_metadata(file_size, segments, completed_segments, latest_partial_offsets)
                raise Exception(f"Not all segments completed: {len(completed_segments)}/{len(segments)}")
            
        except asyncio.CancelledError:
            # Save progress to metadata before exiting
            latest_partial_offsets: Dict[int, int] = {}
            for seg_id, seg_start, seg_end in remaining_segments:
                if seg_id in completed_segments:
                    continue
                latest = progress_offsets.get(seg_id, seg_start)
                latest = max(seg_start, min(latest, seg_end + 1))
                if latest > seg_start and latest < seg_end + 1:
                    latest_partial_offsets[seg_id] = latest
            self.write_metadata(file_size, segments, completed_segments, latest_partial_offsets)
            logging.info(f"[Download #{self.process_id}] Paused {os.path.basename(self.local_path)}")
            return False, True, num_connections  # Not successful, but was paused (not failed)
        except Exception as e:
            # Save whatever progress we have
            latest_partial_offsets: Dict[int, int] = {}
            for seg_id, seg_start, seg_end in remaining_segments:
                if seg_id in completed_segments:
                    continue
                latest = progress_offsets.get(seg_id, seg_start)
                latest = max(seg_start, min(latest, seg_end + 1))
                if latest > seg_start and latest < seg_end + 1:
                    latest_partial_offsets[seg_id] = latest
            self.write_metadata(file_size, segments, completed_segments, latest_partial_offsets)
            logging.error(f"[Download #{self.process_id}] Failed {os.path.basename(self.local_path)}: {e}")
            return False, False, num_connections  # Failed, not paused
        finally:
            if fd is not None:
                os.close(fd)
            self.release_disk_space()
            # Decrement active connections
            if self.active_connections is not None:
                with self.active_connections.get_lock():
                    self.active_connections.value -= num_connections
    
    async def download_single_connection(
        self, 
        supports_ranges: bool = True,
        resume_from: int = 0,
        expected_remaining_bytes: int = 0
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

        await self.reserve_disk_space(expected_remaining_bytes)
        
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
                                
                                free_now = available_space_bytes(download_dir)
                                while free_now <= (len(chunk) + chunk_size):
                                    await asyncio.sleep(0.5)
                                    free_now = available_space_bytes(download_dir)
                                
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
            self.release_disk_space()
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
    disk_reserved_bytes: mp.Value,
    min_pending_conversion_bytes: mp.Value,
    disk_safety_margin_bytes: int,
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
        headers={'User-Agent': 'seqflux/3.0'}
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
                    disk_reserved_bytes=disk_reserved_bytes,
                    min_pending_conversion_bytes=min_pending_conversion_bytes,
                    disk_safety_margin_bytes=disk_safety_margin_bytes,
                    max_retries=max_retries
                )
                
                success, was_paused, _ = await downloader.download_with_resume()
                
                if success:
                    # Fix #1: enqueue to processing_queue BEFORE incrementing
                    # download_complete.  The main thread exits its wait-loop as
                    # soon as download_complete reaches initial_task_count and
                    # immediately puts the None sentinel into processing_queue.
                    # If we increment first, the sentinel can overtake this item
                    # and the last .sra file is silently dropped.
                    processing_queue.put(local_path)   # ← hand off to conversion stage first
                    with download_complete.get_lock():
                        download_complete.value += 1
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
    disk_reserved_bytes: mp.Value,
    min_pending_conversion_bytes: mp.Value,
    disk_safety_margin_bytes: int,
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
        disk_reserved_bytes,
        min_pending_conversion_bytes,
        disk_safety_margin_bytes,
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
    fname = os.path.join(
        RUN_LOG_DIR,
        f'log_download_{datetime.datetime.fromtimestamp(t).strftime("%Y%m%d_%H%M%S")}.csv'
    )
    
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

    # Give the pre-activated workers one full probing window to download before
    # the optimizer's first probe overrides download_process_status.
    # Without this delay the optimizer fires at second 0 (before workers even
    # spawn, since URL fetching takes ~3 s) and immediately sets concurrency
    # back to 1, negating the pre-activation entirely.
    deadline = start.value + probing_time
    while time.time() < deadline and transfer_done.value == 0:
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
main_processing_queue = None
main_move_queue = None


def graceful_exit(signum=None, frame=None):
    """Signal handler for SIGINT/SIGTERM."""
    logging.info(f"Graceful exit triggered: signum={signum}")
    try:
        transfer_done.value = 1
        if main_processing_queue is not None:
            try:
                main_processing_queue.put_nowait(None)
            except Exception:
                pass
        if main_move_queue is not None:
            try:
                main_move_queue.put_nowait(None)
            except Exception:
                pass
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

    parser = argparse.ArgumentParser(
        description="SeqFlux resource-aware NCBI SRA acquisition pipeline"
    )
    parser.add_argument("-i", "--input", required=True,
                        help="Text file: one accession per line.")
    parser.add_argument("-o", "--outdir", "--out-dir", default="seqflux/output/",
                        help="Slow-tier destination for .fastq.gz files "
                             "(pigz writes directly here; no separate move stage).")
    parser.add_argument("--sra-dir", default=None,
                        help="Fast-tier directory where .sra downloads are written. "
                             "Defaults to a per-PID subdir under LOCAL_SCRATCH "
                             "(see config_seqflux.get_seqflux_tmpfs_dir).")
    parser.add_argument("--fastq-dir", default=None,
                        help="Fast-tier working directory for the SRA→FASTQ conversion stage. "
                             "The converter creates ./fastq and ./tmp subdirectories here. "
                             "Defaults to the same per-PID LOCAL_SCRATCH subdir used by --sra-dir.")
    parser.add_argument("--fastq", action="store_true",
                        help="Use fastq_ftp instead of sra_ftp")
    parser.add_argument("--segment-size", type=int, default=512,
                        help="Segment size in MB (default: 512)")
    parser.add_argument("--max-segments", type=int, default=8,
                        help="Max segments per file (default: 8)")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="Max retry attempts per task (default: 3)")
    args = parser.parse_args()

    accession_group = _accession_group_name(args.input)
    RUN_LOG_DIR = os.path.join("logs", "seqflux", accession_group)
    os.makedirs(RUN_LOG_DIR, exist_ok=True)

    # Configure logging
    log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
    log_file = os.path.join(
        RUN_LOG_DIR,
        f'seqflux.{datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}.log'
    )
    
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

    logging.info(f"Logging directory: {RUN_LOG_DIR}")

    # Set configuration parameters
    configurations["cpu_count"] = mp.cpu_count()
    if configurations.get("thread_limit", -1) == -1:
        configurations["thread_limit"] = configurations["cpu_count"]
    
    configurations["segment_size"] = args.segment_size * 1024 * 1024
    configurations["max_segments"] = args.max_segments
    configurations["max_retries"] = args.max_retries
    probing_time = configurations.get("probing_sec", 5)

    # Resolve fast-tier paths from CLI flags, falling back to the LOCAL_SCRATCH
    # derived per-PID directory used historically.
    default_tmpfs = get_seqflux_tmpfs_dir()
    sra_dir       = args.sra_dir if args.sra_dir else default_tmpfs
    fastq_work_dir = args.fastq_dir if args.fastq_dir else default_tmpfs
    download_dir   = sra_dir               # downloader writes .sra files here (fast tier)
    root_dir       = args.outdir           # slow-tier destination for .fastq.gz

    for d in (sra_dir, fastq_work_dir, root_dir):
        try:
            os.makedirs(d, exist_ok=True)
        except Exception as e:
            logging.error(f"Failed to create directory {d}: {e}")
            sys.exit(1)

    logging.info(
        f"Storage layout — sra-dir (fast): {sra_dir} | "
        f"fastq-dir (fast): {fastq_work_dir} | out-dir (slow): {root_dir}"
    )

    # Shared counters and structures
    t_start           = time.time()          # ← benchmark: pipeline start
    t_download_end    = mp.Value("d", 0.0)   # ← benchmark: set when last download completes
    download_complete = mp.Value("i", 0)
    failed_count = mp.Value("i", 0)  # Track failed downloads
    move_complete = mp.Value("i", 0)
    transfer_done = mp.Value("i", 0)
    active_connections = mp.Value("i", 0)  # Track actual connection count
    shared_disk_reserved_bytes = mp.Value("Q", 0)  # Shared across downloader + converter
    shared_min_pending_conversion_bytes = mp.Value("Q", 0)  # Smallest queued conversion headroom requirement
    download_disk_safety_margin_bytes = int(
        float(configurations.get("download_disk_safety_margin_gb", 2.0)) * 1024 * 1024 * 1024
    )

    # Use deque instead of Manager list for better performance
    throughput_logs = deque(maxlen=10000)
    throughput_lock = Lock()
    
    # Use JoinableQueue for tasks
    download_queue = mp.JoinableQueue()
    failed_queue = mp.Queue()  # Track failed downloads
    processing_queue = mp.Queue()
    move_queue = mp.Queue()

    main_processing_queue = processing_queue
    main_move_queue = move_queue

    # Read accessions and build download tasks
    with open(args.input) as f:
        accs = [l.strip() for l in f if l.strip()]
    
    field = "fastq_ftp" if args.fastq else "sra_ftp"
    task_count = 0

    # Fetch all accession URLs in parallel to avoid N×RTT sequential delay.
    from concurrent.futures import ThreadPoolExecutor, as_completed as futures_as_completed

    def _fetch(acc):
        try:
            return acc, get_ncbi_urls(acc, field)
        except Exception as e:
            logging.error(f"NCBI lookup failed for {acc}: {e}")
            return acc, []

    # Keep lookup fan-out conservative to avoid hitting NCBI E-utilities rate limits.
    # Worker count controls queueing only; request rate is enforced globally
    # by ncbi_rate_limiter via the configured ncbi_lookup_rps.
    lookup_workers = max(1, min(len(accs), int(configurations.get("ncbi_lookup_workers", 3))))
    with ThreadPoolExecutor(max_workers=lookup_workers) as _pool:
        _futures = {_pool.submit(_fetch, acc): acc for acc in accs}
        for _fut in futures_as_completed(_futures):
            _, url_acc_pairs = _fut.result()
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
    # Pre-activate the first 2 workers so downloads begin immediately without
    # waiting for the optimizer's first 5-second probing window.
    # The optimizer will adjust concurrency from this baseline.
    _initial_active = min(2, num_workers)
    download_process_status = mp.Array("i", [1 if i < _initial_active else 0 for i in range(num_workers)])
    
    # Per-process byte counters (no Manager overhead)
    process_counters = [mp.Value('Q', 0) for _ in range(num_workers)]

    # Start download workers
    download_workers = [
        mp.Process(
            target=download_file_worker, 
            args=(
                i,
                download_queue,
                failed_queue,
                failed_count,
                process_counters[i],
                active_connections,
                shared_disk_reserved_bytes,
                shared_min_pending_conversion_bytes,
                download_disk_safety_margin_bytes,
                processing_queue,
            )
        ) 
        for i in range(num_workers)
    ]
    for p in download_workers:
        p.daemon = True
        p.start()

    converter = SRAConverter(
        processing_queue=processing_queue,
        move_queue=move_queue,
        work_dir=fastq_work_dir,
        compressed_output_dir=root_dir,
        nvme_device=get_nvme_device(),
        threads_per_job=configurations.get("conversion_threads", 8),
        cpu_threshold=configurations.get("cpu_threshold", 85.0),
        nvme_threshold=configurations.get("nvme_threshold", 92.0),
        max_jobs=configurations.get("max_conversion_jobs", None),
        max_pigz_jobs=configurations.get("max_pigz_jobs", None),
        required_size_factor=configurations.get(
            "conversion_required_factor",
            configurations.get("conversion_output_factor", 12.0),
        ),
        reserve_size_factor=configurations.get("conversion_reserve_factor", 12.0),
        pigz_reserve_factor=configurations.get("conversion_pigz_reserve_factor", 3.5),
        disk_safety_margin_gb=configurations.get("conversion_disk_safety_margin_gb", 0.0),
        shared_reserved_bytes=shared_disk_reserved_bytes,
        shared_pending_headroom_bytes=shared_min_pending_conversion_bytes,
    )
    converter.start()

    configurations["thread_limit"] = configurations.get("max_cc", mp.cpu_count())

    # FFS-direct pipeline: pigz writes .fastq.gz straight to root_dir (slow
    # tier), so no movement is required and no FileMover or completion-drainer
    # threads are needed. The move_queue is still populated by the converter
    # (purely as a completion signal stream) and is drained inline after the
    # converter stops; see the drain loop near the benchmark-summary section.

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
    
    with t_download_end.get_lock():
        t_download_end.value = time.time()   # ← benchmark: all downloads finished

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

    # Drain the move_queue inline. After converter.stop() has returned, the
    # result-collector loop has already pushed every successful .gz path; no
    # new puts will arrive. We empty the queue with get_nowait so we never
    # block, count entries for the benchmark log, and move on.
    completed_compressed = 0
    while True:
        try:
            gz_path = move_queue.get_nowait()
        except queue.Empty:
            break
        if gz_path is None:
            continue
        completed_compressed += 1
    logging.info(
        f"FFS-direct: {completed_compressed} compressed file(s) emitted "
        f"directly to slow tier {root_dir} (no move stage)."
    )

    # ── Benchmark timing summary ─────────────────────────────────────────────
    t_end = time.time()

    # Phase windows (absolute epoch seconds)
    #   download:   pipeline start → last file fully downloaded
    #   conversion: first fasterq-dump launched → last fasterq-dump finished
    #   compression:first pigz launched        → last pigz finished
    # Phases overlap intentionally (seqflux pipelines them concurrently).
    _dl_start   = t_start
    _dl_end     = t_download_end.value
    _fq_start   = converter.t_first_fasterq_start   # 0.0 if no job ran
    _fq_end     = converter.t_last_fasterq_done
    _pz_start   = converter.t_first_pigz_start
    _pz_end     = converter.t_last_pigz_done

    def _overlap(s1, e1, s2, e2):
        """Overlap in seconds between two [start, end] intervals. 0 if either is unset."""
        if s1 == 0.0 or s2 == 0.0:
            return 0.0
        return max(0.0, min(e1, e2) - max(s1, s2))

    import json as _json
    timing_result = {
        "tool":       "seqflux",
        "accessions": accs,
        "phases": {
            "download": {
                "start":      _dl_start,
                "end":        _dl_end,
                "duration_s": round(_dl_end - _dl_start, 2),
            },
            "conversion": {
                "start":      _fq_start,
                "end":        _fq_end,
                "duration_s": round(max(0.0, _fq_end - _fq_start), 2),
            },
            "compression": {
                "start":      _pz_start,
                "end":        _pz_end,
                "duration_s": round(max(0.0, _pz_end - _pz_start), 2),
            },
        },
        "overlaps_s": {
            "download_x_conversion":  round(_overlap(_dl_start, _dl_end, _fq_start, _fq_end), 2),
            "download_x_compression": round(_overlap(_dl_start, _dl_end, _pz_start, _pz_end), 2),
            "conversion_x_compression": round(_overlap(_fq_start, _fq_end, _pz_start, _pz_end), 2),
        },
        "total_time_s": round(t_end - t_start, 2),
        # Legacy flat keys kept for backward compatibility with benchmark_compare.py
        "download_time_s":    round(_dl_end - _dl_start, 2),
        "conversion_time_s":  round(max(0.0, _fq_end - _fq_start), 2),
        "compression_time_s": round(max(0.0, _pz_end - _pz_start), 2),
    }

    ts_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timing_json = os.path.join(RUN_LOG_DIR, f"benchmark_seqflux_results_{ts_str}.json")
    with open(timing_json, "w") as _f:
        _json.dump(timing_result, _f, indent=2)

    ov = timing_result["overlaps_s"]
    ph = timing_result["phases"]
    logging.info(
        f"\n{'='*60}\n"
        f"  BENCHMARK SUMMARY (seqflux)\n"
        f"{'='*60}\n"
        f"  Phase windows (wall-clock, overlapping):\n"
        f"    Download    {ph['download']['start']:.3f} → {ph['download']['end']:.3f}"
        f"  ({ph['download']['duration_s']:.1f}s)\n"
        f"    Conversion  {ph['conversion']['start']:.3f} → {ph['conversion']['end']:.3f}"
        f"  ({ph['conversion']['duration_s']:.1f}s)\n"
        f"    Compression {ph['compression']['start']:.3f} → {ph['compression']['end']:.3f}"
        f"  ({ph['compression']['duration_s']:.1f}s)\n"
        f"  Overlaps:\n"
        f"    Download  ∩ Conversion:  {ov['download_x_conversion']:.1f}s\n"
        f"    Download  ∩ Compression: {ov['download_x_compression']:.1f}s\n"
        f"    Conversion ∩ Compression:{ov['conversion_x_compression']:.1f}s\n"
        f"  Total wall-clock:          {timing_result['total_time_s']:.1f}s\n"
        f"{'='*60}\n"
        f"  Results → {timing_json}\n"
        f"{'='*60}"
    )
    
    # Report failed downloads.
    # mp.Queue.empty() is not reliable across processes, so drain by expected
    # count with a bounded wait to avoid dropping late-arriving failures.
    failed_list = []
    expected_failed = max(0, failed_count.value)
    drain_deadline = time.time() + 10.0
    while len(failed_list) < expected_failed:
        remaining = drain_deadline - time.time()
        if remaining <= 0:
            break
        try:
            failed_list.append(failed_queue.get(timeout=min(1.0, remaining)))
        except queue.Empty:
            continue

    if len(failed_list) < expected_failed:
        logging.warning(
            f"Failed queue drain timed out: expected {expected_failed}, "
            f"collected {len(failed_list)}"
        )
    
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
