#!/usr/bin/env python3
"""Shared NCBI SRA URL lookup utilities.

This module centralizes efetch/runinfo parsing, fallback run_new XML parsing,
retry/backoff policy, and a process-local thread-safe rate limiter.
"""

from __future__ import annotations

import csv
import logging
import random
import threading
import time
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple

import requests

NCBI_EFETCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
NCBI_RUN_NEW = "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/run_new"


class NCBIRateLimiter:
    """Global, non-bursty request spacing limiter."""

    def __init__(self, max_rps: float = 2.0):
        self.max_rps = max(0.1, float(max_rps))
        self.min_interval = 1.0 / self.max_rps
        self.next_allowed_at = time.monotonic()
        self.lock = threading.Lock()

    def wait_if_needed(self) -> None:
        """Reserve a slot and wait until it is due."""
        with self.lock:
            now = time.monotonic()
            slot_time = max(now, self.next_allowed_at)
            self.next_allowed_at = slot_time + self.min_interval
        delay = slot_time - time.monotonic()
        if delay > 0:
            time.sleep(delay)


_RATE_LIMITERS: Dict[float, NCBIRateLimiter] = {}
_RATE_LIMITERS_LOCK = threading.Lock()


def _get_rate_limiter(max_rps: float) -> NCBIRateLimiter:
    key = round(float(max_rps), 6)
    with _RATE_LIMITERS_LOCK:
        limiter = _RATE_LIMITERS.get(key)
        if limiter is None:
            limiter = NCBIRateLimiter(max_rps=key)
            _RATE_LIMITERS[key] = limiter
        return limiter


def _parse_runinfo_csv(text: str, field: str, acc: str) -> List[Tuple[str, str]]:
    lines = [line for line in text.strip().splitlines() if line.strip()]
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
        if idx >= len(row):
            continue
        for url in row[idx].split(";"):
            if not url:
                continue
            if "://" not in url:
                url = "https://" + url
            url_acc_pairs.append((url, acc))
    return url_acc_pairs


def _parse_run_new_xml(text: str, field: str, acc: str) -> List[Tuple[str, str]]:
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []

    urls: List[str] = []
    preferred_semantic = "SRA Lite" if field == "sra_ftp" else "fastq"

    for sra_file in root.findall(".//SRAFile"):
        semantic_name = (sra_file.get("semantic_name") or "").strip()
        url_attr = (sra_file.get("url") or "").strip()

        if semantic_name.lower() == preferred_semantic.lower() and url_attr.startswith(("http://", "https://")):
            urls.append(url_attr)

        for alt in sra_file.findall("Alternatives"):
            alt_url = (alt.get("url") or "").strip()
            if alt_url.startswith(("http://", "https://")) and semantic_name.lower() == preferred_semantic.lower():
                urls.append(alt_url)

    if not urls and field == "sra_ftp":
        # Last-resort mode: keep pipeline alive with any HTTPS SRA URL.
        for sra_file in root.findall(".//SRAFile"):
            candidate = (sra_file.get("url") or "").strip()
            if candidate.startswith(("http://", "https://")):
                urls.append(candidate)
            for alt in sra_file.findall("Alternatives"):
                alt_url = (alt.get("url") or "").strip()
                if alt_url.startswith(("http://", "https://")):
                    urls.append(alt_url)

    seen = set()
    deduped: List[str] = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    return [(url, acc) for url in deduped]


def get_ncbi_urls(
    acc: str,
    field: str = "sra_ftp",
    *,
    max_attempts: int = 5,
    backoff_base: float = 1.0,
    timeout: int = 30,
    max_rps: float = 2.0,
    user_agent: str = "seqflux/3.0 (+https://github.com/)",
    tool_name: str = "seqflux",
    email: str = "",
    api_key: str = "",
    logger: Optional[logging.Logger] = None,
) -> List[Tuple[str, str]]:
    """Resolve download URLs for one accession using efetch + fallback endpoint."""
    log = logger or logging.getLogger(__name__)
    limiter = _get_rate_limiter(max_rps=max_rps)

    headers = {"User-Agent": user_agent}
    efetch_params = {
        "db": "sra",
        "id": acc,
        "rettype": "runinfo",
        "retmode": "text",
        "tool": tool_name,
    }
    if email:
        efetch_params["email"] = email
    if api_key:
        efetch_params["api_key"] = api_key

    run_new_params = {"acc": acc}

    for attempt in range(1, int(max_attempts) + 1):
        try:
            limiter.wait_if_needed()
            log.info(f"Fetching URLs for {acc} from NCBI SRA using field '{field}'")
            resp = requests.get(
                NCBI_EFETCH,
                params=efetch_params,
                headers=headers,
                timeout=timeout,
            )
            if resp.status_code == 200:
                parsed = _parse_runinfo_csv(resp.text, field=field, acc=acc)
                if parsed:
                    return parsed
                log.warning(f"NCBI runinfo returned no usable URLs for {acc}")
            elif resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"Retryable HTTP status {resp.status_code}")
            else:
                log.error(f"NCBI runinfo non-retryable status for {acc}: {resp.status_code}")
                break
        except Exception as exc:
            if attempt == int(max_attempts):
                log.error(f"NCBI runinfo failed for {acc} after {max_attempts} attempts: {exc}")
            else:
                sleep_s = float(backoff_base) * (2 ** (attempt - 1)) + random.uniform(0.0, 0.5)
                log.warning(
                    f"NCBI runinfo attempt {attempt}/{max_attempts} failed for {acc}: {exc}; "
                    f"retrying in {sleep_s:.2f}s"
                )
                time.sleep(sleep_s)

    try:
        limiter.wait_if_needed()
        if api_key:
            run_new_params["api_key"] = api_key
        resp2 = requests.get(
            NCBI_RUN_NEW,
            params=run_new_params,
            headers=headers,
            timeout=timeout,
        )
        resp2.raise_for_status()
        fallback_urls = _parse_run_new_xml(resp2.text, field=field, acc=acc)
        if fallback_urls:
            log.info(f"Fallback lookup succeeded for {acc} via run_new")
            return fallback_urls
        log.error(f"Fallback lookup returned no usable URLs for {acc}")
    except Exception as exc:
        log.error(f"Fallback lookup failed for {acc}: {exc}")

    return []
