#!/usr/bin/env python3
"""Shared local-scratch path helpers for cluster and local runs."""

import os
from typing import Optional


def get_nvme_base() -> str:
    """Return the local scratch base directory for this process."""
    return os.environ.get(
        "LOCAL_SCRATCH",
        os.path.join(
            "/scratch",
            os.environ.get("USER", "user"),
            f"job_{os.environ.get('SLURM_JOB_ID', 'local')}",
        ),
    )


def nvme_path(*parts: str) -> str:
    """Build a path rooted under the configured local scratch base."""
    return os.path.join(get_nvme_base(), *parts)


def get_nvme_device() -> str:
    """Return the diskstats device name used for local NVMe admission control."""
    return os.environ.get("EXPANSE_NVME_DEVICE", "nvme0n1")


def seqflux_tmpfs_dir(pid: Optional[int] = None) -> str:
    """Return the per-process SeqFlux scratch directory."""
    return os.path.join(get_nvme_base(), f"seqflux_{os.getpid() if pid is None else pid}")
