import os
from typing import Optional

from storage_config import fastbiodl_tmpfs_dir, get_nvme_base, nvme_path


def get_fastbiodl_nvme_base() -> str:
    """Return the configured FastBioDL scratch base directory."""
    return str(configurations.get("nvme_base") or get_nvme_base())


def get_fastbiodl_work_dir() -> str:
    """Return the shared FastBioDL scratch work directory."""
    return os.path.join(get_fastbiodl_nvme_base(), "fastbiodl")


def get_fastbiodl_tmpfs_dir(pid: Optional[int] = None) -> str:
    """Return the per-process scratch directory used by fastbiodl_upgrade.py."""
    configured_base = get_fastbiodl_nvme_base()
    if configured_base == get_nvme_base():
        return fastbiodl_tmpfs_dir(pid=pid)
    return os.path.join(configured_base, f"fastbiodl_{os.getpid() if pid is None else pid}")


configurations = {
    "nvme_base": get_nvme_base(),
    "download_dir": nvme_path("fastbiodl_downloads"),
    "method": "gradient", # options: [gradient, bayes]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1 #-1 for infinite
    },
    "thread_limit": 15,
    "max_conversion_jobs": 1,
    "max_pigz_jobs": 3,
    "conversion_threads": 8,
    "conversion_required_factor": 10.0,
    "conversion_reserve_factor": 10.5,
    "conversion_pigz_reserve_factor": 3.5,
    # Legacy alias kept for backward compatibility with older call sites.
    "conversion_output_factor": 10.0,
    "conversion_disk_safety_margin_gb": 0.0,
    "download_disk_safety_margin_gb": 20.0,
    "K": 1.02,
    "probing_sec": 5, # probing interval in seconds
    "ncbi_lookup_rps": 2.0,
    "loglevel": "info",
}
