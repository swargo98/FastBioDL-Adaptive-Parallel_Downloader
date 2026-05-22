import os
from typing import Optional

from storage_config import seqflux_tmpfs_dir, get_nvme_base, nvme_path


def get_seqflux_nvme_base() -> str:
    """Return the configured SeqFlux scratch base directory."""
    return str(configurations.get("nvme_base") or get_nvme_base())


def get_seqflux_work_dir() -> str:
    """Return the shared SeqFlux scratch work directory."""
    return os.path.join(get_seqflux_nvme_base(), "seqflux")


def get_seqflux_tmpfs_dir(pid: Optional[int] = None) -> str:
    """Return the per-process scratch directory used by seqflux.py."""
    configured_base = get_seqflux_nvme_base()
    if configured_base == get_nvme_base():
        return seqflux_tmpfs_dir(pid=pid)
    return os.path.join(configured_base, f"seqflux_{os.getpid() if pid is None else pid}")


configurations = {
    "nvme_base": get_nvme_base(),
    "download_dir": nvme_path("seqflux_downloads"),
    "method": "gradient", # options: [gradient, bayes]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1 #-1 for infinite
    },
    "thread_limit": 15,
    "max_conversion_jobs": 1,
    "max_pigz_jobs": 1,
    "conversion_threads": 8,
    "conversion_required_factor": 12.0,
    "conversion_reserve_factor": 12.0,
    "conversion_pigz_reserve_factor": 3.5,
    # Legacy alias kept for backward compatibility with older call sites.
    "conversion_output_factor": 12.0,
    "conversion_disk_safety_margin_gb": 0.0,
    "download_disk_safety_margin_gb": 20.0,
    "K": 1.02,
    "probing_sec": 5, # probing interval in seconds
    "ncbi_lookup_rps": 2.0,
    "loglevel": "info",
}
