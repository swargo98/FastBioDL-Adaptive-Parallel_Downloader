configurations = {
    "download_dir": "/mnt/raid0/fastbiodl_downloads/",
    "method": "gradient", # options: [gradient, bayes]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1 #-1 for infinite
    },
    "thread_limit": 15,
    "max_conversion_jobs": 1,
    "max_pigz_jobs": 1,
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