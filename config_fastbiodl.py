configurations = {
    "download_dir": "/mnt/nvme0n1/fastbiodl_downloads/",
    "method": "gradient", # options: [gradient, bayes]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1 #-1 for infinite
    },
    "thread_limit": 15,
    "max_conversion_jobs": 8,
    "conversion_threads": 4,
    "conversion_output_factor": 10.0,
    "conversion_disk_safety_margin_gb": 1.0,
    "K": 1.02,
    "probing_sec": 5, # probing interval in seconds
    "ncbi_lookup_rps": 2.0,
    "loglevel": "info",
}