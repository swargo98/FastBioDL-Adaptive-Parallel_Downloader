configurations = {
    "ftp": {
        "host": "192.168.1.1",
        "username": 'rms',
        "password": '112358',
        "port": 21,
        "remote_dir": "/"
    },
    "download_dir": "/mnt/nvme0n1/apd_downloads/",
    "method": "gradient", # options: [gradient, bayes]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1 #-1 for infinite
    },
    "thread_limit": 15,
    "K": 1.02,
    "probing_sec": 5, # probing interval in seconds
    "loglevel": "info",
}