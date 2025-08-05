configurations = {
    "ftp": {
        "host": "192.168.1.1",
        "username": 'rms',
        "password": '112358',
        "port": 21,
        "remote_dir": "/"
    },
    "data_dir": "dest/",
    "method": "gradient", # options: [gradient, bayes, random, brute, probe, cg, lbfgs]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1 #-1 for infinite
    },
    "thread_limit": 15,
    "K": 1.02,
    "probing_sec": 20, # probing interval in seconds
    "file_transfer": True,
    "io_limit": -1, # I/O limit (Mbps) per thread
    "loglevel": "info",
}