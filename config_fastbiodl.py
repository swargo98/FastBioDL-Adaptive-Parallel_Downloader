configurations = {
    "data_dir": "dest/",
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