configurations = {
    "receiver": {
        "host": "127.0.0.1",
        "port": 50021
    },
    "data_dir": "/data/",
    "direct": False, # Direct I/O
    "checksum": False, # Checksum Verification
    "method": "probe", # options: [gradient, bayes, brute, probe]
    "bayes": {
        "initial_run": 3,
        "num_of_exp": -1 #-1 for infinite
    },
    "random": {
        "num_of_exp": 10
    },
    "B": 10, # severity of the packet loss punishment
    "K": 1.02, # cost of increasing concurrency
    "loglevel": "info",
    "probing_sec": 3, # probing interval in seconds
    "fixed_probing": {
        "bsize": 10,
        "thread": 4
    },
    "max_cc": 40,
}