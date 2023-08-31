import os
import time
import numpy as np
import glob
import pathlib


class Utils:
    def __init__(self, configs, logger) -> None:
        self.configs = configs
        self.logger = logger


    def tcp_stats(self):
        start = time.time()
        sent, retm = 0, 0
        HOST, PORT = self.configs["receiver"]["host"], self.configs["receiver"]["port"]
        RCVR_ADDR = str(HOST) + ":" + str(PORT)

        try:
            data = os.popen("ss -ti").read().split("\n")
            for i in range(1,len(data)):
                if RCVR_ADDR in data[i-1]:
                    parse_data = data[i].split(" ")
                    for entry in parse_data:
                        if "data_segs_out" in entry:
                            sent += int(entry.split(":")[-1])

                        if "bytes_retrans" in entry:
                            pass

                        elif "retrans" in entry:
                            retm += int(entry.split("/")[-1])

        except Exception as e:
            print(e)

        end = time.time()
        self.logger.debug("Time taken to collect tcp stats: {0}ms".format(np.round((end-start)*1000)))
        return sent, retm


    def parse_files(self):
        root = self.configs["data_dir"]
        files = []

        if pathlib.Path(root).exists():
            root = root if root[-1] == "/" else root + "/"

            for fpath in glob.glob(root + "**", recursive=True):
                if os.path.isfile(fpath):
                    name = fpath.replace(root, "")
                    size = os.path.getsize(fpath)
                    files.append((size, name))

        return files #sorted(files)[::-1]

