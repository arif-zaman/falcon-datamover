## Only supports Concurrency optimization

import os
import time
import socket
import warnings
import numpy as np
import pprint
import argparse
import pathlib
import multiprocessing as mp
from falcon.configs import configurations
from falcon.logs import logger
from falcon.search import Optimizer
from falcon.utils import Utils

warnings.filterwarnings("ignore", category=FutureWarning)


def send_file(process_id, q):
    while file_incomplete.value > 0:
        if process_status[process_id] == 0:
            pass
        else:
            while concurrency.value < 1:
                pass

            logger.debug("Start Process :: {0}".format(process_id))
            try:
                sock = socket.socket()
                sock.settimeout(3)
                sock.connect((HOST, PORT))

                while (not q.empty()) and (process_status[process_id] == 1):
                    try:
                        file_id = q.get()

                    except:
                        process_status[process_id] = 0
                        break

                    offset = file_offsets[file_id]
                    to_send = file_sizes[file_id] - offset

                    if (to_send > 0) and (process_status[process_id] == 1):
                        filename = root + file_names[file_id]
                        file = open(filename, "rb")
                        msg = file_names[file_id] + "," + str(int(offset))
                        msg += "," + str(int(to_send)) + "\n"
                        sock.send(msg.encode())

                        logger.debug("starting {0}, {1}, {2}".format(process_id, file_id, filename))

                        while (to_send > 0) and (process_status[process_id] == 1):
                            block_size = min(chunk_size, to_send)
                            sent = sock.sendfile(file=file, offset=int(offset), count=int(block_size))
                            offset += sent
                            to_send -= sent
                            file_offsets[file_id] = offset

                    if to_send > 0:
                        q.put(file_id)
                    else:
                        file_incomplete.value = file_incomplete.value - 1

                sock.close()

            except socket.timeout as e:
                pass

            except Exception as e:
                process_status[process_id] = 0
                logger.debug("Process: {0}, Error: {1}".format(process_id, str(e)))

            logger.debug("End Process :: {0}".format(process_id))

    process_status[process_id] = 0


def rcv_file(sock, process_id):
    while True:
        try:
            client, address = sock.accept()
            logger.info("{u} connected".format(u=address))
            process_status[process_id] = 1
            total = 0
            d = client.recv(1).decode()
            while d:
                header = ""
                while d != '\n':
                    header += str(d)
                    d = client.recv(1).decode()

                file_stats = header.split(",")
                filename, offset, to_rcv = str(file_stats[0]), int(file_stats[1]), int(file_stats[2])

                if "/" in filename:
                    curr_dir = "/".join(filename.split("/")[:-1])
                    pathlib.Path(root + curr_dir).mkdir(parents=True, exist_ok=True)

                fd = os.open(root+filename, os.O_CREAT | os.O_RDWR)
                os.lseek(fd, offset, os.SEEK_SET)
                logger.debug("Receiving file: {0}".format(filename))
                chunk = client.recv(chunk_size)

                while chunk:
                    logger.debug("Chunk Size: {0}".format(len(chunk)))
                    os.write(fd, chunk)
                    to_rcv -= len(chunk)
                    total += len(chunk)

                    if to_rcv > 0:
                        chunk = client.recv(min(chunk_size, to_rcv))
                    else:
                        logger.debug("Successfully received file: {0}".format(filename))
                        chunk = None
                        break

                os.close(fd)
                d = client.recv(1).decode()

            total = np.round(total/(1024*1024))
            logger.info("{u} exited. total received {d} MB".format(u=address, d=total))
            client.close()
            process_status[process_id] = 0
        except Exception as e:
            logger.error(str(e))


def sample_transfer(params):
    global throughput_logs, exit_signal

    if file_incomplete.value == 0:
        return exit_signal

    params = [1 if x<1 else int(np.round(x)) for x in params]
    logger.info("Sample Transfer -- Probing Parameters: {0}".format(params))
    concurrency.value = params[0]

    current_cc = np.sum(process_status)
    for i in range(configurations["thread_limit"]):
        if i < params[0]:
            if (i >= current_cc):
                process_status[i] = 1
        else:
            process_status[i] = 0

    logger.debug("Active CC: {0}".format(np.sum(process_status)))

    time.sleep(1)
    prev_sc, prev_rc = utility.tcp_stats()
    n_time = time.time() + probing_time - 1.1
    # time.sleep(n_time)
    while (time.time() < n_time) and (file_incomplete.value > 0):
        time.sleep(0.1)

    curr_sc, curr_rc = utility.tcp_stats()
    sc, rc = curr_sc - prev_sc, curr_rc - prev_rc

    logger.debug("TCP Segments >> Send Count: {0}, Retrans Count: {1}".format(sc, rc))
    thrpt = np.mean(throughput_logs[-2:]) if len(throughput_logs) > 2 else 0

    lr, B, K = 0, int(configurations["B"]), float(configurations["K"])
    if sc != 0:
        lr = rc/sc if sc>rc else 0

    # score = thrpt
    plr_impact = B*lr
    # cc_impact_lin = (K-1) * concurrency.value
    # score = thrpt * (1- plr_impact - cc_impact_lin)
    cc_impact_nl = K**concurrency.value
    score = (thrpt/cc_impact_nl) - (thrpt * plr_impact)
    score_value = np.round(score * (-1))

    logger.info("Sample Transfer -- Throughput: {0}Mbps, Loss Rate: {1}%, Score: {2}".format(
        np.round(thrpt), np.round(lr*100, 2), score_value))

    if file_incomplete.value == 0:
        return exit_signal
    else:
        return score_value


def normal_transfer(params):
    concurrency.value = max(1, int(np.round(params[0])))
    logger.info("Normal Transfer -- Probing Parameters: {0}".format([concurrency.value]))

    for i in range(concurrency.value):
        process_status[i] = 1

    while (np.sum(process_status) > 0) and (file_incomplete.value > 0):
        pass


def run_transfer():
    optimizer = Optimizer(
        configurations=configurations,
        black_box_function=sample_transfer,
        logger=logger,
        verbose=True
        )
    params = [2]

    if configurations["method"].lower() == "brute":
        logger.info("Running Brute Force Optimization .... ")
        params = optimizer.brute_force()

    elif configurations["method"].lower() == "hill_climb":
        logger.info("Running Hill Climb Optimization .... ")
        params = optimizer.hill_climb()

    elif configurations["method"].lower() == "gradient":
        logger.info("Running Gradient Optimization .... ")
        params = optimizer.gradient_opt_fast()

    elif configurations["method"].lower() == "probe":
        logger.info("Running a fixed configurations Probing .... ")
        params = [configurations["fixed_probing"]["thread"]]

    else:
        logger.info("Running Bayesian Optimization .... ")
        params = optimizer.bayes_opt()

    if file_incomplete.value > 0:
        normal_transfer(params)


def report_throughput(start_time):
    global throughput_logs
    previous_total = 0
    previous_time = 0

    while file_incomplete.value > 0:
        t1 = time.time()
        time_since_begining = np.round(t1-start_time, 1)

        if time_since_begining >= 0.1:
            if time_since_begining >= 3 and sum(throughput_logs[-3:]) == 0:
                file_incomplete.value = 0

            total_bytes = np.sum(file_offsets)
            thrpt = np.round((total_bytes*8)/(time_since_begining*1000*1000), 2)

            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total*8)/(curr_time_sec*1000*1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            throughput_logs.append(curr_thrpt)
            m_avg = np.round(np.mean(throughput_logs[-60:]), 2)

            logger.info("Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps, 60Sec_Average: {3}Mbps".format(
                time_since_begining, curr_thrpt, thrpt, m_avg))

            t2 = time.time()
            time.sleep(max(0, 1 - (t2-t1)))


def main():
    global root, exit_signal, chunk_size, HOST, PORT, utility
    global probing_time, throughput_logs, concurrency, process_status
    global file_names, file_sizes, file_offsets, file_incomplete

    pp = pprint.PrettyPrinter(indent=4)
    parser=argparse.ArgumentParser()
    parser.add_argument("agent", help="Please choose agent type: sender or receiver")
    parser.add_argument("--host", help="Receiver host address; default: 127.0.0.1")
    parser.add_argument("--port", help="Receiver port number; default: 50021")
    parser.add_argument("--data_dir", help="data directory of sender or receiver")
    parser.add_argument("--method", help="choose one of them : gradient, bayes, brute, probe")
    args = vars(parser.parse_args())
    # pp.pprint(f"Command line arguments: {args}")
    sender = False
    configurations["thread_limit"] = min(max(1,configurations["max_cc"]), mp.cpu_count())

    if args["agent"].lower() == "sender":
        sender = True

    if args["host"]:
        configurations["receiver"]["host"] = args["host"]

    if args["port"]:
        configurations["receiver"]["port"] = int(args["port"])

    if args["data_dir"]:
        configurations["data_dir"] = args["data_dir"]

    if args["method"]:
        configurations["method"] = args["method"]

    pp.pprint(configurations)

    manager = mp.Manager()
    root = configurations["data_dir"]
    root = root if root[-1] == "/" else root + "/"
    exit_signal = 10 ** 10
    chunk_size = 1 * 1024 * 1024
    HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
    utility = Utils(configurations, logger)

    if sender:
        probing_time = configurations["probing_sec"]
        # file_names = os.listdir(root)
        file_names = utility.parse_files()
        file_sizes = [os.path.getsize(root+filename) for filename in file_names]
        file_count = len(file_names)
        throughput_logs = manager.list()
        concurrency = mp.Value("i", 0)
        file_incomplete = mp.Value("i", file_count)
        process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])
        file_offsets = mp.Array("d", [0.0 for i in range(file_count)])

        q = manager.Queue(maxsize=file_count)
        for i in range(file_count):
            q.put(i)

        workers = [mp.Process(target=send_file, args=(i, q)) for i in range(configurations["thread_limit"])]
        for p in workers:
            p.daemon = True
            p.start()

        start = time.time()
        reporting_process = mp.Process(target=report_throughput, args=(start,))
        reporting_process.daemon = True
        reporting_process.start()
        run_transfer()
        end = time.time()

        time_since_begining = np.round(end-start, 3)
        total = np.round(np.sum(file_offsets) / (1024*1024*1024), 3)
        thrpt = np.round((total*8*1024)/time_since_begining,2)
        logger.info("Total: {0} GB, Time: {1} sec, Throughput: {2} Mbps".format(
            total, time_since_begining, thrpt))

        reporting_process.terminate()
        for p in workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.1)
    else:
        sock = socket.socket()
        sock.bind((HOST, PORT))
        sock.listen(configurations["thread_limit"])

        process_status = mp.Array("i", [0 for _ in range(configurations["thread_limit"])])
        workers = [mp.Process(target=rcv_file, args=(sock, i,)) for i in range(configurations["thread_limit"])]
        for p in workers:
            p.daemon = True
            p.start()

        process_status[0] = 1
        while sum(process_status) > 0:
            time.sleep(0.1)

        for p in workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.1)
