## Only supports Concurrency optimization

import os
import re
import time
import socket
import warnings
import numpy as np
import mmap
import pprint
import argparse
import pathlib
import hashlib
import subprocess
from redis import Redis
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from falcon.configs import configurations
from falcon.logs import logger
from falcon.search import Optimizer
from falcon.utils import Utils

warnings.filterwarnings("ignore", category=FutureWarning)
mp.log_to_stderr(logger.CRITICAL)
redis_host = os.environ.get("REDIS_HOSTNAME", "134.197.113.70")
redis_port = os.environ.get("REDIS_PORT", 6379)
redis_key = "falcon-write"
r_conn = Redis(redis_host, redis_port, retry_on_timeout=True)


def send_file(process_id, qsmall, qlarge):
    while file_incomplete.value > 0:
        if process_status[process_id] == 0:
            pass
        else:
            while concurrency.value < 1:
                pass

            logger.debug("Start Process :: {0}".format(process_id))
            try:
                if ":" in HOST:
                    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                else:
                    sock = socket.socket()

                sock.settimeout(3)
                sock.connect((HOST, PORT))

                while (not qsmall.empty() or not qlarge.empty()) and (process_status[process_id] == 1):
                    max_lcc = concurrency.value * 0.3
                    if process_id < max_lcc:
                        try:
                            if not qlarge.empty():
                                file_id = qlarge.get()
                            else:
                                if not qsmall.empty():
                                    file_id = qsmall.get()
                        except:
                            process_status[process_id] = 0
                            break
                    else:
                        try:
                            if not qsmall.empty():
                                file_id = qsmall.get()
                            else:
                                if not qlarge.empty():
                                    file_id = qlarge.get()
                        except:
                            process_status[process_id] = 0
                            break


                    offset = file_offsets[file_id]
                    fsize, fname = file_info[file_id]
                    to_send = fsize - offset

                    if process_status[process_id] == 1:
                        filepath = root + fname
                        with open(filepath, "rb") as file:
                            if configurations["checksum"] and fname in hash_values:
                                metadata = f"{fname},{hash_values[fname]},{int(offset)},{int(to_send)}\n"
                            else:
                                metadata = f"{fname},0,{int(offset)},{int(to_send)}\n"

                            sock.send(metadata.encode())
                            logger.debug("starting {0}, {1}, {2}".format(process_id, file_id, filepath))

                            while (to_send > 0) and (process_status[process_id] == 1):
                                block_size = int(min(chunk_size, to_send))
                                if root == "/dev/zero":
                                    sent = sock.send(file.read(block_size))
                                else:
                                    sent = sock.sendfile(file=file, offset=int(offset), count=block_size)

                                offset += sent
                                to_send -= sent
                                file_offsets[file_id] = offset

                    if to_send > 0:
                        if fsize < 1024 * 1024:
                            qsmall.put(file_id)
                        else:
                            qlarge.put(file_id)
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
            logger.debug("{u} connected".format(u=address))
            process_status[process_id] = 1
            total = 0
            d = client.recv(1).decode()
            while d:
                header = ""
                while d != '\n':
                    header += str(d)
                    d = client.recv(1).decode()

                filename, file_hash, offset, to_rcv = header.split(",")
                offset = int(offset)
                to_rcv = int(to_rcv)

                if file_hash != "0":
                    hash_values[filename] = file_hash

                if "/" in filename:
                    curr_dir = "/".join(filename.split("/")[:-1])
                    pathlib.Path(root + curr_dir).mkdir(parents=True, exist_ok=True)

                if configurations["direct"]:
                    logger.info("Direct I/O - {}".format(filename))
                    fd = os.open(root+filename, os.O_CREAT | os.O_RDWR | os.O_DIRECT | os.O_SYNC)
                    mm = mmap.mmap(-1, to_rcv)
                else:
                    fd = os.open(root+filename, os.O_CREAT | os.O_RDWR)

                os.lseek(fd, offset, os.SEEK_SET)
                logger.debug("Receiving file: {0}".format(filename))

                while to_rcv > 0:
                    chunk = client.recv(min(chunk_size, to_rcv))
                    if configurations["direct"]:
                        mm.write(chunk)
                    else:
                        os.write(fd, chunk)

                    to_rcv -= len(chunk)
                    total += len(chunk)

                if configurations["direct"]:
                    os.write(fd, mm)
                    mm.close()

                os.close(fd)
                if to_rcv == 0:
                    logger.debug("Successfully received file: {0}".format(filename))

                d = client.recv(1).decode()

            total = np.round(total/(1024*1024))
            logger.debug("{u} exited. total received {d} MB".format(u=address, d=total))
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


def whitebox(start_cc):
    global throughput_logs, network_logs
    rlimit, wlimit, nlimit = 56000, 56000,40000
    max_target = min(rlimit, wlimit, nlimit)
    max_thrpt_cc = 0
    concurrency.value = start_cc
    track = []

    while True:
        logger.info("Choosen Concurrency: {0}".format([concurrency.value]))
        for i in range(concurrency.value):
            process_status[i] = 1

        start = time.time()
        while time.time()-start<probing_time*0.9 and (np.sum(process_status) > 0) and (file_incomplete.value > 0):
            pass

        if (np.sum(process_status) > 0) and (file_incomplete.value > 0):
            avg_thrpt = round(np.mean(throughput_logs[-2:]))
            track.append(np.mean(network_logs[-2:], axis=0))

            if len(track)>2:
                rspeed, wspeed, nspeed = np.mean(track[-2:], axis=0)
                curr_target = min(rlimit-rspeed, wlimit-wspeed, nlimit-nspeed) + avg_thrpt
                curr_target = round(max(0.3 * max_target, curr_target))
                max_thrpt_cc = max(max_thrpt_cc, avg_thrpt//concurrency.value)
                concurrency.value = int(round(curr_target/max_thrpt_cc))

            continue
        break

    return [concurrency.value]


def normal_transfer(params):
    concurrency.value = max(1, int(np.round(params[0])))
    logger.info("Fixed Concurrency: {0}".format([concurrency.value]))

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
        params = [configurations["thread_limit"]] #[configurations["fixed_probing"]["thread"]]

    elif configurations["method"].lower() == "whitebox":
        logger.info("Running Whitebox Optimization .... ")
        params = whitebox(2)

    else:
        logger.info("Running Bayesian Optimization .... ")
        params = optimizer.bayes_opt()

    if file_incomplete.value > 0:
        normal_transfer(params)


def monitor(disk):
    global network_logs, sender
    command = f"dstat --disk --mem --net --swap --disk-util -D {disk} --bits"
    dstat = subprocess.Popen(command.split(), stdout=subprocess.PIPE, text=True)

    while ((sender and file_incomplete.value > 0) or (not sender)) and (line := dstat.stdout.readline()) != "":
        values = re.sub(r'\x1b\[[\d;]+m', '', line).replace("\n","").split("|")
        if not values[0].strip()[0].isdigit():
            continue

        data = {}
        # Disk
        temp = values[0].split()
        data["disk"] = {}
        data["disk"]["read"] = temp[0]
        data["disk"]["write"] = temp[1]

        # Memory
        temp = values[1].split()
        data["memory"] = {}
        data["memory"]["used"] = temp[0]
        data["memory"]["free"] = temp[1]
        data["memory"]["buffer"] = temp[2]
        data["memory"]["cache"] = temp[3]

        # Network
        temp = values[2].split()
        data["network"] = {}
        data["network"]["receive"] = temp[0]
        data["network"]["send"] = temp[1]

        # Swap Memory
        temp = values[3].split()
        data["swap"] = {}
        data["swap"]["used"] = temp[0]
        data["swap"]["free"] = temp[1]

        # Utilization (%)
        data["util"] = float(values[-1])
        mbits = {"g": 10**3, "m": 10**0, "k": 10**(-3), "b": 10**(-6)}
        for key in data:
            if isinstance(data[key], dict):
                for metric in data[key]:
                    if data[key][metric][-1].lower() in ("g","m","k","b"):
                       data[key][metric] = float(data[key][metric][:-1]) * mbits[data[key][metric][-1].lower()]
                       data[key][metric] = round(data[key][metric], 3)
                    else:
                        data[key][metric] = 0

        if sender:
            net_speed = data["network"]["receive"] + data["network"]["send"]
            write_speed = 0
            try:
                write_speed = float(r_conn.get(redis_key))
            except:
                pass

            network_logs.append([data["disk"]["read"], write_speed, net_speed])
        else:
            logger.info(f'Write Speed: {data["disk"]["write"]} Mbps')
            r_conn.set(redis_key, float(data["disk"]["write"]), 3)
        logger.debug(data)

    dstat.kill()


def report_throughput(start_time):
    global throughput_logs, network_logs
    previous_total = 0
    previous_time = 0

    while file_incomplete.value > 0:
        t1 = time.time()
        time_since_begining = np.round(t1-start_time, 1)

        if time_since_begining >= 0.1:
            if time_since_begining >= 10 and sum(throughput_logs[-10:]) == 0:
                file_incomplete.value = 0

            total_bytes = np.sum(file_offsets)
            thrpt = np.round((total_bytes*8)/(time_since_begining*1000*1000), 2)

            curr_total = total_bytes - previous_total
            curr_time_sec = np.round(time_since_begining - previous_time, 3)
            curr_thrpt = np.round((curr_total*8)/(curr_time_sec*1000*1000), 2)
            previous_time, previous_total = time_since_begining, total_bytes
            throughput_logs.append(curr_thrpt)
            # m_avg = np.round(np.mean(throughput_logs[-60:]), 2)
            btraffic = max(0, int(network_logs[-1][-1]) - int(curr_thrpt) if network_logs else 0)

            logger.info("Throughput @{0}s: Current: {1}Mbps, Average: {2}Mbps, Background Traffic: {3}Mbps".format(
                time_since_begining, curr_thrpt, thrpt, btraffic))

            t2 = time.time()
            time.sleep(max(0, 1 - (t2-t1)))


def get_hash(fname):
    file_path = root + fname
    logger.debug(file_path)
    start = time.time()
    md5 = hashlib.md5()

    with open(file_path, 'rb') as ff:
        data = ff.read(chunk_size)
        while data:
            md5.update(data)
            data = ff.read(chunk_size)

    hash_value = md5.hexdigest()
    end = time.time()

    logger.info(f"file: {file_path}, hash={hash_value}, time={round(end-start, 1)} sec")
    return (fname, hash_value)


def get_checksum(files):
    logger.info("Running checksum calculation ....")
    start = time.time()

    with ProcessPoolExecutor(max_workers=configurations["thread_limit"]) as executor:
        futures = []
        for _, file in files:
            futures.append(executor.submit(get_hash, file))

        hash_values = {}
        for future in futures:
            key, value = future.result()
            hash_values[key] = value

    logger.info(f"Total checksum calculation time: {round(time.time() - start, 1)} sec")
    return hash_values


def main():
    global root, exit_signal, chunk_size, HOST, PORT, utility, hash_values, sender
    global probing_time, throughput_logs, network_logs, concurrency, process_status
    global file_info, file_offsets, file_incomplete

    pp = pprint.PrettyPrinter(indent=4)
    parser=argparse.ArgumentParser()
    parser.add_argument("agent", help="Please choose agent type: sender or receiver")
    parser.add_argument("--host", help="Receiver host address; default: 127.0.0.1")
    parser.add_argument("--port", help="Receiver port number; default: 50021")
    parser.add_argument("--data_dir", help="data directory of sender or receiver")
    parser.add_argument("--method", help="choose one of them : gradient, probe")
    parser.add_argument("--max_cc", help="maximum concurrency")
    parser.add_argument("--direct", help="enable direct I/O")
    parser.add_argument("--checksum", help="enable checksum verification")
    args = vars(parser.parse_args())
    # pp.pprint(f"Command line arguments: {args}")
    sender = False
    configurations["thread_limit"] = mp.cpu_count() #min(max(1,configurations["max_cc"]), )

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

    if args["max_cc"]:
        configurations["thread_limit"] = int(args["max_cc"])

    if args["direct"]:
        configurations["direct"] = True

    if args["checksum"]:
        configurations["checksum"] = True

    pp.pprint(configurations)

    manager = mp.Manager()
    root = configurations["data_dir"]
    if root != "/dev/zero" and root != "/dev/null":
        root = root if root[-1] == "/" else root + "/"

    exit_signal = 10 ** 10
    chunk_size = 1 * 1024 * 1024
    HOST, PORT = configurations["receiver"]["host"], configurations["receiver"]["port"]
    utility = Utils(configurations, logger)
    hash_values = manager.dict()

    if sender:
        probing_time = configurations["probing_sec"]
        if root == "/dev/zero":
            configurations["direct"] = False
            configurations["checksum"] = False
            file_info = []
            for i in range(configurations["thread_limit"]):
                file_info.append((10**11, str()))

        else:
            file_info = utility.parse_files()

            if configurations["checksum"]:
                hash_values.update(get_checksum(file_info))

            logger.debug(hash_values)

        file_count = len(file_info)
        qsmall, qlarge = manager.Queue(), manager.Queue()
        for i in range(file_count):
            if file_info[i][0] < 1024 * 1024:
                qsmall.put(i)
            else:
                qlarge.put(i)

        logger.info(f"Small files: {qsmall.qsize()}, Large files: {qlarge.qsize()}")
        file_offsets = mp.Array("d", [0.0 for i in range(file_count)])
        throughput_logs = manager.list()
        network_logs = manager.list()
        concurrency = mp.Value("i", 0)
        file_incomplete = mp.Value("i", file_count)
        process_status = mp.Array("i", [0 for i in range(configurations["thread_limit"])])

        workers = [mp.Process(target=send_file, args=(i, qsmall, qlarge)) for i in range(configurations["thread_limit"])]
        for p in workers:
            p.daemon = True
            p.start()

        start = time.time()
        dstat_process = mp.Process(target=monitor, args=("md0",))
        dstat_process.daemon = True
        dstat_process.start()

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
        dstat_process.terminate()
        for p in workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.1)
    else:
        if root == "/dev/null":
            configurations["direct"] = False
            configurations["checksum"] = False

        if ":" in HOST:
            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        else:
            sock = socket.socket()

        sock.bind((HOST, PORT))
        sock.listen(configurations["thread_limit"])

        process_status = mp.Array("i", [0 for _ in range(configurations["thread_limit"])])
        workers = [mp.Process(target=rcv_file, args=(sock, i,)) for i in range(configurations["thread_limit"])]
        for p in workers:
            p.daemon = True
            p.start()

        dstat_process = mp.Process(target=monitor, args=("md0",))
        dstat_process.daemon = True
        dstat_process.start()

        process_status[0] = 1
        while sum(process_status) > 0:
            time.sleep(0.1)

        dstat_process.terminate()
        for p in workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.1)

        if len(hash_values) > 0:
            rcv_checksums = get_checksum(list(hash_values.keys()))
            count = 0
            for key in hash_values:
                if hash_values[key] != rcv_checksums[key]:
                    logger.info(f"Integrity verification failed: {key}")
                else:
                    count += 1

            logger.info(f"Checksum verification success: {count}/{len(hash_values)}")
