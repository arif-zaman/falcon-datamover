# Falcon: Online File Transfers Optimization
The application can only correctly function on Linux-based operating systems due to several Linux-based functional dependencies.

## Installation

You can install the falcon-datamover package from [PyPI](https://pypi.org/project/falcon-datamover/):

    pip install falcon-datamover

or

    python3 -m pip install falcon-datamover

The Falcon package is supported on Python 3.6 and above.

## Usage

Falcon is a command line application. please run help to find required arguments

    $ falcon --help
    usage: falcon [-h] [--host HOST] [--port PORT] [--data_dir DATA_DIR] [--method METHOD] [--max_cc MAX_CC] [--direct DIRECT] [--checksum CHECKSUM] agent

    positional arguments:
    agent                Please choose agent type: sender or receiver

    optional arguments:
    -h, --help           show this help message and exit
    --host HOST          Receiver host address; default: 127.0.0.1
    --port PORT          Receiver port number; default: 50021
    --data_dir DATA_DIR  data directory of sender or receiver
    --method METHOD      choose one of them : gradient, probe
    --max_cc MAX_CC      maximum concurrency
    --direct DIRECT      enable direct I/O
    --checksum CHECKSUM  enable checksum verification

for example: on the receiver node:

    $ falcon receiver --host 10.10.1.2 --port 5000 --data_dir /data/dest/

similarly, on the sender node:

    $ falcon sender --host 10.10.1.2 --port 5000 --data_dir /data/src/ --method probe
