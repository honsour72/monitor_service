from __future__ import annotations

import argparse
from contextlib import contextmanager
from multiprocessing import Process, Event
from time import perf_counter
from typing import Callable

from loguru import logger as log


class SqreamUtilityFunctionTimeExceeded(Exception):
    """Execution time of utility function has exceeded"""


def get_command_line_arguments() -> argparse.Namespace:
    """usage: main.py [-h --help] [--host] [--port] [--database] --username --password [--clustered] [--service]
                      [--loki_host] [--loki_port] [--log_file_path]

    Command-line interface for monitor-service project

    required arguments:
      --username            Sqream database username
      --password            Sqream database password

    optional arguments:
      -h, --help            show this help message and exit
      --host                Sqream ip address (default: `localhost`)
      --port                Sqream port (default: `5000`)
      --database            Sqream database (default: `master`)
      --clustered           Sqream clustered (default: `False`)
      --service             Sqream service (default: `monitor`)
      --loki_host           Loki remote address (default: `localhost`)
      --loki_port           Loki remote port (default: `3100`)
      --log_file_path       Path to file to store logs (default: `None`)

    :return: argparse.Namespace with parsed arguments
    """
    parser = argparse.ArgumentParser(prog="Sqream monitor service",
                                     description="Developer as a part of RCA (Root Cause Analysis) "
                                                 "for observe customer's sqream infrastructure",
                                     epilog="Developed by Michael Rogozin (michaelr@sqreamtech.com)")
    parser.add_argument("--username", type=str, help="Specify Sqream username", required=True)
    parser.add_argument("--password", type=str, help="Specify Sqream password", required=True)
    parser.add_argument("--host", type=str, help="Sqream ip address", default="localhost")
    parser.add_argument("--port", type=int, help="Specify Sqream port", default=5000)
    parser.add_argument("--database", type=str, help="Specify Sqream database", default="master")
    parser.add_argument("--clustered", action="store_true", help="Specify Sqream clustered")
    parser.add_argument("--service", type=str, help="Sqream service (default: `monitor`)", default="monitor")
    parser.add_argument("--loki_host", type=str, help="Loki remote address", default="127.0.0.1")
    parser.add_argument("--loki_port", type=int, help="Loki remote port", default="3100")
    parser.add_argument("--log_file_path", type=str, help="Name of file to store logs", default=None)

    return parser.parse_args()


def add_log_sink(log_file_path: str | None = None) -> None:
    """Add loguru sink for store log lines if `log_file_path` was specified. More documentation here:
    https://loguru.readthedocs.io/en/stable/api/logger.html#loguru._logger.Logger.add
    :param log_file_path: string - path for logs file
    :return: None
    """
    if log_file_path is not None:
        log.info(f"Logs also will be provided to {log_file_path}")
        log.add(log_file_path)


@contextmanager
def timeit(execution_seconds_limit: int = 3600) -> Callable[[], float]:
    start = perf_counter()
    yield lambda: perf_counter() - start
    if perf_counter() - start > execution_seconds_limit:
        raise SqreamUtilityFunctionTimeExceeded("Execution time exceeds {} seconds".format(execution_seconds_limit))


def terminate_metric_processes(*_, processes: list[Process] | None = None, stop_event: Event | None = None) -> None:
    """Handler for killing multiprocessing processes if program was interrupted (ctrl+c pressed)
    or in case of unhandled exception
    :param stop_event: multiprocessing.Event class for set it to prevent other processes work
    :param _: signal and frame - mandatory for `signal.signal` - removed here, because we just need to kill everything
    :param processes: list of `multiprocessing.Process` instances
    :return: None
    """
    if stop_event is not None:
        stop_event.set()
    if processes is not None:
        log.info(f"Killing all ({len(processes)}) processes")
        for process in processes:
            process.terminate()
            log.info(f"Process `{process.name}` terminated successfully")
