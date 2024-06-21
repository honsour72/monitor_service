from __future__ import annotations

import argparse
import sys
from functools import wraps
from time import time
from typing import Any, Callable

from loguru import logger as log


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


def safe(with_trace: bool = False) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
    def decorator(func: Callable) -> Callable[[], Any]:

        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as handled_exception:
                if with_trace:
                    log.exception(handled_exception)
                else:
                    log.error(handled_exception)
                sys.exit(1)

        return wrapper

    return decorator


def timeit(execution_seconds_limit: int = 3600) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """This decorator will time function execution and check:
    if it greater than `execution_seconds_limit` parameter, then raise an TimeoutException
    otherwise it will adjust `func` functionality to return time execution in seconds

    Resolves:
    * SQ-17912: https://sqream.atlassian.net/browse/SQ-17912
    * Sq-17913: https://sqream.atlassian.net/browse/SQ-17913

    :param execution_seconds_limit: timeout in seconds, decorator will raise an exception after reaching it
    :return: other decorator since it is a decorator with parameter :)
    """
    def decorator(func: Callable) -> Callable[[], Any]:

        @wraps(func)
        def wrapper(*args, **kwargs) -> tuple[Any, float]:
            start = time()
            result = func(*args, **kwargs)
            finish = time() - start

            if finish > execution_seconds_limit:
                raise TimeoutError(f"Execution of `{func.__name__}` function has reached the limit of time: "
                                   f"`{execution_seconds_limit}` in seconds. Arguments was: {args!r}, {kwargs!r}")
            return result, round(finish, 2)

        return wrapper
    return decorator
