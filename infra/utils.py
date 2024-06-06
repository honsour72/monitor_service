from __future__ import annotations

import argparse
import json
import os.path
from functools import wraps
from typing import Any, Callable

import requests
from loguru import logger as log

from infra.sqream_connection import SqreamConnection

# Since SQ-17798 we DO NOT need to send some metrics to loki (reset_leveldb_stats)
# Details: https://sqream.atlassian.net/browse/SQ-17798
_ALLOWED_METRICS = {
    "show_server_status": {"send_to_loki": True},
    "show_locks": {"send_to_loki": True},
    "get_leveldb_stats": {"send_to_loki": True},
    "show_cluster_nodes": {"send_to_loki": True},
    "get_license_info": {"send_to_loki": True},
    "reset_leveldb_stats": {"send_to_loki": False},
}


def get_command_line_arguments() -> argparse.Namespace:
    """usage: main.py [-h --help] [--host] [--port] [--database] [--user] [--password] [--clustered] [--service]
                      [--loki_host] [--loki_port] [--log_file_path]

    Command-line interface for monitor-service project

    optional arguments:
      -h, --help            show this help message and exit
      --host                Sqream ip address (default: `localhost`)
      --port                Sqream port (default: `5000`)
      --database            Sqream database (default: `master`)
      --user                Sqream user (default: `sqream`)
      --password            Sqream password (default: `sqream`)
      --clustered           Sqream clustered (default: `False`)
      --service             Sqream service (default: `monitor`)
      --loki_host           Loki remote address (default: `localhost`)
      --loki_port           Loki remote port (default: `3100`)
      --log_file_path       Path to file to store logs (default: `None`)

    :return: argparse.Namespace with parsed arguments
    """
    parser = argparse.ArgumentParser(description="Command-line interface for monitor-service project")
    parser.add_argument("--host", type=str, help="Sqream ip address", default="localhost")
    parser.add_argument("--port", type=int, help="Specify Sqream port", default=5000)
    parser.add_argument("--database", type=str, help="Specify Sqream database", default="master")
    parser.add_argument("--user", type=str, help="Specify Sqream user", default="sqream")
    parser.add_argument("--password", type=str, help="Specify Sqream password", default="sqream")
    parser.add_argument("--clustered", action="store_true", help="Specify Sqream clustered")
    parser.add_argument("--service", type=str, help="Sqream service (default: `monitor`)",
                        default="monitor")
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


def do_startup_checkups(args: argparse.Namespace) -> None:
    log.info("Starting checkups...")
    # 1. Check all customer metrics are allowed
    check_customer_metrics()
    # 2. Check sqream connection is established
    check_sqream_connection(args)
    # 3. Check sqream is working on CPU and not on GPU
    check_sqream_on_cpu(host=args.host, port=args.port)
    # 4. Check Loki's connection is established
    check_loki_connection(url=f"http://{args.loki_host}:{args.loki_port}/metrics")


def check_customer_metrics() -> None:
    """Check all metrics provided by customer in the `monitor_input.json` are known and values are valid"""
    customer_metrics = get_customer_metrics()
    for customer_metric in customer_metrics:
        if customer_metric not in _ALLOWED_METRICS:
            raise NameError(f"Metric `{customer_metric}` from `monitor_input.json` isn't allowed. "
                            f"Allowed metrics: {_ALLOWED_METRICS}")
        # try to convert value to float for make sure customer provide it correctly
        metric_timeout = customer_metrics[customer_metric]
        try:
            float(metric_timeout)
        except ValueError:
            raise ValueError(f"Can not convert metric `{customer_metric}` value `{metric_timeout}` to `float` type")
    log.success(f"All metrics are validated and allowed")


def get_customer_metrics(metrics_json_path: str | None = None) -> dict[str, int]:
    if metrics_json_path is None:
        metrics_json_path = os.path.join(os.getcwd(), "monitor_input.json")

    with open(metrics_json_path) as json_file:
        metrics = json.load(json_file)
        return metrics


def check_sqream_connection(args: argparse.Namespace) -> None:
    try:
        SqreamConnection(host=args.host, port=args.port, database=args.database, user=args.user,
                         password=args.password, clustered=args.clustered, service=args.service)
    except ConnectionRefusedError as connection_err:
        # except and raise it here because native exception text (`Connection refused, perhaps wrong IP?`)
        # isn't enough to understand the issue
        raise ConnectionRefusedError(f"Can not establish connection to sqream database `{args.database}` on "
                                     f"{args.host}:{args.port}. Credentials were: user=`{args.user}`, "
                                     f"password=`{args.password}` (clustered = `{args.clustered}`) "
                                     f"Service: {args.service}. Source exception is: {connection_err}")
    else:
        log.success("Sqream connection established successfully")


def check_sqream_on_cpu(host: str, port: int) -> None:
    """Ticket: https://sqream.atlassian.net/browse/SQ-17718

    We need to make sure that worker runs on CPU to avoid data affection
    :param host: sqreamd (server picker) address to establish connection
    :param port: sqreamd (server picker) port to establish connection
    :return: None
    """
    try:
        SqreamConnection.execute("select 1")
    except Exception:
        log.success(f"Query `select 1` raises `Internal Runtime Error` which means sqream is running on CPU.")
    else:
        raise TypeError(f"sqreamd on `{host}:{port}` works on GPU instead of CPU")


def check_loki_connection(url: str) -> None:
    response = requests.get(url)
    msg = f"Request `curl -X GET {url}` returns status_code = {response.status_code}"
    if response.status_code != 200:
        raise ValueError(msg)
    log.success(f"Loki connection established successfully.")


def is_metric_should_be_send(metric_name: str) -> bool:
    return _ALLOWED_METRICS[metric_name]["send_to_loki"]


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
        return wrapper
    return decorator
