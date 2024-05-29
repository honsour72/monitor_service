from __future__ import annotations

import argparse
import inspect
import json
import os.path
from typing import Any

import requests
from loguru import logger as log

from infra import sqream_metrics
from infra.sqream_metrics import ShowLocks, ShowClusterNodes, ShowServerStatus, GetLicenseInfo, GetLevelDbStats
from infra.sqream_connection import SqreamConnection


def get_command_line_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Command-line interface for monitor-service project')
    parser.add_argument('--host', type=str, help='Sqream ip address', default='localhost')
    parser.add_argument('--port', type=int, help='Specify Sqream port', default=5000)
    parser.add_argument('--database', type=str, help='Specify Sqream database', default='master')
    parser.add_argument('--user', type=str, help='Specify Sqream user', default='sqream')
    parser.add_argument('--password', type=str, help='Specify Sqream password', default='sqream')
    parser.add_argument('--clustered', action='store_true', help='Specify Sqream clustered')
    parser.add_argument('--service', type=str, help="Sqream service (default: `monitor`)",
                        default='monitor')
    parser.add_argument('--loki_host', type=str, help='Loki remote address', default='127.0.0.1')
    parser.add_argument('--loki_port', type=int, help='Loki remote port', default="3100")

    return parser.parse_args()


def do_startup_checkups(args: argparse.Namespace) -> None:
    log.info("Starting checkups...")
    # 1. Check all customer metrics are allowed
    check_customer_metrics()
    # 2. Check sqream connection is established
    check_sqream_connection(args)
    # 3. Check sqream is working on CPU and not on GPU
    check_sqream_on_cpu(host=args.host, port=args.port)
    # 4. Check Loki's connection is established
    check_loki_connection(url=f"http://{args.loki_host}:{args.loki_port}/ready")


def check_customer_metrics() -> None:
    """Check all metrics provided by customer in the `monitor_input.json` are known"""
    customer_metrics = get_customer_metrics()
    allowed_metrics = get_allowed_metrics()
    for customer_metric in customer_metrics:
        if customer_metric not in allowed_metrics:
            raise NameError(f"Metric `{customer_metric}` from `monitor_input.json` isn't allowed. "
                            f"Allowed metrics: {allowed_metrics}")
    log.success(f"All customer metrics {customer_metrics} are allowed")


def get_allowed_metrics(
    metric_name: str | None = None,
) -> (
    list[str]
    | ShowLocks
    | ShowClusterNodes
    | ShowServerStatus
    | GetLicenseInfo
    | GetLevelDbStats
    | None
):
    """
    Return `job` - sting attribute from every Metric dataclass in sqream_metrics.py if `metric_name` isn't specified
    Metric class instance - otherwise
    :param metric_name: string - name of metric, like `show_locks`, `get_leveldb_stats`, etc.
    :return: Metric class instance if `metric_name` is specified, list of all metrics based on `job` attribute otherwise
    """

    # Take all sqream_metrics.py entities which are classes and have `job` attribute
    # (only metrics dataclass has)
    metric_names = []
    for _, m_cls in inspect.getmembers(sqream_metrics, inspect.isclass):
        if hasattr(m_cls, "job"):
            if metric_name and metric_name == m_cls.job:
                return m_cls
            metric_names.append(m_cls.job)

    if metric_name is None:
        return metric_names

    raise NameError(f"Current metric `{metric_name}` wasn't found in allowed metrics: `{metric_names}`")


def get_customer_metrics(metrics_json_path: str = None) -> dict[str, int]:
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
    try:
        SqreamConnection.execute("select 1")
    except Exception as InternalRuntimeError:
        log.success(f"Query `select 1` raises `Internal Runtime Error` which means sqream is running on CPU. "
                    f"(Exception: `{repr(InternalRuntimeError)}`)")
    else:
        raise TypeError(f"sqreamd on `{host}:{port}` works on GPU instead of CPU")


def check_loki_connection(url: str) -> None:
    response = requests.get(url)
    msg = f"Request `curl -X GET {url}` returns `{response.text.strip()}` with status_code = {response.status_code}"
    if response.status_code != 200:
        raise ValueError(f"{msg}. Perhaps Loki's Ingester is not ready")
    log.success(f"Loki connection established successfully ({msg})")


def safe(with_trace: bool = False) -> callable:
    def decorator(func: callable) -> callable:
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
