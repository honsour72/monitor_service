from __future__ import annotations

import json
from pathlib import Path

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


def do_startup_checkups(host: str,
                        port: int,
                        database: str,
                        username: str,
                        password: str,
                        clustered: bool,
                        service: str,
                        loki_host: str,
                        loki_port: int) -> None:
    log.info("Starting checkups...")
    # 1. Check all customer metrics are allowed
    check_customer_metrics()
    # 2. Check sqream connection is established
    check_sqream_connection(host, port, database, username, password, clustered, service)
    # 3. Check sqream is working on CPU and not on GPU
    check_sqream_on_cpu(host=host, port=port)
    # 4. Check Loki's connection is established
    check_loki_connection(url=f"http://{loki_host}:{loki_port}/metrics")


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
        if metric_timeout <= 0:
            raise ValueError(f"Metric `{customer_metric}` timeout = {metric_timeout}. "
                             f"It can not be negative or equal zero")
    log.success("All metrics are validated and allowed")


def get_customer_metrics(metrics_json_path: str | None = None) -> dict[str, int]:
    if metrics_json_path is None:
        # Here we need to get absolute path of `monitor_input.json`
        # regardless of directory from which we start `main.py`

        # For example, if we run `python main.py` from /home/sqreamdb-monitor-service with os.getcwd(),
        # we will get path like `/home/sqreamdb-monitor-service/monitor_input.json`
        # Or if we do the same command but from `/home` directory,
        # we will get = `/home/monitor_input.json` which is wrong

        # for that reason we can not use `os.getcwd()` here and use `Path('utils.py').parent.parent`
        # to make it `reverse-relative`
        metrics_json_path = Path(__file__).parent.parent / "monitor_input.json"

    with open(metrics_json_path) as json_file:
        metrics = json.load(json_file)
        return metrics


def check_sqream_connection(host: str,
                            port: int,
                            database: str,
                            username: str,
                            password: str,
                            clustered: bool,
                            service: str,
                            ) -> None:
    try:
        SqreamConnection(host=host, port=port, database=database, user=username, password=password,
                         clustered=clustered, service=service)
    except ConnectionRefusedError as connection_err:
        # except and raise it here because native exception text (`Connection refused, perhaps wrong IP?`)
        # isn't enough to understand the issue
        raise ConnectionRefusedError(f"Can not establish connection to sqream database `{database}` on "
                                     f"{host}:{port}. Credentials were: user=`{username}`, "
                                     f"password=`{password}` (clustered = `{clustered}`) "
                                     f"Service: {service}. Source exception is: {connection_err!r}")
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
        log.success("Query `select 1` raises `Internal Runtime Error` which means sqream is running on CPU.")
    else:
        raise TypeError(f"sqreamd on `{host}:{port}` works on GPU instead of CPU")


def check_loki_connection(url: str) -> None:
    """Check connection to loki instance with `/metrics` endpoint - it's the shortest way to make sure loki is working
    without affecting data
    :param url: full URL to loki instance
    :return: None
    """
    response = requests.get(url)
    msg = f"Request `curl -X GET {url}` returns status_code = {response.status_code}"
    if response.status_code != 200:
        raise ValueError(msg)
    log.success("Loki connection established successfully.")


def is_metric_should_be_send(metric_name: str) -> bool:
    """Since SQ-17798 we DO NOT need to send some metrics to loki (`reset_leveldb_stats`)
    Details: https://sqream.atlassian.net/browse/SQ-17798
    :param metric_name: name of metric to get `send_to_loki` key
    :return: True if we need send it to Loki, False - otherwise
    """
    return _ALLOWED_METRICS[metric_name]["send_to_loki"]
