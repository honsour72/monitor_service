from __future__ import annotations

import argparse
import json
import signal
from time import sleep, time
from dataclasses import asdict
from datetime import datetime
from multiprocessing import Process

import requests
from loguru import logger as log

from infra.utils import get_customer_metrics, get_allowed_metrics
from infra.sqream_connection import SqreamConnection


def run_monitor(args: argparse.Namespace) -> None:
    """
    Main function for run monitor service, especially `multiprocessing.Process` for every metric
    :param args: sequence of command-line arguments
    :return: None
    """
    loki_url = f"http://{args.loki_host}:{args.loki_port}/loki/api/v1/push"
    # collect customer metrics from `monitor_input.json`
    metrics = get_customer_metrics()
    log.info(f"Starting {len(metrics)} process for metrics: {', '.join(metrics.keys())}")
    # List of processes to kill other if something will happen to anyone
    processes = []
    # Run process for every metric
    for i, metric_name in enumerate(metrics):
        p = Process(target=schedule_process_for_metric,
                    args=(metric_name, metrics[metric_name], loki_url, processes))
        p.start()
        processes.append(p)

    signal.signal(signal.SIGINT, lambda s, f: terminate_metric_processes(s, f, processes=processes))


def terminate_metric_processes(*_, processes: list[Process] | None = None) -> None:
    """
    Handler for killing multiprocessing processes if program was interrupted (ctrl+c pressed)
    or in case of unhandled exception
    :param _: signal and frame - mandatory for `signal.signal` - removed here, because we just need to kill everything
    :param processes: list of `multiprocessing.Process` instances
    :return: None
    """
    if processes is not None:
        log.info(f"Killing all ({len(processes)}) processes")
        for process in processes:
            process.terminate()
    return None


def schedule_process_for_metric(metric_name: str, metric_timeout: int, url: str, processes: list[Process]) -> None:
    """
    Main process function to receive metric data from sqream and push it to Loki instance
    :param metric_name: string, metric name from `monitor_input.json`
    :param metric_timeout: int, timeout to wait after every `select <metric_name>();` query
    :param url: loki specific endpoint to push logs
    :param processes: list of `multiprocessing.Process` to kill all process in case of exception
    :return: None
    """
    log.debug(f"[{metric_name}]: Start process with timeout = {metric_timeout} sec")
    try:
        while True:
            # get data from sqream
            rows = SqreamConnection.execute(f"select {metric_name}()")
            if rows is None:
                raise ValueError(f"Sqream `select {metric_name}();` query returned `None` type (`{rows}`)")
            # send data to loki
            push_logs_to_loki(url=url, metric_name=metric_name, rows=rows)
            # sleep `metric_timeout` seconds
            sleep(metric_timeout)
    except Exception as unhandled_exception:
        log.exception(unhandled_exception)
    except KeyboardInterrupt:
        log.info(f"[{metric_name}]: process interrupted by user")
    finally:
        terminate_metric_processes(signal.SIGINT, None, processes)
        return


def push_logs_to_loki(url: str, metric_name: str, rows: list[tuple[str | int]]) -> None:
    if len(rows) == 0:
        log.warning(f"[{metric_name}]: sqream query `select {metric_name}();` returned 0 rows. Skip sending it to Loki")
        return
    log.info(f"[{metric_name}]: Get {len(rows)} rows from sqream after `select {metric_name}();` query")
    payload = build_payload(metric_name=metric_name, data=rows)
    answer = requests.post(url, json=payload)
    answer.raise_for_status()
    log.info(f"[Loki response]: {answer}. Request was: "
             f"`curl -X POST -H 'Content-Type: application/json' --data-raw '{json.dumps(payload)}' {url}`")


def build_payload(metric_name: str, data: list[tuple[str | int]]) -> dict[str, list[dict]]:
    """
    https://grafana.com/docs/loki/latest/reference/loki-http-api/#examples
    :param metric_name:
    :param data: list with tuples of `select <metric_name>();` query result
    :return: special dictionary for post request method
    """
    metric_cls = get_allowed_metrics(metric_name=metric_name)
    if isinstance(metric_cls, list):
        raise TypeError(f"[{metric_name}]: function `get_allowed_metrics` returned `{metric_cls}` "
                        f"instead of class instance")

    values = []
    labels = {"service_name": "monitor", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    for row in data:
        metric = metric_cls(*row)
        labels = asdict(metric)
        value = [str(int(time() * 1e9)), str(labels)]
        values.append(value)

    stream = {
        "stream": labels,
        "values": [[str(int(time() * 1e9)), str(labels)]]
    }
    payload = {"streams": [stream]}
    return payload
