from __future__ import annotations

import argparse
import json
import signal
from time import sleep, time
from datetime import datetime
from multiprocessing import Process, Event

import requests
from loguru import logger as log

from infra.utils import get_customer_metrics
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
    killall_process_event = Event()
    # Run process for every metric
    for i, metric_name in enumerate(metrics):
        p = Process(target=schedule_process_for_metric,
                    args=(metric_name, metrics[metric_name], loki_url, killall_process_event),
                    name=metric_name)
        p.start()
        processes.append(p)

    signal.signal(signal.SIGINT, lambda s, f: terminate_metric_processes(
        s, f, processes=processes, stop_event=killall_process_event))

    while not killall_process_event.is_set():
        # monitor every process every second
        sleep(1)

    terminate_metric_processes(signal.SIGTERM, processes=processes)


def terminate_metric_processes(*_, processes: list[Process] | None = None, stop_event: Event | None = None) -> None:
    """
    Handler for killing multiprocessing processes if program was interrupted (ctrl+c pressed)
    or in case of unhandled exception
    :param stop_event:
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
    return None


def schedule_process_for_metric(metric_name: str, metric_timeout: int, url: str, stop_event: Event) -> None:
    """
    Main process function to receive metric data from sqream and push it to Loki instance
    :param stop_event:
    :param metric_name: string, metric name from `monitor_input.json`
    :param metric_timeout: int, timeout to wait after every `select <metric_name>();` query
    :param url: loki specific endpoint to push logs
    :return: None
    """
    log.debug(f"[{metric_name}]: Start process with timeout = {metric_timeout} sec")
    try:
        while not stop_event.is_set():
            # get data from sqream
            data = SqreamConnection.execute(f"select {metric_name}()")
            # send data to loki
            push_logs_to_loki(url=url, metric_name=metric_name, data=data)
            # sleep `metric_timeout` seconds
            sleep(metric_timeout)
    except KeyboardInterrupt:
        log.info(f"[{metric_name}]: Process interrupted by user. Stop all metrics")
    except requests.HTTPError as loki_lost_connection:
        log.error(f"[{metric_name}]: Connection to loki was lost. Stop all metrics. "
                  f"(Native error: {loki_lost_connection})")
    except ConnectionRefusedError as sq_lost_connection:
        log.error(f"[{metric_name}]: Connection to Sqream instance was lost. Stop all metrics. "
                  f"(Native error: {sq_lost_connection})")
    except Exception as unhandled_exception:
        log.exception(unhandled_exception)
    finally:
        stop_event.set()
        return None


def push_logs_to_loki(url: str, metric_name: str, data: list[dict[str, str | int]] | dict[str, str | int]) -> None:
    if len(data) == 0:
        log.warning(f"[{metric_name}]: sqream query `select {metric_name}();` returned 0 rows. Skip sending it to Loki")
        return None
    log.info(f"[{metric_name}]: Get {len(data)} rows from sqream after `select {metric_name}();` query")
    payload = build_payload(metric_name=metric_name, data=data)
    answer = requests.post(url, json=payload)
    message = (f"[{metric_name}]: {answer}. Request was: "
               f"`curl -X POST -H 'Content-Type: application/json' --data-raw '{json.dumps(payload)}' {url}`")
    if answer.status_code == 204:
        log.info(message)
    else:
        raise requests.HTTPError(message)


def build_payload(metric_name: str, data: list[dict[str, str | int]] | dict[str, str | int]) -> dict[str, list[dict]]:
    """
    https://grafana.com/docs/loki/latest/reference/loki-http-api/#examples
    :param metric_name:
    :param data: list with tuples of `select <metric_name>();` query result
    :return: special dictionary for post request method
    """
    labels = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "job": metric_name}

    if isinstance(data, dict):
        labels.update(data)
        values = [[str(int(time() * 1e9)), str(labels)]]
    else:
        values = []
        for row in data:
            labels.update(row)
            value = [str(int(time() * 1e9)), str(row.values())]
            values.append(value)

    stream = {
        "stream": labels,
        "values": values
    }
    payload = {"streams": [stream]}
    return payload
