from __future__ import annotations

import argparse
import json
import signal
from time import sleep, time
from datetime import datetime
from multiprocessing import Process, Event
from typing import Any
from urllib3.exceptions import NewConnectionError

import requests
from loguru import logger as log

from infra.utils import get_customer_metrics, is_metric_should_be_send
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
    log.info(f"Starting {len(metrics)} processes for metrics: {', '.join(metrics.keys())}")
    # List of processes to kill other if something will happen to anyone
    processes = []
    # multiprocessing Event to stop process's `while True` loop: if event.set() while loop will be broken
    process_crushed = Event()
    # Run process for every metric
    for metric_name, metric_timeout in metrics.items():
        p = Process(target=run_metric_worker,
                    args=(metric_name, metric_timeout, loki_url, process_crushed),
                    name=metric_name)
        p.start()
        processes.append(p)

    # set `terminate_metric_processes` as a function which will be triggered after ctrl+c pressed
    signal.signal(signal.SIGINT, lambda s, f: terminate_metric_processes(
        s, f, processes=processes, stop_event=process_crushed))

    # monitor event with loop below
    while not process_crushed.is_set():
        # monitor every process every second
        sleep(1)
    # if `process_crushed` event was set - terminate all other processes
    terminate_metric_processes(signal.SIGTERM, processes=processes)


def terminate_metric_processes(*_, processes: list[Process] | None = None, stop_event: Event | None = None) -> None:
    """
    Handler for killing multiprocessing processes if program was interrupted (ctrl+c pressed)
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


def run_metric_worker(metric_name: str, metric_timeout: int, url: str, stop_event: Event) -> None:
    """
    Main process function to receive metric data from sqream and push it to Loki instance

    Steps:
    1) Get data from sqream
    2) Check if this metric should be sent to Loki
    3) Send data to loki if we need it
    4) Sleep timeout (metric frequency)

    :param stop_event: multiprocessing.Event instance to keep Process working or abort it
    :param metric_name: string, metric name from `monitor_input.json`
    :param metric_timeout: float, timeout to wait after every `select <metric_name>();` query
    :param url: loki specific endpoint to push logs
    :return: None
    """

    log.debug(f"[{metric_name}]: process with timeout = {metric_timeout} sec started successfully")
    try:
        while not stop_event.is_set():
            # 1) Get data from sqream
            data = SqreamConnection.execute(f"select {metric_name}()")

            # 2) Check if this metric should be sent to Loki
            if is_metric_should_be_send(metric_name=metric_name):

                if len(data) == 0:
                    log.warning(f"[{metric_name}]: sqream query `select {metric_name}();`"
                                f" returned 0 rows. Skip sending it to Loki")
                else:
                    log.success(f"[{metric_name}]: `select {metric_name}();` -> {len(data)} rows")
                    # 3) Send data to loki if we need it
                    push_logs_to_loki(url=url, metric_name=metric_name, metric_timeout=metric_timeout, data=data)

            else:
                log.info(f"[{metric_name}]: shouldn't be sent to Loki")

            # 4) Sleep timeout (metric frequency)
            sleep(metric_timeout)
    except KeyboardInterrupt:
        log.info(f"[{metric_name}]: Process interrupted by user. Stop all metrics")
    except (requests.HTTPError, NewConnectionError) as loki_lost_connection:
        log.error(f"[{metric_name}]: Connection to loki was lost. Stop all metrics. "
                  f"(Native error: {loki_lost_connection})")
    except ConnectionRefusedError as sq_lost_connection:
        log.error(f"[{metric_name}]: Connection to Sqream instance was lost. Stop all metrics. "
                  f"(Native error: {sq_lost_connection})")
    except Exception as unhandled_exception:
        log.exception(unhandled_exception)
    finally:
        stop_event.set()


def push_logs_to_loki(url: str,
                      metric_name: str,
                      metric_timeout: int,
                      data: list[dict[str, str | int]] | dict[str, str | int]) -> None:
    """
    Function to send post http request to loki

    :param metric_timeout: frequency - just a label for loki storage
    :param url: loki endpoint to push logs (http://host:port/loki/api/v1/push)
    :param metric_name: string, metric name from `monitor_input.json` for labels in loki storage
    :param data: list of dicts or dict with row(s) data within

    Examples:
    1) For one row it will be one dict:
    { "server_ip": "127.0.0.1", "server_port": 5000, ... "statement_id": "node_6999" }
    2) For many rows it will be a list with dicts inside:
    [
        { "write_limit": "123", "read_limit": "321", ... "license_info": "some text" },
        ...
        { "write_limit": "456", "read_limit": "654", ... "license_info": "other text" },
    ]
    3) Sometimes data maybe empty list `[]`

    :return: Nothing, just send post http request
    """

    payload = build_payload(metric_name=metric_name, metric_timeout=metric_timeout, data=data)
    answer = requests.post(url, json=payload, allow_redirects=False, verify=True)
    if answer.status_code == 204:
        log.success(f"[{metric_name}]: Loki successfully accepts {len(data)} rows")
    else:
        raise requests.HTTPError(f"[{metric_name}]: {answer}. Request was: "
                                 f"`curl -X POST -H 'Content-Type: application/json' --data-raw "
                                 f"'{json.dumps(payload)}' {url}`")


def build_payload(metric_name: str,
                  metric_timeout: int,
                  data: list[dict[str, str | int]] | dict[str, str | int]
                  ) -> dict[str, list[dict[str, Any]]]:
    """
    Examples of curl post request for pushing logs to loki:
    https://grafana.com/docs/loki/latest/reference/loki-http-api/#examples

    Minimal example below:

    curl -H "Content-Type: application/json" -s -X POST "http://localhost:3100/loki/api/v1/push" \
        --data-raw '{"streams": [{ "stream": { "foo": "bar2" }, "values": [ [ "1570818238000000000", "fizzbuzz" ] ] }]}'

    It will send to loki one line: `fizzbuzz` with the timestamp `1570818238000000000` and label `foo=bar2`
    You also can send multiple lines but with only one labels mapping

    For our case every batch sending to loki will have:
    - `timestamp`
    - `metric_name` as `job`
    - `metric_timeout` as `response_time`

    Any utility functions return one row (which will be converted to dict{filed:value})
    will additional have `fields` as labels (provided by `labels.update(data)` code below)

    :param metric_timeout: frequency, how often utility function <metric_name> will be triggered
    :param metric_name: name of utility function used for `job` field in Loki
    :param data: list of dicts or dict with rows data within (see `push_logs_to_loki` for examples)
    :return: special dictionary (data-raw in example above) for post request body
    """

    labels: dict[str, Any] = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                              "job": metric_name,
                              "response_time": metric_timeout}

    if isinstance(data, dict):
        labels.update(data)
        values = [[str(int(time() * 1e9)), json.dumps(data)]]
    else:
        values = []
        for row in data:
            value = [str(int(time() * 1e9)), json.dumps(row)]
            values.append(value)

    stream = {
        "stream": labels,
        "values": values
    }
    return {"streams": [stream]}
