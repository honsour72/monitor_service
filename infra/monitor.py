from __future__ import annotations

import json
import signal
from multiprocessing import Event
from time import sleep
from pathlib import Path

import requests
from loguru import logger as log

from infra.sqream_connection import SqreamConnection
from infra.metric_worker import MetricWorkerProcess
from infra.utils import terminate_metric_processes


class MonitorService:
    _ALLOWED_METRICS = {
        "show_server_status": {"send_to_loki": True},
        "show_locks": {"send_to_loki": True},
        "get_leveldb_stats": {"send_to_loki": True},
        "show_cluster_nodes": {"send_to_loki": True},
        "get_license_info": {"send_to_loki": True},
        "reset_leveldb_stats": {"send_to_loki": False},
    }

    def __init__(self,
                 host: str,
                 port: int,
                 database: str,
                 username: str,
                 password: str,
                 clustered: bool,
                 service: str,
                 loki_host: str,
                 loki_port: int,
                 log_file_path: str):
        self.host: str = host
        self.port: int = port
        self.username: str = username
        self.password: str = password
        self.database: str = database
        self.clustered: bool = clustered
        self.service: str = service
        self.loki_host: str = loki_host
        self.loki_port: int = loki_port
        self.log_file_path: str = log_file_path

        self.workers: list[MetricWorkerProcess] = []
        self.stop_event: Event = Event()
        self.metrics: dict[str, int] = self.get_customer_metrics()
        self.sqream_connection: SqreamConnection = SqreamConnection(host=self.host,
                                                                    port=self.port,
                                                                    database=self.database,
                                                                    username=self.username,
                                                                    password=self.password,
                                                                    clustered=self.clustered,
                                                                    service=self.service)
        # Check sqream is working on CPU and not on GPU
        self.check_sqream_on_cpu()
        # Check Loki's connection is established
        self.check_loki_connection()

    def check_customer_metrics(self, customer_metrics: dict[str, int]) -> None:
        """Check all metrics provided by customer in the `monitor_input.json` are known and values are valid"""
        for customer_metric in customer_metrics:
            if customer_metric not in self._ALLOWED_METRICS:
                raise NameError(f"Metric `{customer_metric}` from `monitor_input.json` isn't allowed. "
                                f"Allowed metrics: {self._ALLOWED_METRICS}")
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

    def get_customer_metrics(self, metrics_json_path: str | None = None) -> dict[str, int]:
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

        self.check_customer_metrics(metrics)
        return metrics

    def check_sqream_on_cpu(self) -> None:
        """Ticket: https://sqream.atlassian.net/browse/SQ-17718

        We need to make sure that worker runs on CPU to avoid data affection
        :return: None
        """
        try:
            self.sqream_connection.execute("select 1")
        except Exception:
            log.success("Query `select 1` raises `Internal Runtime Error` which means sqream is running on CPU.")
        else:
            raise TypeError(f"sqreamd on `{self.host}:{self.port}` works on GPU instead of CPU")

    def check_loki_connection(self) -> None:
        """Check connection to loki instance with `/metrics` endpoint - it's the shortest way to make sure loki is working
        without affecting data
        :return: None
        """
        response = requests.get(f"http://{self.loki_host}:{self.loki_port}/metrics")
        msg = (f"Request `curl -X GET http://{self.loki_host}:{self.loki_port}/metrics` "
               f"returns status_code = {response.status_code}")
        if response.status_code != 200:
            raise ValueError(msg)
        log.success("Loki connection established successfully.")

    def run(self):
        """Main function for run monitor service, especially `multiprocessing.Process` for every metric
        :return: None
        """
        # set `terminate_metric_processes` as a function which will be triggered after ctrl+c pressed
        signal.signal(signal.SIGINT, lambda s, f: terminate_metric_processes(
            s, f, processes=self.workers, stop_event=self.stop_event))

        self._init_workers()

        for worker in self.workers:
            worker.start()

        # monitor every process every second
        while not self.stop_event.is_set():
            sleep(1)

        # if `process_crushed` with any exception (loop above was broken) - terminate all other processes
        terminate_metric_processes(signal.SIGTERM, processes=self.workers)

    def _init_workers(self):
        for metric_name, metric_timeout in self.metrics.items():
            worker = MetricWorkerProcess(metric_name=metric_name, metric_timeout=metric_timeout,
                                         stop_event=self.stop_event, host=self.host, port=self.port,
                                         username=self.username, password=self.password,
                                         database=self.database, service=self.service, clustered=self.clustered,
                                         send_to_loki=self._ALLOWED_METRICS[metric_name]["send_to_loki"],
                                         loki_url=f"http://{self.loki_host}:{self.loki_port}/loki/api/v1/push")
            self.workers.append(worker)
