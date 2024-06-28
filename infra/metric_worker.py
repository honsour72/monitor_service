from __future__ import annotations

import json
from datetime import datetime
from time import time
from typing import Any
from multiprocessing import Process, Event

from time import sleep
import requests
from requests.exceptions import HTTPError
from urllib3.exceptions import NewConnectionError
from loguru import logger as log
import sys

from infra.sqream_connection import SqreamConnection


class MetricWorkerProcess(Process):
    def __init__(self,
                 metric_name: str,
                 metric_timeout: int,
                 send_to_loki: bool,
                 host: str,
                 port: int,
                 username: str,
                 password: str,
                 database: str,
                 clustered: bool,
                 service: str,
                 loki_url: str,
                 stop_event: Event,
                 *args,
                 **kwargs):
        self.metric_name = metric_name
        self.metric_timeout = metric_timeout
        self.send_to_loki = send_to_loki
        self.loki_url = loki_url
        self.stop_event = stop_event
        self.sqream_connection: SqreamConnection = SqreamConnection(host=host, port=port, username=username,
                                                                    password=password, database=database,
                                                                    clustered=clustered, service=service)
        super().__init__(*args, **kwargs)

    def run(self):
        """Main process function to receive metric data from sqream and push it to Loki instance

        Steps:
        1) Get data from sqream
        2) Check if this metric should be sent to Loki
        3) Send data to loki if we need it
        4) Sleep timeout (metric frequency)

        :return: None
        """
        log.debug(f"[{self.metric_name}]: process with timeout = {self.metric_timeout} sec started successfully")
        try:
            while not self.stop_event.is_set():
                # 1) Get data from sqream
                data, execution_time = self.sqream_connection.execute(f"select {self.metric_name}()")

                # 2) Check if this metric should be sent to Loki
                if self.send_to_loki:

                    if len(data) == 0:
                        log.warning(f"[{self.metric_name}]: sqream query `select {self.metric_name}();`"
                                    f" returned 0 rows. Skip sending it to Loki")
                    else:
                        log.success(f"[{self.metric_name}]: fetched {len(data)} rows from sqream")
                        # 3) Send data to loki if we need it
                        self.push_logs_to_loki(data=data)

                else:
                    log.info(f"[{self.metric_name}]: shouldn't be sent to Loki")

                # 4) Sleep gap nearby timeout (metric frequency)
                timeout = self.count_metric_timeout(execution_time=execution_time)
                log.info(f"[{self.metric_name}]: timeout = {timeout} seconds, "
                         f"because sqream execution time was {execution_time}.")
                sleep(timeout)
        except KeyboardInterrupt:
            log.info(f"[{self.metric_name}]: Process interrupted by user. Stop all metrics")
        except (HTTPError, NewConnectionError):
            log.error(f"[{self.metric_name}]: Connection to loki was lost. Stop all metrics.")
        except ConnectionRefusedError:
            log.error(f"[{self.metric_name}]: Connection to Sqream instance was lost. Stop all metrics")
        except Exception as unhandled_exception:
            log.exception(unhandled_exception)
            sys.exit(2)
        finally:
            self.stop_event.set()

    def push_logs_to_loki(self, data: list[dict[str, str | int]] | dict[str, str | int]) -> None:
        """Function to send post http request to loki

        :param data: list of dicts or dict with row(s) data within

        Examples
        --------
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
        payload = self.build_payload(data=data)
        answer = requests.post(self.loki_url, json=payload, allow_redirects=False, verify=True)
        if answer.status_code == 204:
            log.success(f"[{self.metric_name}]: Loki successfully accepts {len(data)} rows")
        else:
            raise requests.HTTPError(f"[{self.metric_name}]: {answer}. Request was: "
                                     f"`curl -X POST -H 'Content-Type: application/json' --data-raw "
                                     f"'{json.dumps(payload)}' {self.loki_url}`")

    def build_payload(self, data: list[dict[str, str | int]] | dict[str, str | int]) -> dict[str, list[dict[str, Any]]]:
        """Examples of curl post request for pushing logs to loki:
        https://grafana.com/docs/loki/latest/reference/loki-http-api/#examples

        Minimal example below:

        curl -H "Content-Type: application/json" -s -X POST "http://localhost:3100/loki/api/v1/push" \
            --data-raw '{"streams": [{ "stream": { "foo": "bar2" }, "values": [ [ "1570818238000000000", "foo" ] ] }]}'

        It will send to loki one line: `fizzbuzz` with the timestamp `1570818238000000000` and label `foo=bar2`
        You also can send multiple lines but with only one labels mapping

        For our case every batch sending to loki will have:
        - `timestamp`
        - `metric_name` as `job`
        - `metric_timeout` as `response_time`

        Any utility functions return one row (which will be converted to dict{filed:value})
        will additional have `fields` as labels (provided by `labels.update(data)` code below)

        :param data: list of dicts or dict with rows data within (see `push_logs_to_loki` for examples)
        :return: special dictionary (data-raw in example above) for post request body
        """
        labels: dict[str, Any] = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                  "job": self.metric_name,
                                  "response_time": self.metric_timeout}

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
            "values": values,
        }
        return {"streams": [stream]}

    def count_metric_timeout(self, execution_time: float) -> float:
        """We need to count the rest of timeout, because we needn't wait full time.

        For example, if we have a metric with 3 seconds of timeout and 2 seconds of execution on sqream side
        (`execution_time` < `metric_timeout`)
        Timeline of that metric could look like:

        |           |                        seconds                            |
        |   action  | 1  | 2  | 3  | 4  | 5  | 6  | 7  | 8  | 9  | 10 | 11 | 12 |
        |-----------|----|----|----|----|----|----|----|----|----|----|----|----|
        | execution |[][]|[][]|----|----|----|[][]|[][]|----|----|----|[][]|[][]|
        |  timeout  |----|----|####|####|####|----|----|####|####|####|----|----|

        And for 12 seconds we will handle metric execution only 2 times according to table above
        However we need it different:

        |           |                        seconds                            |
        |   action  | 1  | 2  | 3  | 4  | 5  | 6  | 7  | 8  | 9  | 10 | 11 | 12 |
        |-----------|----|----|----|----|----|----|----|----|----|----|----|----|
        | execution |[][]|[][]|----|[][]|[][]|----|[][]|[][]|----|[][]|[][]|----|
        |  timeout  |####|####|####|####|####|####|####|####|####|####|####|####|

        Metric execution and waiting need to be parallelized, because in this case we can handle metric 4 times instead of 3

        For metric with execution time greater than timeout (`execution_time` > `metric_timeout`)
        for example, 6 seconds as execution time and 3 - as a timeout, table could like:

        |           |                        seconds                            |
        |   action  | 1  | 2  | 3  | 4  | 5  | 6  | 7  | 8  | 9  | 10 | 11 | 12 |
        |-----------|----|----|----|----|----|----|----|----|----|----|----|----|
        | execution |[][]|[][]|[][]|[][]|[][]|----|----|----|[][]|[][]|[][]|[][]|
        |  timeout  |----|----|----|----|----|####|####|####|----|----|----|----|

        And we can handle full metric data only one time. However, if we parallelize execution and waiting,
        we will get it twice:

        |           |                        seconds                            |
        |   action  | 1  | 2  | 3  | 4  | 5  | 6  | 7  | 8  | 9  | 10 | 11 | 12 |
        |-----------|----|----|----|----|----|----|----|----|----|----|----|----|
        | execution |[][]|[][]|[][]|[][]|[][]|----|[][]|[][]|[][]|[][]|[][]|----|
        |  timeout  |####|####|####|####|####|####|####|####|####|####|####|####|
                    ╰──1 timeout───╯╰──2 timeout──╯╰──3 timeout──╯╰──4 timeout──╯

        :param execution_time: time sqream took to execute metric, seconds
        :return: actual time, how many seconds process need to wait
        """
        if execution_time > self.metric_timeout:
            # 1.  We need to understand how many times execution exceeded timout with: execution_time // metric_timeout
            #     if execution time for example - 38 seconds. By whole division on metric timeout we will get:
            #     38 // 10 = 3
            # 2.  Multiplying result above with metric timeout and plus other timeout, we will get a whole time to wait:
            #     3 * 10 + 10 = 40
            # 3.  Now we need to see the difference between whole timeout and execution time: 40 - 38 = 2 seconds
            #     In full formula it looks like: 38 // 10 * 10 + 10 - 38 = 2
            #                                       1     2    3    4
            timeout = execution_time // self.metric_timeout * self.metric_timeout + self.metric_timeout - execution_time
        else:
            # Otherwise just make a deduction, like: 15 (timeout) - 8 (execution time) = 7 result time to wait
            timeout = self.metric_timeout - execution_time
        return round(timeout, 2)
