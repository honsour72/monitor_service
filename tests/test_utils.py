import json
import os
import re
from subprocess import call

import pytest
from loguru import logger as log

from infra.sqream_connection import SqreamConnection
from infra.startups import check_loki_connection, check_sqream_connection
from infra.utils import add_log_sink


class TestUtils:
    monitor_input_json_temp_name = "monitor_input_temp.json"
    temp_log_name = "temp.log"

    @staticmethod
    def read_log_file_content(log_file_name: str) -> str:
        with open(log_file_name) as file:
            return file.read()

    def test_add_log_sink(self):
        test_log = "test.log"

        log.info("This line won't be in file")
        add_log_sink(test_log)
        log.info("This line will be")

        content = self.read_log_file_content(test_log)
        try:
            assert "This line will be" in content
            assert "This line won't be in file" not in content
        finally:
            os.remove(test_log)

    def test_negative_no_monitor_input_json(self, monitor_input_json):

        os.rename(monitor_input_json, self.monitor_input_json_temp_name)

        exit_code = call(["python", "main.py", "--username=sqream", "--password=sqream",
                          f"--log_file_path={self.temp_log_name}"])
        content = self.read_log_file_content(self.temp_log_name)

        try:
            assert exit_code == 1, f"monitor service expected exit code = 1, got {exit_code}"
            assert re.search(r"ERROR.+No such file or directory:.+monitor_input\.json", content), (
                "No ERROR line about `monitor_input.json` in logs"
            )
        finally:
            os.rename(self.monitor_input_json_temp_name, monitor_input_json)
            os.remove(self.temp_log_name)

    def test_negative_empty_monitor_input_json(self, monitor_input_json):
        os.rename(monitor_input_json, self.monitor_input_json_temp_name)

        os.system(f"touch {monitor_input_json}")

        exit_code = call(["python", "main.py", "--username=sqream", "--password=sqream",
                          f"--log_file_path={self.temp_log_name}"])
        assert exit_code == 1, f"monitor service expected exit code = 1, got {exit_code}"

        content = self.read_log_file_content(self.temp_log_name)

        try:
            assert re.search(r"ERROR.+Expecting value: line 1 column 1 \(char 0\)", content), (
                "No ERROR line about `monitor_input.json` in logs"
            )
        finally:
            os.rename(self.monitor_input_json_temp_name, monitor_input_json)
            os.remove(self.temp_log_name)

    @pytest.mark.parametrize(
        ("metrics", "expected_error"),
        (
            ({"wrong_metric": 1}, "Metric `wrong_metric` from `monitor_input.json` isn't allowed."),
            ({"show_locks": 2, "wrong_metric": 1}, "Metric `wrong_metric` from `monitor_input.json` isn't allowed."),
            ({"show_locks": "wrong_value"}, "Can not convert metric `show_locks` value `wrong_value` to `float` type"),
            ({"show_locks": -5}, "Metric `show_locks` timeout = -5. It can not be negative or equal zero"),
            ({"get_license_info": 0}, "Metric `get_license_info` timeout = 0. It can not be negative or equal zero"),
        ),
        ids=("wrong_metric", "allowed_metric_and_wrong_metric", "allowed_metric_wrong_value",
             "allowed_metric_negative_value", "allowed_metric_zero_value"),
    )
    def test_negative_not_allowed_metric_or_value(self, monitor_input_json, metrics, expected_error):
        os.rename(monitor_input_json, self.monitor_input_json_temp_name)

        with open(monitor_input_json, "w") as new_monitor_json:
            new_monitor_json.write(json.dumps(metrics))

        exit_code = call(["python", "main.py", "--username=sqream", "--password=sqream",
                          f"--log_file_path={self.temp_log_name}"])
        assert exit_code == 1, f"monitor service expected exit code = 1, got {exit_code}"

        content = self.read_log_file_content(self.temp_log_name)

        try:
            assert expected_error in content, f"Expected error (`{expected_error}`) not found in log content"
        finally:
            os.rename(self.monitor_input_json_temp_name, monitor_input_json)
            os.remove(self.temp_log_name)

    @pytest.mark.parametrize(
        ("host", "port", "database", "username", "password", "service", "clustered", "exception"),
        (
                ("wrong_host", 5000, "master", "sqream", "sqream", "monitor", False,
                 "Name or service not known"),
                ("localhost", "wrong_port", "master", "sqream", "sqream", "monitor", False,
                 "an integer is required (got type str)"),
                ("localhost", 8000, "master", "sqream", "sqream", "monitor", False,
                 "Can not establish connection to sqream"),
                ("localhost", 5000, "flomaster", "sqream", "sqream", "monitor", False,
                 "Database flomaster no longer exists"),
                ("localhost", 5000, "master", "ne_sqream", "sqream", "monitor", False,
                 "Error connecting to database: Login failure: role 'ne_sqream' doesn't exist"),
                ("localhost", 5000, "master", "sqream", "wrong_password", "monitor", False,
                 "Error connecting to database: "),
                ("localhost", 5000, "master", "sqream", "sqream", "wrong_service?", False,
                 "Error connecting to database: Login failure: "
                 "There are no instances subscribed to the requested service."),
                ("localhost", 5000, "master", "sqream", "sqream", "monitor", True,
                 "Connected with clustered=True, but apparently not a server picker port"),
        ),
        ids=("wrong_host", "wrong_port", "taken_port", "wrong_dbname", "wrong_username",
             "wrong_password", "wrong_service", "clustered=True"),
    )
    def test_negative_check_sqream_connection(
            self, host, port, database, username, password, service, clustered, exception,
    ):

        with pytest.raises(Exception) as sqream_connection_error:
            check_sqream_connection(host=host, port=port, username=username, password=password,
                                    clustered=clustered, service=service, database=database)
        assert exception in str(sqream_connection_error)

        SqreamConnection.close()

    @pytest.mark.parametrize(
        "url",
        (
            "http://wrong_host:3100/what",
            "http://localhost:8000",
            "http://wrong:5432",
        ),
        ids=("wrong_host_ok_port", "ok_host_wrong_port", "wrong_host_wrong_port"),
    )
    def test_negative_check_loki_connection(self, url: str):
        with pytest.raises(Exception) as loki_exception:
            check_loki_connection(url)
        assert "Max retries exceeded with url" in str(loki_exception)
