import os
import re


class TestCheckups:
    monitor_input_json_temp_name = "temp.json"
    temp_log_name = "temp.log"

    @staticmethod
    def read_log_file_content(log_file_name: str) -> str:
        with open(log_file_name) as file:
            return file.read()

    def test_negative_no_monitor_input_json(self, monitor_input_json):

        os.rename(monitor_input_json, self.monitor_input_json_temp_name)

        os.system(f"python main.py --log_file_path={self.temp_log_name}")

        content = self.read_log_file_content(self.temp_log_name)

        try:
            assert re.search(r"ERROR.+No such file or directory:.+monitor_input\.json", content), (
                "No ERROR line about `monitor_input.json` in logs"
            )
        finally:
            os.rename(self.monitor_input_json_temp_name, monitor_input_json)
            os.remove(self.temp_log_name)

    def test_negative_empty_monitor_input_json(self, monitor_input_json):
        os.rename(monitor_input_json, self.monitor_input_json_temp_name)

        os.system(f"touch {monitor_input_json}")

        os.system(f"python main.py --log_file_path={self.temp_log_name}")

        content = self.read_log_file_content(self.temp_log_name)

        try:
            assert re.search(r"ERROR.+Expecting value: line 1 column 1 \(char 0\)", content), (
                "No ERROR line about `monitor_input.json` in logs"
            )
        finally:
            os.rename(self.monitor_input_json_temp_name, monitor_input_json)
            os.remove(self.temp_log_name)
