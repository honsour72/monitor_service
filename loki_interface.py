import requests
import time
from datetime import datetime
import json

# when init me i need to get loki_url
class LokiInit:
    def __init__(self, loki_url):
        self.loki_url = loki_url


    def get_show_server_status(self, metric_data: tuple) -> tuple:
        server_status_output = {
            "service": f"{metric_data[0]}",
            "instance_id": f"{metric_data[1]}",
            "connection_id": f"{metric_data[2]}",
            "server_ip": f"{metric_data[3]}",
            "server_port": f"{metric_data[4]}",
            "database_name": f"{metric_data[5]}",
            "user_name": f"{metric_data[6]}",
            "client_ip": f"{metric_data[7]}",
            "statement_id": f"{metric_data[8]}",
            "statement": f"{metric_data[9]}",
            "statement_start_time": f"{metric_data[10]}",
            "statement_status": f"{metric_data[11]}",
            "statement_status_start": f"{metric_data[12]}"
        }
        return server_status_output

    def get_show_locks(self, metric_data: tuple) -> tuple:
        show_locks_output = {
                "statement_id": f"{metric_data[0]}",
                "statement_string": f"{metric_data[1]}",
                "username": f"{metric_data[2]}",
                "server": f"{metric_data[3]}",
                "port": f"{metric_data[4]}",
                "locked_object": f"{metric_data[5]}",
                "lock_mode": f"{metric_data[6]}",
                "statement_start_time": f"{metric_data[7]}",
                "lock_start_time": f"{metric_data[8]}"
            }
        return show_locks_output

    def get_leveldb_stats(self,  metric_data: tuple) -> tuple:
        leveldb_stats_output = {
                "timestamp": f"{metric_data[0]}",
                "server_ip": f"{metric_data[1]}",
                "server_port": f"{metric_data[2]}",
                "msg": f"{metric_data[3]}",
                "count": f"{metric_data[4]}",
                "average": f"{metric_data[5]}",
                "max": f"{metric_data[6]}",
                "max_timestamp": f"{metric_data[7]}",
                "variance": f"{metric_data[8]}",
            }
        return leveldb_stats_output

    def create_log_entry(self, metric_lable, log_line):
        """Create a log entry for Loki."""
        timestamp = int(time.time() * 1e9)  # Convert to nanoseconds
        return {
            'streams': [
                {
                    'stream': metric_lable,
                    'values': [
                        [str(timestamp), log_line]
                    ]
                }
            ]
        }

    def push_logs_to_loki(self, log_entry):
        """Push logs to Loki."""
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.post(self.loki_url, json=log_entry, headers=headers)
        response.raise_for_status()
        print('Successfully pushed logs to Loki.')

    def dict_to_log_line(self, data):
        """Convert a dictionary to a formatted log line."""
        return json.dumps(data)

    def send_metric_table_to_loki(self, metric_name: str, metric_info: dict):
        metric_lable = {'job': f"{metric_name}"}
        log_line = self.dict_to_log_line(metric_info)
        log_entry = self.create_log_entry(metric_lable, log_line)
        self.push_logs_to_loki(log_entry)
