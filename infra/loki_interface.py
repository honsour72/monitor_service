from __future__ import annotations

import json
import time
from datetime import datetime

import requests


class LokiInit:
    METRICS = {}

    def __init_subclass__(cls, **kwargs):
        cls.METRICS[cls.METRIC] = cls

    def __init__(self, loki_url: str):
        self.loki_url = loki_url

    @staticmethod
    def dict_to_log_line(data: dict):
        """Convert a dictionary to a formatted log line."""
        return json.dumps(data)

    @staticmethod
    def create_log_entry(metric_labels: dict, log_line: str = None) -> dict:
        """Create a log entry for Loki."""
        return {
            'streams': [
                {
                    'stream': metric_labels,
                    'values': [
                        [str(int(time.time() * 1e9)), str(metric_labels)]
                    ]
                }
            ]
        }
    
    def push_logs_to_loki(self, log_entry: dict):
        """Push logs to Loki."""
        response = requests.post(self.loki_url, json=log_entry)
        response.raise_for_status()

    def send_metric_table_to_loki(self, metric_name: str, metric_info: dict):
        metric_labels = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "job": f"{metric_name}"}
        metric_labels.update(metric_info)
        # log_line = self.dict_to_log_line(metric_info)
        log_entry = self.create_log_entry(metric_labels)
        response = requests.post(self.loki_url, json=log_entry)
        response.raise_for_status()


class ShowServerStatus(LokiInit):
    METRIC = 'show_server_status'

    @staticmethod
    def get_metric(metric_data: tuple[str | int]) -> dict[str, int | str]:
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


class ShowLocks(LokiInit):
    METRIC = 'show_locks'

    @staticmethod
    def get_metric(metric_data: tuple) -> dict:
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


class GetLeveldbStats(LokiInit):
    METRIC = 'get_leveldb_stats'

    @staticmethod
    def get_metric(metric_data: tuple) -> dict:
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


class ShowClusterNodes(LokiInit):
    METRIC = 'show_cluster_nodes'

    @staticmethod
    def get_metric(metric_data: tuple) -> dict:
        show_cluster_nodes_output = {
            "server_ip": f"{metric_data[0]}",
            "server_port": f"{metric_data[1]}",
            "connection_id": f"{metric_data[2]}",
            "instance_id": f"{metric_data[3]}",
            "last_heartbeat": f"{metric_data[4]}",
            "connection_status": f"{metric_data[5]}",
        }
        return show_cluster_nodes_output


class GetLicenseInfo(LokiInit):
    METRIC = 'get_license_info'

    @staticmethod
    def get_metric(metric_data: tuple) -> dict:
        license_info_output = {
            "compressed_cluster_size": f"{metric_data[0]}",
            "uncompressed_cluster_size": f"{metric_data[1]}",
            "compress_type": f"{metric_data[2]}",
            "cluster_size_limit": f"{metric_data[3]}",
            "expiration_date": f"{metric_data[4]}",
            "is_date_expired": f"{metric_data[5]}",
            "is_size_exceeded": f"{metric_data[6]}",
            "cluster_size_left": f"{metric_data[7]}",
            "data_read_size_limit": f"{metric_data[8]}",
            "data_write_size_limit": f"{metric_data[9]}",
            "gpu_limit": f"{metric_data[10]}",
        }
        return license_info_output
    