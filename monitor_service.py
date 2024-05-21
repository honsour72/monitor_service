#!/usr/bin/env python
import os
import time
import pysqream
from datetime import datetime
import json
import threading
from colorama import Fore
import inspect
import argparse
from loki_interface import LokiInit

sq_conn = ""
loki_url = "" # Get defined by the user in the set_and_get_args function

m_log_level_color = {
    'INFO': Fore.WHITE,
    'WARN': Fore.YELLOW,
    'ERROR': Fore.RED,
    'SUCCESS': Fore.GREEN
}
data_sources = {
    "show_server_status": {
        "service": str,
        "instance_id": str,
        "connection_id": int,
        "server_ip": str,
        "server_port": int,
        "database_name": str,
        "user_name": str,
        "client_ip": str,
        "statement_id": int,
        "statement": str,
        "statement_start_time": str,
        "statement_status": str,
        "statement_status_start": str
    },
    "show_locks": {
        "statement_id": str,
        "statement_string": str,
        "username": str,
        "server": str,
        "port": str,
        "locked_object": str,
        "lock_mode": str,
        "statement_start_time": str,
        "lock_start_time": str
    },
    "get_leveldb_stats": {
        "timestamp": str,
        "server_ip": str,
        "server_port": int,
        "msg": str,
        "count": int,
        "average": float,
        "max": float,
        "max_timestamp": str,
        "variance": float
    }
}

m_escaping_character_metrics = {
    "show_server_status": [list(data_sources["show_server_status"]).index('statement')],
    "show_locks": [list(data_sources["show_locks"]).index('statement_string')]
}


class SqInit:
    def __init__(self, args):
        self.ip = args.sqream_ip
        self.port = args.sqream_port
        self.database = args.sqream_database
        self.user = args.sqream_user
        self.password = args.sqream_password
        self.clustered = args.sqream_clustered
        self.service = args.sqream_service

    def connect(self):
        try:
            conn = pysqream.connect(host=self.ip, port=self.port, database=self.database
                                    , username=self.user, password=self.password
                                    , clustered=self.clustered, service=self.service)
            return conn
        except Exception as e:
            raise Exception(Fore.RED, f"Unable to connect to Sqream: {str(e)}")
        
    def fetchall(self, cur, i_metric):
        try:
            cur.execute(f"select {i_metric}()")
            result = cur.fetchall()
            if len(result) > 0 and i_metric in m_escaping_character_metrics.keys():
                for row_idx, row in enumerate(result):
                    for col_idx, col in enumerate(row):
                        if col_idx in m_escaping_character_metrics[i_metric]:
                            # print("-- ESCAPE POSITION:",str(col_idx))
                            tmp_lst = list(row)
                            tmp_lst[col_idx] = row[col_idx].replace('\"', "\'").replace("\'", "\'\'")
                            result[row_idx] = tuple(tmp_lst)
            return result
        except Exception as e:
            raise Exception(f"Unable to fetch from Sqream: {str(e)}")
        finally:
            cur.close()


def log(i_log_level, i_log_message):
    if i_log_level not in m_log_level_color.keys():
        raise Exception(i_log_level + ' is not a valid log level')
    print(m_log_level_color[i_log_level], ','.join(
        (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), i_log_level, inspect.stack()[1][3].upper(), i_log_message)))


def set_and_get_arguments():
    """
    Connection parameters include:
    * IP/Hostname
    * Port
    * database name
    * username
    * password 
    * Connect through load balancer, or direct to worker (Default: false - direct to worker)
    * use SSL connection (default: false)
    * Optional service queue (default: 'sqream')
    """
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--sqream_ip', metavar='sqream_ip', type=str, nargs='?', help='Sqream IP address',
                        default='192.168.4.25')
    parser.add_argument('--sqream_port', metavar='sqream_port', type=int, nargs='+', help='Sqream Port',
                        default=5000)
    parser.add_argument('--sqream_database', metavar='sqream_database', type=str, nargs='?', help='Sqream Database',
                        default='master')
    parser.add_argument('--sqream_user', metavar='sqream_user', type=str, nargs='?', help='Sqream user',
                        default='sqream')
    parser.add_argument('--sqream_password', metavar='sqream_password', type=str, nargs='?', help='Sqream password',
                        default='sqream')
    parser.add_argument('--sqream_clustered', metavar='sqream_clustered', type=bool, nargs='?', help='Sqream clustered',
                        default=False)
    parser.add_argument('--sqream_service', metavar='sqream_service', type=str, nargs='?', help='Sqream service (Default: \'monitor\')',
                        default='monitor')
    parser.add_argument('--remote_ip', metavar='remote_ip', type=str, help='Loki remote IP address',
                        default='127.0.0.1')
    parser.add_argument('--remote_port', metavar='remote_port', type=int, help='Loki remote Port',
                        default="3100")
    return parser.parse_args()


def send_info_to_loki(sq_instance, metric_name: str, metric_execution_time: int):
        loki_instance = LokiInit(loki_url)
        monitor_metric = loki_instance.METRICS[metric_name]
        sq_conn = sq_instance.connect()
        sq_cur = sq_conn.cursor()
        info_about_metric = sq_instance.fetchall(sq_cur, metric_name)
       
        while True:
            log("INFO", f"{metric_name} - METRIC STARTED")
            for m_info in info_about_metric:
                loki_instance.send_metric_table_to_loki(metric_name, monitor_metric.get_metric_tuple(m_info))
            log("SUCCESS", f"{metric_name} - METRIC ENDED")    
            time.sleep(metric_execution_time)


def monitor_service_manager(args, config_monitor_file):
    sq_instance = SqInit(args)
    for metric_name, metric_execution_time in config_monitor_file.items():
        job_thread = threading.Thread(target=send_info_to_loki, args=(sq_instance, metric_name, metric_execution_time), name=metric_name + "_thread")
        job_thread.start()


def main():
    global loki_url

    args = set_and_get_arguments()
    print(args)
    loki_url = f"http://{args.remote_ip}:{args.remote_port}/loki/api/v1/push"
    config_monitor_file: tuple = json.load(open(f"{os.getcwd()}/monitor_input.json"))
    monitor_service_manager(args, config_monitor_file)

if __name__ == '__main__':
    log("INFO", 'monitor service started')
    main()
