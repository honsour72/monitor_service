#!/usr/bin/env python
import os
import time
import json
import threading
import inspect
import argparse
import logging
from typing import Literal, List, Union
from datetime import datetime
import pysqream
from colorama import Fore, Style
from loki_interface import LokiInit


sq_conn = ""
m_log_level_color = {
    'DEBUG': Fore.BLUE,
    'INFO': Fore.WHITE,
    'WARNING': Fore.YELLOW,
    'ERROR': Fore.RED,
    'SUCCEED': Fore.GREEN,
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
            conn = pysqream.connect(
                host=self.ip,
                port=self.port,
                database=self.database, 
                username=self.user, 
                password=self.password, 
                clustered=self.clustered,
                service=self.service
            )

            return conn
        except Exception as e:
            log("ERROR", f"Unable to connect to Sqream: {str(e)}")
        
    def fetchall(self, cur, i_metric: str) -> List[Union[tuple, None]]:
        try:
            cur.execute(f"select {i_metric}()")
            result = cur.fetchall()
            if result and i_metric in m_escaping_character_metrics.keys():
                for row_idx, row in enumerate(result):
                    for col_idx, col in enumerate(row):
                        if col_idx in m_escaping_character_metrics[i_metric]:
                            # print("-- ESCAPE POSITION:",str(col_idx))
                            tmp_lst = list(row)
                            tmp_lst[col_idx] = row[col_idx].replace('\"', "\'").replace("\'", "\'\'")
                            result[row_idx] = tuple(tmp_lst)
            return result
        except Exception as e:
            log("ERROR",f"Unable to fetch from Sqream: {str(e)}")
        finally:
            cur.close()


def set_logger():
    logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def log(i_log_level: str, i_log_message: str):
    logger = logging.getLogger()
    if i_log_level not in m_log_level_color:
        raise ValueError(f'{i_log_level} is not a valid log level')

    calling_function = inspect.stack()[1][3].upper()
    log_message = f'{calling_function} - {i_log_message}'
    colored_message = f'{m_log_level_color[i_log_level]}{log_message}{Style.RESET_ALL}'

    if i_log_level == 'DEBUG':
        logger.debug(colored_message)
    elif i_log_level == 'INFO':
        logger.info(colored_message)
    elif i_log_level == 'WARNING':
        logger.warning(colored_message)
    elif i_log_level == 'ERROR':
        logger.error(colored_message)
    elif i_log_level == 'SUCCEED':
        logger.info(colored_message)


def set_and_get_arguments():
    """
    Sqream connection parameters include:
    * IP/Hostname
    * Port
    * database name
    * username
    * password 
    * Connect through load balancer, or direct to worker (Default: false - direct to worker)
    * use SSL connection (default: false)
    * Optional service queue (default: 'sqream')

    Loki connection parameters:
    * remote_ip 
    """
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--sqream_ip', type=str, help='Specify Sqream ip address', default='192.168.4.25')
    parser.add_argument('--sqream_port', type=int, help='Specify Sqream port', default=5000)                    
    parser.add_argument('--sqream_database', type=str, help='Specify Sqream database', default='master')                    
    parser.add_argument('--sqream_user', type=str, help='Specify Sqream user', default='sqream')                     
    parser.add_argument('--sqream_password', type=str, help='Specify Sqream password', default='sqream')                     
    parser.add_argument('--sqream_clustered', type=bool, help='Specify Sqream clustered', default=False)                        
    parser.add_argument('--sqream_service', type=str, help='Specify Sqream service (Default: \'monitor\')', default='monitor')                        
    parser.add_argument('--remote_ip', type=str, help='Specify Loki remote ip address', default='127.0.0.1')
    parser.add_argument('--remote_port', type=int, help='Specify Loki remote port', default="3100")
                        
    return parser.parse_args()


def send_info_to_loki(sq_instance, loki_instance, metric_name: str, metric_execution_time: int):
        monitor_metric = loki_instance.METRICS[metric_name]
        sq_conn = sq_instance.connect()
        sq_cur = sq_conn.cursor()
        info_about_metric = sq_instance.fetchall(sq_cur, metric_name)
       
        while True:
            log("INFO", f"{metric_name} - METRIC STARTED")
            for m_info in info_about_metric:
                loki_instance.send_metric_table_to_loki(metric_name, monitor_metric.get_metric_tuple(m_info))
            log("SUCCEED", f"{metric_name} - METRIC ENDED")   
            time.sleep(metric_execution_time)


def monitor_service_manager(args, config_monitor_file: dict):
    loki_url = f"http://{args.remote_ip}:{args.remote_port}/loki/api/v1/push"
    sq_instance = SqInit(args)
    loki_instance = LokiInit(loki_url)
    unsupported_keys = [key for key in loki_instance.METRICS if key not in list(config_monitor_file.keys())]
    assert not unsupported_keys, f"Unsupported keys found: {unsupported_keys}"
    for metric_name, metric_execution_time in config_monitor_file.items():
        job_thread = threading.Thread(target=send_info_to_loki, 
                                      args=(sq_instance, loki_instance, metric_name, metric_execution_time), 
                                      name=metric_name + "_thread")
        job_thread.start()


def main():
    set_logger()
    args = set_and_get_arguments()
    log("INFO", f"{args}")
    config_monitor_file: dict = json.load(open(f"{os.getcwd()}/monitor_input.json"))
    monitor_service_manager(args, config_monitor_file)


if __name__ == '__main__':
    log("INFO", 'monitor service started')
    main()
