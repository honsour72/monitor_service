#!/usr/bin/env python
import os
import time
import json
import threading
import argparse
from typing import List, Union
import logging
import pysqream
import colorlog
from loki_interface import LokiInit
from pysqream.connection import Connection
from pysqream.cursor import Cursor


class SqInit:
    def __init__(self, args: argparse.Namespace):
        self.ip = args.sqream_ip
        self.port = args.sqream_port
        self.database = args.sqream_database
        self.user = args.sqream_user
        self.password = args.sqream_password
        self.clustered = args.sqream_clustered
        self.service = args.sqream_service

    def connect(self) -> type[Connection]:
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
            logging.error( f"Unable to connect to Sqream: {str(e)}")
        
    def fetchall(self, cur: type[Cursor], metric: str) -> List[Union[tuple, None]]: 
        result = []
        try:
            cur.execute(f"select {metric}()")
            result = cur.fetchall()
        except Exception as e:
            logging.error(f"Unable to fetch from Sqream: {str(e)}")
        finally:
            cur.close()
            return result


def set_logger():
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'white',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        }
    ))

    logger = colorlog.getLogger()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def set_and_get_arguments() -> argparse.Namespace:
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
    parser.add_argument('--sqream_clustered', action='store_true' ,help='Specify Sqream clustered')                        
    parser.add_argument('--sqream_service', type=str, help='Specify Sqream service (Default: \'monitor\')', default='monitor')                        
    parser.add_argument('--remote_ip', type=str, help='Specify Loki remote ip address', default='127.0.0.1')
    parser.add_argument('--remote_port', type=int, help='Specify Loki remote port', default="3100")
                        
    return parser.parse_args()


def send_info_to_loki(sq_instance: type[SqInit], loki_instance: type[LokiInit], metric_name: str, metric_execution_time: int):
    monitor_metric = loki_instance.METRICS[metric_name]
    sq_conn = sq_instance.connect()
    sq_cur = sq_conn.cursor()
    info_about_metric = sq_instance.fetchall(sq_cur, metric_name)

    while True:
        logging.info(f"{metric_name} - METRIC STARTED - {len(info_about_metric)} ROWS FOUND")
        for m_info in info_about_metric:
            loki_instance.send_metric_table_to_loki(metric_name, monitor_metric.get_metric_tuple(m_info))  
        logging.debug(f"SUCCEED - {metric_name} - METRIC ENDED - INSERTED SUCCSESFULLY")  
        time.sleep(metric_execution_time)


def monitor_service_manager(args: argparse.Namespace, config_monitor_file: dict):
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
    logging.warning('monitor service started')
    args = set_and_get_arguments()
    logging.info(f"{args}")
    config_monitor_file: dict = json.load(open(f"{os.getcwd()}/monitor_input.json"))
    monitor_service_manager(args, config_monitor_file)


if __name__ == '__main__':
    main()
