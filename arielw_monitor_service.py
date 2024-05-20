#!/usr/bin/env python
import pysqream
import psycopg2
from datetime import datetime
import json
import threading
import time
from colorama import Fore
import inspect
import argparse
import os
from prometheus_client import start_http_server, Counter

sq_conn = ""

m_log_level_color = {
    'INFO': Fore.WHITE,
    'WARN': Fore.YELLOW,
    'ERROR': Fore.RED,
    'SUCCESS': Fore.GREEN
}

m_types = {
    int: 'INT',
    float: 'FLOAT',
    str: 'TEXT',
    datetime: 'TIMESTAMP9'  # Timestamp with milliseconds
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
        self.ip = args['sqream_ip']
        self.port = args['sqream_port']
        self.database = args['sqream_database']
        self.user = args['sqream_user']
        self.password = args['sqream_password']
        self.clustered = args['sqream_clustered']
        self.service = args['sqream_service']
        print(self.ip)

    def connect(self):
        # print(Fore.WHITE, "SQConnection OPENED")
        try:
            conn = pysqream.connect(host=self.ip, port=self.port, database=self.database
                                    , username=self.user, password=self.password
                                    , clustered=self.clustered, service=self.service)
            return conn
        except Exception as e:
            raise Exception(Fore.RED, f"Unable to connect to Sqream: {str(e)}")


class PgInit:
    def __init__(self, args):
        self.server = args['remote_server']
        self.ip = args['remote_ip']
        self.port = args['remote_port']
        self.database = args['remote_database']
        self.user = args['remote_user']
        self.password = args['remote_password']

    def connect(self):
        # print(Fore.WHITE, "PGConnection OPENED")
        try:
            conn = psycopg2.connect(user=self.user,
                                    password=self.password,
                                    host=self.ip,
                                    port=self.port,
                                    database=self.database)
            return conn
        except Exception as e:
            raise Exception(Fore.RED, f"Unable to connect to Postgres: {str(e)}")

    def create_if_not_exist(self, i_table):
        pg_conn = self.connect()
        cur = pg_conn.cursor()
        table_columns = data_sources[i_table]
        try:
            lst = []
            for x in table_columns:
                pg_col = x + " " + m_types[type(x)]
                lst.append(pg_col)
            stmt = "create table " + i_table + "( metric_time timestamp, " + ','.join([x for x in lst]) + ");"
            cur.execute(stmt)
            pg_conn.commit()
        except Exception as e:
            log('WARN', str("Could not create table " + i_table + ". " + str(e)))
        finally:
            pg_conn.close()


class Deleter:
    def __init__(self, args):
        self.frequency = args['deleter_freq']
        self.kept_data = args['deleter_kept_data']
        run_deleter(self.frequency, self.kept_data)


def log(i_log_level, i_log_message):
    if i_log_level not in m_log_level_color.keys():
        raise Exception(i_log_level + ' is not a valid log level')
    print(m_log_level_color[i_log_level], ','.join(
        (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), i_log_level, inspect.stack()[1][3].upper(), i_log_message)))


def fetchall(cur, i_metric):
    try:
        cur.execute(f"select {i_metric}()")
        result = cur.fetchall()
        print(result)
        if len(result) > 0 and i_metric in m_escaping_character_metrics.keys():
            for row_idx, row in enumerate(result):
                for col_idx, col in enumerate(row):
                    if col_idx in m_escaping_character_metrics[i_metric]:
                        # print("-- ESCAPE POSITION:",str(col_idx))
                        tmp_lst = list(row)
                        tmp_lst[col_idx] = row[col_idx].replace("\"", "\'").replace("\'", "\'\'")
                        result[row_idx] = tuple(tmp_lst)
        return result
    except Exception as e:
        raise Exception(f"Unable to fetch from Sqream: {str(e)}")


def pg_monitor(sq_instance, pg_instance, i_table):
    try:
        sq_conn = sq_instance.connect()
        pg_conn = pg_instance.connect()
        pq_cur = pg_conn.cursor()
        sq_cur = sq_conn.cursor()
        rows = fetchall(sq_cur, i_table)
        print(i_table,": ",rows)
        if len(rows) > 0:
            rows = [(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],) + row for row in rows]
            res = ','.join(f"{x}" for x in rows).replace("\"", "\'")
            postgres_insert_query = f"INSERT INTO {i_table} VALUES " + res + ";"
            pq_cur.execute(postgres_insert_query)
            pg_conn.commit()
            count = pq_cur.rowcount
            log("SUCCESS", str(f"{i_table} - {count} Records INSERTED successfully !"))
        else:
            log("INFO", str(f"{i_table} - 0 Records"))
        pg_conn.close()
        sq_conn.close()
    except Exception as e:
        if not sq_conn.cur_closed:
            sq_conn.close()
        if pg_conn.closed:
            pg_conn.close()
        raise Exception(f"Unable to fill '{i_table}'. Error: " + str(e))


def metric_scheduler(sq_instance, pg_instance, i_metric_table_name, i_freq_sec, dest):
    log("INFO", f"{i_metric_table_name} - JOB STARTED")
    if dest == "prom":
        #  PrometheusInit.set_up_prometheus()
         while True:
            time.sleep(i_freq_sec)
            log("INFO", f"{i_metric_table_name} - METRIC STARTED")
            # PrometheusInit.send_metrices(sq_instance, i_metric_table_name)
            log("INFO", f"{i_metric_table_name} - METRIC ENDED")
    else:
        pg_instance.create_if_not_exist(i_metric_table_name)
        while True:
            time.sleep(i_freq_sec)
            log("INFO", f"{i_metric_table_name} - METRIC STARTED")
            pg_monitor(sq_instance, pg_instance, i_metric_table_name)
            log("INFO", f"{i_metric_table_name} - METRIC ENDED")


def run_metric_scheduler(sq_instance, pg_instance, i_metric_name, i_freq_sec, dest):
    job_thread = threading.Thread(target=metric_scheduler, args=(sq_instance, pg_instance, i_metric_name, i_freq_sec, dest,),
                                  name=i_metric_name + "_thread")
    job_thread.start()


def metric_deleter(pg_instance, i_freq_sec, i_kept_data_in_sec):
    log("INFO", "JOB STARTED")
    while True:
        time.sleep(i_freq_sec)
        for metric_table_name in data_sources.keys():
            try:
                log("INFO", f"{metric_table_name} - STARTED")
                pg_conn = pg_instance.connect()
                pg_cur = pg_conn.cursor()
                delete_stmt = (f"delete from {metric_table_name} where metric_time < NOW()::timestamp - INTERVAL '"
                               f"{str(i_kept_data_in_sec)} seconds'")
                pg_cur.execute(delete_stmt)
                deleted_res = pg_cur.rowcount
                log_level = "SUCCESS" if deleted_res > 0 else "INFO"
                log(log_level, f"{metric_table_name} - {str(deleted_res)} records DELETED successfully !")
                pg_conn.commit()
                log("INFO", f"{metric_table_name} - ENDED")
                pg_conn.close()
            except Exception as e:
                log("ERROR", f"Unable to delete data from table: {metric_table_name}. Error: " + str(e))
                if pg_conn.closed:
                    pg_conn.close()


def run_deleter(i_freq_sec, i_kept_data_sec, dest):
    job_thread = threading.Thread(target=metric_deleter, args=(pg_instance, i_freq_sec, i_kept_data_sec, dest,),
                                  name="metric_deleter")
    job_thread.start()


def timeout(i_sec):
    for i in range(i_sec):
        time.sleep(1)


""""------------------------------------------------------------"""
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
parser.add_argument('--remote_server', metavar='remote_server', type=str, nargs='?', help='Remote server (pgsql/mysql)',
                    default='pgsql')
parser.add_argument('--remote_ip', metavar='remote_ip', type=str, nargs='?', help='Remote IP address',
                    default='192.168.1.12')
parser.add_argument('--remote_port', metavar='remote_port', type=int, nargs='?', help='Remote Port',
                     default=5432)
parser.add_argument('--remote_database', metavar='remote_database', type=str, nargs='?', help='monitor_service',
                    default='monitor_service')
parser.add_argument('--remote_user', metavar='remote_user', type=str, nargs='?', help='Remote user',
                     default='postgres')
parser.add_argument('--remote_password', metavar='remote_password', type=str, nargs='?', help='Remote password',
                    default='postgres11')
parser.add_argument('--deleter_freq', metavar='deleter_freq', type=int, nargs='?', help='Deleter frequency (seconds)',
                    default=10)
parser.add_argument('--deleter_kept_data', metavar='deleter_kept_data', type=int, nargs='?',
                    help='Deleter keep data of X seconds', default=30000)
# parser.add_argument('--dest', metavar='destination source', help='set destination to send data to', required=True)

# parser.add_arFgument('--timeout', metavar='timeout', type=int, nargs='?', help='Monitor service timeout', default=0)

args = parser.parse_args()
print(args)
# dest = args.dest.lower()

monitoring_tables = ["show_server_status", "show_locks", "get_leveldb_stats"]
monitor_input = json.load(open(f"sqreamdb-monitor-service/monitor_input.json"))
# pg_instance = PgInit(args.__dict__)
sq_instance = SqInit(args.__dict__)
# deleter = Deleter(args.__dict__)

# for metric in monitor_input.keys():
#     print(metric)
#     if metric not in monitoring_tables:
#         log("WARN", f"SCHEDULER - Metric '{metric}' not found in monitoring tables, probably wrong input")
#         continue
#     run_metric_scheduler(sq_instance, pg_instance, metric, monitor_input[metric], dest)
sq_conn = sq_instance.connect()
sq_cur = sq_conn.cursor()
rows = fetchall(sq_cur, "show_server_status")
print(rows)
# for row in rows:
#     print(row)
#     for r in row:
#         print(r)