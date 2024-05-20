import requests
import time
from datetime import datetime
import json

# Loki configuration
LOKI_URL = 'http://192.168.4.99:3100/loki/api/v1/push'

"""
monitor
node_9383
3
192.168.4.25
5000
master
sqream
192.168.4.25
178
select show_server_status()
20-05-2024 17:45:54
Executing
20-05-2024 17:45:54
"""
data_sources = {
    "show_server_status": [
        {
            "service": "service1",
            "instance_id": "instance1",
            "connection_id": 1,
            "server_ip": "192.168.1.1",
            "server_port": 5432,
            "database_name": "db1",
            "user_name": "user1",
            "client_ip": "192.168.1.100",
            "statement_id": 1001,
            "statement": "SELECT * FROM table1",
            "statement_start_time": "2024-05-20T12:00:00Z",
            "statement_status": "running",
            "statement_status_start": "2024-05-20T12:00:01Z"
        }
    ],
    "show_locks": [
        {
            "statement_id": "stmt1",
            "statement_string": "SELECT * FROM table2",
            "username": "user2",
            "server": "192.168.1.2",
            "port": "5432",
            "locked_object": "table2",
            "lock_mode": "exclusive",
            "statement_start_time": "2024-05-20T12:01:00Z",
            "lock_start_time": "2024-05-20T12:01:01Z"
        }
    ],
    "get_leveldb_stats": [
        {
            "timestamp": "2024-05-20T12:02:00Z",
            "server_ip": "192.168.1.3",
            "server_port": 6000,
            "msg": "leveldb stats",
            "count": 10,
            "average": 5.5,
            "max": 10.0,
            "max_timestamp": "2024-05-20T12:02:01Z",
            "variance": 2.0
        }
    ]
}

def get_server_status(metric_list: tuple):
    server_status_output = "get_leveldb_stats"
    {
            "timestamp": f"{metric_list['timestamp']}",
            "server_ip": f"{metric_list['server_ip']}",
            "server_port": f"{metric_list['server_port']}",
            "msg": f"{metric_list['msg']}",
            "count": f"{metric_list['count']}",
            "average": f"{metric_list['average']}",
            "max": f"{metric_list['max']}",
            "max_timestamp": f"{metric_list['max_timestamp']}",
            "variance": f"{metric_list['variance']}",
    }
    return server_status_output

def create_log_entry(label, log_line):
    """Create a log entry for Loki."""
    timestamp = int(time.time() * 1e9)  # Convert to nanoseconds
    return {
        'streams': [
            {
                'stream': label,
                'values': [
                    [str(timestamp), log_line]
                ]
            }
        ]
    }

def push_logs_to_loki(log_entry):
    """Push logs to Loki."""
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(LOKI_URL, json=log_entry, headers=headers)
    response.raise_for_status()
    print('Successfully pushed logs to Loki.')

def dict_to_log_line(data):
    """Convert a dictionary to a formatted log line."""
    return json.dumps(data)

def main():
    # for table_name, rows in data_sources.items():
    #     print(table_name)
    #     print(rows)
    for table_name, rows in data_sources.items():
        for row in rows:
            label = {'job': table_name}
            print(f"{label}\n")
            log_line = dict_to_log_line(row)
            print(f"{log_line}\n")
            log_entry = create_log_entry(label, log_line)
            print(f"{log_entry}\n")
            push_logs_to_loki(log_entry)
            time.sleep(1)  # Sleep to simulate log generation interval

if __name__ == '__main__':
    main()
    # while True:
    #     main()
    #     time.sleep(10)




