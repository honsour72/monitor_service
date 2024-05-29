# sqreamdb-monitor-service

![Static Badge](https://img.shields.io/badge/colorama-0.4.6-darkblue)
![Static Badge](https://img.shields.io/badge/numpy-1.26.4-blue)
![Static Badge](https://img.shields.io/badge/psycopg2-2.9.9-orange)
![Static Badge](https://img.shields.io/badge/pyarrow-16.1.0-red)
![Static Badge](https://img.shields.io/badge/pysqream-5.0.0-yellow)

Python implementation of SQreamDB monitor service.

## How to configure project environment

Python version: 3.9

1. Create virtual environment
```commandline
python3.9 -m venv .venv
```

2. Activate virtual environment
```commandline
. .venv/bin/activate
```

3. Install requirements 
```commandline
pip install -r requirements.txt
```

## How to trigger Monitor service

### 1. Sqream worker configuration examples

`sqream_config.json`

```json
{
    "cudaMemQuota":0,
    "gpu": 0,
    "legacyConfigFilePath": "sqream_config_legacy.json",
    "metadataServerIp": "127.0.0.1",
    "metadataServerPort": 3105,
    "port": 5000,
    "useConfigIP": true,
    "limitQueryMemoryGB" : 8,
    "initialSubscribedServices": "monitor"
}
```

`sqream_config_legacy.json`

```json
{
    "debugNetworkSession": false,
    "developerMode": true,
    "diskSpaceMinFreePercent": 1,
    "enableLogDebug": true,
    "insertCompressors": 8,
    "insertParsers": 8,
    "nodeInfoLoggingSec": 0,
    "reextentUse": false,
    "showFullExceptionInfo": true,
    "showInternalExceptionInfo": false,
    "useClientLog": true,
    "useMetadataServer": true,
    "spoolMemoryGB" : 4,
    "clientReconnectionTimeout": 10000,
    "liveConnectionThreshold": 100000
}
```

### 2. Start monitor worker (no-GPU resource)

1) Go to sqream package directory

```commandline
cd <sqream_package_dir>
```

2) Run metadata_server in background

```commandline
bin/metadata_server &
```

3) Run sqreamd worker in background

```commandline
bin/sqreamd -config <monitor_service_root_dir>/config_files/sqream_config.json &
```

### 3. Run Monitor Service

1) Go to monitor service root directory

```commandline
cd <monitor_service_root_dir>
```

2) Configure `monitor_input.json` if you need

```json
{
  "show_server_status": 7,
  "show_locks": 2,
  "get_leveldb_stats": 5,
  "show_cluster_nodes": 4,
  "get_license_info": 5
}
```

3) Run monitor service

```commandline
python monitor_service.py
```


## Graph (for better understanding what's happening)

```mermaid
graph LR
A(__main__) -->|First metric| B1[show_server_status]
A(__main__) -->|Second metric| B2[show_locks]
A(__main__) -->B3[...]
A(__main__) -->|N-th metric| B4[get_leveldb_stats]
B1 --> C(run_metric_scheduler)
B2 --> C(run_metric_scheduler)
B3 --> C(run_metric_scheduler)
B4 --> C(run_metric_scheduler)
C --> D(Thread.start
        metric_scheduler)
D --> E(while True:)
E --> F(pg_monitor)
F --> E
```

## Useful links:

* [SQreamDB documentation](https://docs.sqream.com/en/latest/)