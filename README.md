# sqreamdb-monitor-service

![Static Badge](https://img.shields.io/badge/colorama-0.4.6-darkblue)
![Static Badge](https://img.shields.io/badge/numpy-1.26.4-blue)
![Static Badge](https://img.shields.io/badge/psycopg2-2.9.9-orange)
![Static Badge](https://img.shields.io/badge/pyarrow-16.1.0-red)
![Static Badge](https://img.shields.io/badge/pysqream-5.0.0-yellow)

Python implementation of Sqreamdb monitor service.

## How to configure project

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

## How to trigger Monitor service:
1. Start monitor worker (no-GPU resource):
  * cd <PACKAGE>
  * bin/metadata_server
  * bin/sqreamd <CLUSTER> 0 5000 ~/.sqream/license.enc -config <MONITOR_SERVICE_CLONE>/config_files/sqream_config.json &

2. Trigger Monitor Service:
  * cd <MONITOR_SERVICE_CLONE>
  * python3.9 monitor_service.py


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


## Next steps

1) Expansion: add utility functions scrapping `show_cluster_nodes`, `get_license_info`
2) Store information in Prometheus instead of postgres
3) Integrate monitor service with TI alike