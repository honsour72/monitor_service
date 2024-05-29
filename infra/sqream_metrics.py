from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ShowServerStatus:
    service: str
    instance_id: str
    connection_id: int
    server_ip: str
    server_port: int
    database_name: str
    user_name: str
    client_ip: str
    statement_id: int
    statement: str
    statement_start_time: str
    statement_status: str
    statement_status_start: str
    job: str = "show_server_status"


@dataclass
class ShowLocks:
    statement_id: str
    statement_string: str
    username: str
    server: str
    port: str
    locked_object: str
    lock: str
    mode: str
    statement_start_time: str
    lock_start_time: str
    job: str = "show_locks"


@dataclass
class GetLevelDbStats:
    ts: str
    server_ip: str
    server_port: int
    msg: str
    m_count: float
    m_average: float
    m_max: float
    m_max_timestamp: str
    m_variance: float
    job: str = "get_leveldb_stats"


@dataclass
class ShowClusterNodes:
    server_ip: str
    server_port: int
    connection_id: int
    instance_id: str
    last_heartbeat: int
    connection_status: str
    job: str = "show_cluster_nodes"


@dataclass
class GetLicenseInfo:
    compressed_cluster_size: str
    uncompressed_cluster_size: str
    compress_type: str
    cluster_size_limit: str
    expiration_date: str
    is_date_expired: str
    is_size_exceeded: str
    cluster_size_left: str
    data_read_size_limit: str
    data_write_size_limit: str
    gpu_limit: str
    job: str = "get_license_info"
