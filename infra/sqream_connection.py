from __future__ import annotations

from typing import Literal

import pysqream
from pysqream.connection import Connection


class SqreamConnection:
    connection: Connection = None

    def __new__(cls, host: str, port: int, database: str, user: str, password: str, clustered: bool, service: str):
        if cls.connection is None:
            cls.connection = pysqream.connect(host=host, port=port, database=database, username=user, password=password,
                                              clustered=clustered, service=service)
        return cls

    @staticmethod
    def execute(query: str, fetch: Literal["one", "all"] = "all") -> list[tuple] | tuple[str | int]:
        """

        :param query:
        :param fetch:
        :return: list of tuples (many rows) - for fetchall, tuple with data (one row) - for fetchone

        Examples:
        For `select show_cluster_nodes()` query results will be:
        1) fetchall:
        [ ('127.0.0.1', 5000, 9,  'node_2777', 2, 'available'),
          ('127.0.0.1', 5000, 10, 'node_3888', 3, 'pending'),
          ('127.0.0.1', 5000, 11, 'node_4999', 4, 'busy')
        ]

        2) fetchone:
        ('127.0.0.1', 5000, 9, 'node_2777', 2, 'available')
        """
        with SqreamConnection.connection.cursor() as cursor:
            cursor.execute(query)
            if fetch == "one":
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()
        return result

    @staticmethod
    def close():
        if not SqreamConnection.connection.con_closed:
            SqreamConnection.connection.close_connection()
