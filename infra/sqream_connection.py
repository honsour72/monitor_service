from __future__ import annotations

from typing import Literal

import pysqream
from pysqream.connection import Connection

from infra.utils import timeit


class SqreamConnection:
    """Representation of sqream connection class."""

    host: str | None = None
    port: int | None = None
    username: str | None = None
    password: str | None = None
    database: str | None = None
    clustered: bool | None = None
    service: str | None = None
    connection: Connection | None = None

    def __init__(self, host: str, port: int, database: str, username: str, password: str, clustered: bool, service: str):
        if self.connection is None:
            self.connection = pysqream.connect(host=host, port=port, database=database, username=username,
                                               password=password, clustered=clustered, service=service)

    def execute(self,
                query: str,
                fetch: Literal["one", "all"] = "all"
                ) -> tuple[list[dict[str, int | str]], float] | tuple[dict[str | int], float]:
        """:param query: sqream query to execute, e.g. `select show_locks()`
        :param fetch: possible way to get rows: `all` or `one`. Default - `all`
        :return: list of dicts (many rows) - for fetchall, dict (one row) - for fetchone

        Column names will be captured from cursor's `col_names` attribute

        Examples of return:
        1) For fetchone:
        { "server_ip": "127.0.0.1", "server_port": 5000, ... "statement_id": "node_6999" }

        2) For fetchall:
        [
            { "write_limit": "123", "read_limit": "321", ... "license_info": "some text" },
            ...
            { "write_limit": "456", "read_limit": "654", ... "license_info": "other text" },
        ]

        3) For some strange reason `cursor.fetchone()` sometimes can return None.
        Method will return empty list in that case

        Note:
        ----
        For some strange reasons Loki can not receive http post request body data with spaces. For example, this data
        {"key name": "key value"}
        will not be handled (Response is 400: Bad request) - and this:
        {"key_name": "key_value"}
        will be handled

        For this reason I use `replace(" ", "_")` to change spaces on underscore sign before result

        """
        with timeit() as elapsed_time:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                if fetch == "one":
                    result = cursor.fetchone()
                else:
                    result = cursor.fetchall()

            if result is None:
                return [], elapsed_time()

            if fetch == "one":
                return ({col_name.replace(" ", "_"): value for col_name, value in zip(cursor.col_names, result)},
                        elapsed_time())

            return [{col_name.replace(" ", "_"): value for col_name, value in zip(cursor.col_names, row)}
                    for row in result], elapsed_time()

    def close(self) -> None:
        if self.connection is not None and not self.connection.con_closed:
            self.connection.close_connection()
