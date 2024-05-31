from __future__ import annotations

from typing import Literal

import pysqream
from pysqream.connection import Connection


class SqreamConnection:
    """Representation of sqream connection class."""

    connection: Connection | None = None

    def __new__(cls, host: str, port: int, database: str, user: str, password: str, clustered: bool, service: str):
        if cls.connection is None:
            cls.connection = pysqream.connect(host=host, port=port, database=database, username=user, password=password,
                                              clustered=clustered, service=service)
        return cls

    @staticmethod
    def execute(query: str, fetch: Literal["one", "all"] = "all") -> list[dict[str, int | str]] | dict[str | int]:
        """:param query: sqream query to execute
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
        with SqreamConnection.connection.cursor() as cursor:
            cursor.execute(query)
            if fetch == "one":
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()

        if result is None:
            return []

        if fetch == "one":
            return {col_name.replace(" ", "_"): value for col_name, value in zip(cursor.col_names, result)}

        return [{col_name.replace(" ", "_"): value for col_name, value in zip(cursor.col_names, row)} for row in result]

    @staticmethod
    def close() -> None:
        if SqreamConnection.connection is not None and not SqreamConnection.connection.con_closed:
            SqreamConnection.connection.close_connection()
