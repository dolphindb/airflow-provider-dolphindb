#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from typing import Sequence

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


class DolphinDBOperator(SQLExecuteQueryOperator):
    """
    Executes sql code in a specific DolphinDB database.

    :param sql: the dolphindb script code to be executed. Can receive a str representing a
        sql statement, a list of str (sql statements).
    :param dolphindb_conn_id: reference to a specific DolphinDB database.
    :param file_path: dolphindb script file(.dos) path.
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    template_fields: Sequence[str] = ("sql",)
    template_fields_renderers = {"sql": "dolphindbsql"}
    template_ext: Sequence[str] = (".dos",)
    ui_color = "#ededed"

    def __init__(
        self, *, dolphindb_conn_id: str = "dolphindb_default", file_path: str | None = None, **kwargs
    ) -> None:
        if file_path is not None:
            with open(file_path) as f:
                sql = f.read()
            kwargs["sql"] = sql
        super().__init__(conn_id=dolphindb_conn_id, **kwargs)
