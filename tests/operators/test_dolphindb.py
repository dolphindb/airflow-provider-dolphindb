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

from unittest import mock

from datetime import datetime
import pytest

from airflow.models.dag import DAG
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow_provider_dolphindb.hooks.dolphindb import DolphinDBHook
from airflow_provider_dolphindb.operators.dolphindb import DolphinDBOperator

TEST_DAG_ID = "unit_test_dag"


class TestDolphinDBOperator:

    def setup_method(self):
        dag = DAG(TEST_DAG_ID, start_date=datetime(2023, 8, 1))
        self.dag = dag
        self.sql = "test_sql"

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_execute_params(self, mock_get_db_hook):
        sql = "select * from test_table"
        dolphindb_conn_id = DolphinDBHook.default_conn_name
        parameters = {"parameter": "value"}
        context = "test_context"
        autocommit = False
        task_id = "test_task_id"

        operator = DolphinDBOperator(
            sql=sql,
            dolphindb_conn_id=dolphindb_conn_id,
            parameters=parameters,
            task_id=task_id,
            dag = self.dag
        )

        operator.execute(context=context)
        assert operator.get_dag() == self.dag
        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql=sql,
            parameters=parameters,
            handler=fetch_all_handler,
            autocommit=autocommit,
            return_last=True,
        )

    @mock.patch.object(DolphinDBHook, "run", autospec=DolphinDBHook.run)
    def test_execute_file(self, mock_run):
        file_path = "/data/sql.dos"
        paras = {"parameter": "value"}

        with mock.patch("builtins.open", mock.mock_open(read_data=self.sql)) as mock_file:
            op = DolphinDBOperator(
                task_id="dolphindb_task", file_path=file_path, dag=self.dag, parameters=paras, dolphindb_conn_id=DolphinDBHook.default_conn_name)
            assert open(file_path).read() == self.sql
            mock_file.assert_called_with(file_path)

        result = op.execute(context="test_context")
        assert result is mock_run.return_value
        mock_run.assert_called_once_with(
            mock.ANY,
            sql=self.sql,
            autocommit=False,
            parameters=paras,
            handler=fetch_all_handler,
            return_last=True
        )

    @mock.patch.object(DolphinDBHook, "run", autospec=DolphinDBHook.run)
    def test_execute_sql(self, mock_run):
        sql = "test_sql_2"
        paras = {"parameter": "value"}
        op = DolphinDBOperator(
            task_id="dolphindb_task", sql=sql, dag=self.dag, parameters=paras, dolphindb_conn_id=DolphinDBHook.default_conn_name)
        
        result = op.execute(context="test_context")
        assert result is mock_run.return_value
        mock_run.assert_called_once_with(
            mock.ANY,
            sql=sql,
            autocommit=False,
            parameters=paras,
            handler=fetch_all_handler,
            return_last=True
        )

    @mock.patch.object(DolphinDBHook, "run", autospec=DolphinDBHook.run)
    def test_execute_multi_sqls(self, mock_run:mock.Mock):
        sqls = [
            "select * from defs()",
            "select * from objs()",
            "select * from table(1 2 3 as c1, 4 5 6 as c2)",
        ]
        op = DolphinDBOperator(
            task_id="execute_multi_sqls",
            sql=sqls,
            dag=self.dag,
            dolphindb_conn_id=DolphinDBHook.default_conn_name
        )
        result = op.execute(context="test_context")
        assert result is mock_run.return_value
        mock_run.assert_called_once_with(
            mock.ANY,
            sql=sqls,
            parameters=None,
            autocommit=False,
            handler=fetch_all_handler,
            return_last=True
        )

