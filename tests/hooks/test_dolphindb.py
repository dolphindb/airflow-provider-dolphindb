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

import json
from unittest import mock
import pytest

from airflow.models import Connection

try:
    from airflow_provider_dolphindb.hooks.dolphindb import DolphinDBHook

except ImportError:
    pytest.skip("DolphinDB not available", allow_module_level=True)


class TestDolphinDBHookConn:
    def setup_method(self):
        self.connection = Connection(
            conn_type="dolphindb",
            host="1.1.1.1",
            login="user",
            password="147258",
            port=9999
        )

        self.db_hook = DolphinDBHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection
        self.test_extras = [
            ["enableSSL", "true"],
            ["enableASYNC", "true"],
            ["keepAliveTime", "30"],
            ["enableChunkGranularityConfig", "true"],
            ["compress", "true"],
            ["enablePickle", "true"],
            ["protocol", "0"],
            ["python", "true"],
            ["startup", "login(`admin,`123456)"],
            ["highAvailability", "true"],
            ["highAvailabilitySites", "['192.168.0.1:8848','192.168.0.1:8849']"],
            ["reconnect", "true"],
            ["enableEncryption", "true"],
        ]

    @mock.patch("pydolphindb.connect")
    def test_get_conn(self, mock_connect: mock.Mock):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["username"] == "user"
        assert kwargs["password"] == "147258"
        assert kwargs["host"] == "1.1.1.1"
        assert kwargs["port"] == 9999

    @mock.patch("pydolphindb.connect")
    def test_get_uri(self, mock_connect: mock.Mock):
        self.connection.extra = json.dumps({"charset": "utf-8"})
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        assert self.db_hook.get_uri() == "dolphindb://user:147258@1.1.1.1:9999/?charset=utf-8"

    @mock.patch("pydolphindb.connect")
    def test_get_conn_default_port(self, mock_connect: mock.Mock):
        self.connection.port = None
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["port"] == 8848

    @mock.patch("pydolphindb.connect")
    def test_get_conn_default_host(self, mock_connect: mock.Mock):
        self.connection.host = None
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["host"] == 'localhost'

    @mock.patch("pydolphindb.connect")
    def test_get_conn_extras(self, mock_connect: mock.Mock):
        ind = 0
        for paras in self.test_extras:
            key = paras[0]
            value = paras[1]
            self.connection.extra = json.dumps({key: value})
            ind += 1
            self.db_hook.get_conn()
            assert mock_connect.call_count == ind
            args, kwargs = mock_connect.call_args
            assert args == ()
            assert kwargs[key] == value


class TestDolphinDBHook:
    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestDolphinDBHook(DolphinDBHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        self.db_hook = UnitTestDolphinDBHook()

    def test_run_without_parameters(self):
        sql = "SQL"
        self.db_hook.run(sql)
        self.cur.execute.assert_called_once_with(sql)
        assert self.conn.commit.called

    def test_run_with_parameters(self):
        sql = "SQL"
        param = ("p1", "p2")
        self.db_hook.run(sql, parameters=param)
        self.cur.execute.assert_called_once_with(sql, param)
        assert self.conn.commit.called

    def test_test_connection_use_dual_table(self):
        status, message = self.db_hook.test_connection()
        self.cur.execute.assert_called_once_with("select 1")
        assert status is True
        assert message == "Connection successfully tested"