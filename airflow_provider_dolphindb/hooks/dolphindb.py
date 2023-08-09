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

import logging

import pydolphindb

import dolphindb as ddb
from airflow.models import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook


class DolphinDBHook(DbApiHook):
    """Interact with DolphinDB."""

    conn_name_attr = "dolphindb_conn_id"
    default_conn_name = "dolphindb_default"
    conn_type = "dolphindb"
    hook_name = "DolphinDB"
    placeholder = "?"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def _get_conn_config_dolphindb(self, conn: Connection) -> dict:
        conn_config = {
            "username": conn.login,
            "password": conn.password,
            "host": conn.host or "localhost",
            "port": int(conn.port) if conn.port else 8848,
        }
        logging.info("Extra arguments: %s", conn.extra_dejson)
        if "enableSSL" in conn.extra_dejson.keys():
            conn_config["enableSSL"] = conn.extra_dejson["enableSSL"]
        if "enableASYNC" in conn.extra_dejson.keys():
            conn_config["enableASYNC"] = conn.extra_dejson["enableASYNC"]
        if "keepAliveTime" in conn.extra_dejson.keys():
            conn_config["keepAliveTime"] = conn.extra_dejson["keepAliveTime"]
        if "enableChunkGranularityConfig" in conn.extra_dejson.keys():
            conn_config["enableChunkGranularityConfig"] = conn.extra_dejson["enableChunkGranularityConfig"]
        if "compress" in conn.extra_dejson.keys():
            conn_config["compress"] = conn.extra_dejson["compress"]
        if "enablePickle" in conn.extra_dejson.keys():
            conn_config["enablePickle"] = conn.extra_dejson["enablePickle"]
        if "protocol" in conn.extra_dejson.keys():
            conn_config["protocol"] = conn.extra_dejson["protocol"]
        if "python" in conn.extra_dejson.keys():
            conn_config["python"] = conn.extra_dejson["python"]
        if "startup" in conn.extra_dejson.keys():
            conn_config["startup"] = conn.extra_dejson["startup"]
        if "highAvailability" in conn.extra_dejson.keys():
            conn_config["highAvailability"] = conn.extra_dejson["highAvailability"]
        if "highAvailabilitySites" in conn.extra_dejson.keys():
            conn_config["highAvailabilitySites"] = conn.extra_dejson["highAvailabilitySites"]
        if "reconnect" in conn.extra_dejson.keys():
            conn_config["reconnect"] = conn.extra_dejson["reconnect"]
        if "enableEncryption" in conn.extra_dejson.keys():
            conn_config["enableEncryption"] = conn.extra_dejson["enableEncryption"]
        return conn_config

    def get_conn(self) -> ddb.session:
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)
        config = self._get_conn_config_dolphindb(airflow_conn)
        conn = pydolphindb.connect(**config)
        return conn

    def get_uri(self) -> str:
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)
        uri = airflow_conn.get_uri()
        return uri
