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

import datetime
import os

from airflow import DAG
from airflow_provider_dolphindb.operators.dolphindb import DolphinDBOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_dolphindb"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2023, 2, 2),
    schedule="@once",
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_operator_dolphindb]
    create_table = DolphinDBOperator(
        task_id="create_table",
        dolphindb_conn_id="dolphindb_default",
        sql="""
            dbPath = "dfs://example_value_db"
            if (existsDatabase(dbPath))
                dropDatabase(dbPath)
            t  = table(100:100, `id`time`vol, [SYMBOL,DATE, INT])
            db = database(dbPath, VALUE, `APPL`IBM`AMZN)
            pt = db.createPartitionedTable(t, `pt, `id)
          """,
          dag = dag,
    )
    # [END howto_operator_dolphindb]

    # [START howto_operator_dolphindb_external_file]
    insert_data = DolphinDBOperator(
        task_id="insert_data",
        dolphindb_conn_id="dolphindb_default",
        file_path="/scripts/insert_data.dos",
        dag = dag,
    )
    # [END howto_operator_dolphindb_external_file]

    create_table >> insert_data
