# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import mock

from airflow.hooks.postgres_hook import PostgresHook

from marquez.dataset import Source, Dataset, DatasetType
from marquez_airflow.extractors.postgres_extractor import PostgresExtractor
from tests.extractors.postgres_common import *


@mock.patch('marquez_airflow.extractors.postgres_extractor.\
PostgresExtractor._get_table_schemas')
def test_extract(mock_get_table_schemas):
    mock_get_table_schemas.side_effect = \
        [[DB_TABLE_SCHEMA], NO_DB_TABLE_SCHEMA]

    expected_inputs = [
        Dataset(
            type=DatasetType.DB_TABLE,
            name=f"{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            source=Source(
                type='POSTGRESQL',
                name=CONN_ID,
                connection_url=CONN_URI
            ),
            fields=[]
        )]

    expected_context = {
        'sql': SQL,
    }

    # Set the environment variable for the connection
    os.environ[f"AIRFLOW_CONN_{CONN_ID.upper()}"] = CONN_URI

    step_metadata = PostgresExtractor(TASK).extract()

    assert step_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert step_metadata.inputs == expected_inputs
    assert step_metadata.outputs == []
    assert step_metadata.context == expected_context


@mock.patch('psycopg2.connect')
def test_get_table_schemas(mock_conn):
    # (1) Define a simple hook class for testing
    class TestPostgresHook(PostgresHook):
        conn_name_attr = 'test_conn_id'

    # (2) Mock calls to postgres
    rows = [
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'id', 1, 'int4'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'amount_off', 2, 'int4'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'customer_email', 3, 'varchar'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'starts_on', 4, 'timestamp'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'ends_on', 5, 'timestamp')
    ]

    mock_conn.return_value \
        .cursor.return_value \
        .fetchall.return_value = rows

    # (3) Mock conn for hook
    hook = TestPostgresHook()
    hook.conn = mock_conn

    # (4) Extract table schemas for task
    extractor = PostgresExtractor(TASK)
    table_schemas = extractor._get_table_schemas(table_names=[DB_TABLE_NAME])

    assert table_schemas == [DB_TABLE_SCHEMA]
