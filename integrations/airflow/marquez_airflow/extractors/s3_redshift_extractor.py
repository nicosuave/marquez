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
import textwrap
from contextlib import closing
from typing import Type, Optional

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from marquez.dataset import Source, Dataset
from marquez.models import DbTableSchema
from marquez.provider.postgres import get_table_schemas

from marquez_airflow.extractors import BaseExtractor, StepMetadata
from marquez_airflow.utils import get_connection_uri


class S3ToRedshiftExtractor(BaseExtractor):
    operator_class = Type[S3ToRedshiftTransfer]
    operator: S3ToRedshiftTransfer

    def __init__(self, operator: S3ToRedshiftTransfer):
        super().__init__(operator)

    @classmethod
    def get_operator_class(cls):
        return cls.operator_class

    def extract(self) -> StepMetadata:
        input = Dataset.from_table_schema(
            source=Source(
                name=self.operator.aws_conn_id,
                type='S3',
                connection_url=f's3://{self.operator.s3_bucket}/{self.operator.s3_key}'
            ),
            table_schema=self._get_table_schema()
        )
        output = Dataset.from_table(
            source=Source(
                name=self.operator.redshift_conn_id,
                type='POSTGRESQL',
                connection_url=get_connection_uri(self.operator.redshift_conn_id)
            ),
            table_name=self.operator.table,
            schema_name=self.operator.schema
        )
        return StepMetadata(
            name=f"{self.operator.dag_id}.{self.operator.task_id}",
            inputs=[input],
            outputs=[output]
        )

    def extract_on_complete(self, task_instance):
        return None

    def _get_table_schema(self) -> Optional[DbTableSchema]:
        hook = PostgresHook(self.operator.redshift_conn_id)
        with closing(hook.get_conn()) as conn:
            schema_query = textwrap.dedent(f"""
                SELECT table_schema,
                table_name,
                column_name,
                ordinal_position,
                udt_name
                FROM information_schema.columns
                WHERE table_name='{self.operator.table}'
                AND table_schema='{self.operator.schema}';
            """)
            schemas = get_table_schemas(conn, schema_query)
            if len(schemas) == 1:
                return schemas[0]
            return None
