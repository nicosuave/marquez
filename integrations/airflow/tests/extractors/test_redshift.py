import os
from unittest import mock

from airflow.operators.redshift_to_s3_operator import RedshiftToS3Transfer
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from marquez.dataset import Dataset, DatasetType, Source, Field

from marquez_airflow.extractors.redshift_s3_extractor import RedshiftToS3Extractor
from marquez_airflow.extractors.s3_redshift_extractor import S3ToRedshiftExtractor
from tests.extractors.postgres_common import *


S3_BUCKET = 's3_bucket'
S3_KEY = 's3_key'
AWS_CONN_ID = 'aws_default'
AWS_CONN_URL = f's3://{S3_BUCKET}/{S3_KEY}'

RedshiftTask = RedshiftToS3Transfer(
    task_id=TASK_ID,
    dag=DAG,
    schema=DB_SCHEMA_NAME,
    table=DB_TABLE_NAME,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY,
    redshift_conn_id=CONN_ID,
    aws_conn_id=AWS_CONN_ID
)

S3Task = S3ToRedshiftTransfer(
    task_id=TASK_ID,
    dag=DAG,
    schema=DB_SCHEMA_NAME,
    table=DB_TABLE_NAME,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY,
    redshift_conn_id=CONN_ID,
    aws_conn_id=AWS_CONN_ID
)


@mock.patch('marquez_airflow.extractors.redshift_s3_extractor.\
RedshiftToS3Extractor._get_table_schema')
def test_redshift_to_s3_extract(mock_get_table_schema):
    mock_get_table_schema.return_value = DB_TABLE_SCHEMA

    # Set the environment variable for the connection
    os.environ[f"AIRFLOW_CONN_{CONN_ID.upper()}"] = CONN_URI

    step_metadata = RedshiftToS3Extractor(RedshiftTask).extract()

    assert step_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert step_metadata.inputs == [Dataset(
        type=DatasetType.DB_TABLE,
        name=f"{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
        source=Source(
            type='POSTGRESQL',
            name=CONN_ID,
            connection_url=CONN_URI
        ),
        fields=[]
    )]
    assert step_metadata.outputs == [Dataset(
        type=DatasetType.DB_TABLE,
        name=f'{DB_SCHEMA_NAME}.{DB_TABLE_NAME}',
        source=Source(
            type='S3',
            name=AWS_CONN_ID,
            connection_url=AWS_CONN_URL
        ),
        fields=[
            Field('id', 'int4', [], None),
            Field('amount_off', 'int4', [], None),
            Field('customer_email', 'varchar', [], None),
            Field('starts_on', 'timestamp', [], None),
            Field('ends_on', 'timestamp', [], None)
        ]
    )]


@mock.patch('marquez_airflow.extractors.redshift_s3_extractor.\
S3ToRedshiftExtractor._get_table_schema')
def test_s3_to_redshift_extract(mock_get_table_schema):
    mock_get_table_schema.return_value = DB_TABLE_SCHEMA

    # Set the environment variable for the connection
    os.environ[f"AIRFLOW_CONN_{CONN_ID.upper()}"] = CONN_URI

    step_metadata = S3ToRedshiftExtractor(S3Task).extract()

    assert step_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert step_metadata.outputs == [Dataset(
        type=DatasetType.DB_TABLE,
        name=f"{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
        source=Source(
            type='POSTGRESQL',
            name=CONN_ID,
            connection_url=CONN_URI
        ),
        fields=[
            Field('id', 'int4', [], None),
            Field('amount_off', 'int4', [], None),
            Field('customer_email', 'varchar', [], None),
            Field('starts_on', 'timestamp', [], None),
            Field('ends_on', 'timestamp', [], None)
        ]
    )]
    assert step_metadata.inputs == [Dataset(
        type=DatasetType.DB_TABLE,
        name=f'{DB_SCHEMA_NAME}.{DB_TABLE_NAME}',
        source=Source(
            type='S3',
            name=AWS_CONN_ID,
            connection_url=AWS_CONN_URL
        ),
        fields=[]
    )]
