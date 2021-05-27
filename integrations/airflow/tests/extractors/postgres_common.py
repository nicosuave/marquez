from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow import DAG as AIRFLOW_DAG
from marquez.models import DbTableName, DbColumn, DbTableSchema

CONN_ID = 'food_delivery_db'
CONN_URI = 'postgres://localhost:5432/food_delivery'
DB_SCHEMA_NAME = 'public'
DB_TABLE_NAME = DbTableName('discounts')
DB_TABLE_COLUMNS = [
    DbColumn(
        name='id',
        type='int4',
        ordinal_position=1
    ),
    DbColumn(
        name='amount_off',
        type='int4',
        ordinal_position=2
    ),
    DbColumn(
        name='customer_email',
        type='varchar',
        ordinal_position=3
    ),
    DbColumn(
        name='starts_on',
        type='timestamp',
        ordinal_position=4
    ),
    DbColumn(
        name='ends_on',
        type='timestamp',
        ordinal_position=5
    )
]
DB_TABLE_SCHEMA = DbTableSchema(
    schema_name=DB_SCHEMA_NAME,
    table_name=DB_TABLE_NAME,
    columns=DB_TABLE_COLUMNS
)
NO_DB_TABLE_SCHEMA = []
SQL = f"SELECT * FROM {DB_TABLE_NAME.name};"
DAG_ID = 'email_discounts'
DAG_OWNER = 'datascience'
DAG_DEFAULT_ARGS = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}
DAG_DESCRIPTION = \
    'Email discounts to customers that have experienced order delays daily'
TASK_ID = 'select'


DAG = dag = AIRFLOW_DAG(
    DAG_ID,
    schedule_interval='@weekly',
    default_args=DAG_DEFAULT_ARGS,
    description=DAG_DESCRIPTION
)

TASK = PostgresOperator(
    task_id=TASK_ID,
    postgres_conn_id=CONN_ID,
    sql=SQL,
    dag=DAG
)
