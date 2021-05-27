from contextlib import closing
from typing import Optional

from marquez.models import DbTableName, DbColumn, DbTableSchema


_TABLE_SCHEMA = 0
_TABLE_NAME = 1
_COLUMN_NAME = 2
_ORDINAL_POSITION = 3
# Use 'udt_name' which is the underlying type of column
# (ex: int4, timestamp, varchar, etc)
_UDT_NAME = 4


def get_table_schemas(conn, schema_query: str):
    # Keeps track of the schema by table.
    schemas_by_table = {}

    with closing(conn.cursor()) as cursor:
        cursor.execute(schema_query)
        x = cursor.fetchall()
        for row in x:
            table_schema_name: str = row[_TABLE_SCHEMA]
            table_name: DbTableName = DbTableName(row[_TABLE_NAME])
            table_column: DbColumn = DbColumn(
                name=row[_COLUMN_NAME],
                type=row[_UDT_NAME],
                ordinal_position=row[_ORDINAL_POSITION]
            )

            # Attempt to get table schema
            table_key: str = f"{table_schema_name}.{table_name}"
            table_schema: Optional[DbTableSchema] = schemas_by_table.get(table_key)

            if table_schema:
                # Add column to existing table schema.
                schemas_by_table[table_key].columns.append(table_column)
            else:
                # Create new table schema with column.
                schemas_by_table[table_key] = DbTableSchema(
                    schema_name=table_schema_name,
                    table_name=table_name,
                    columns=[table_column]
                )
        return list(schemas_by_table.values())

