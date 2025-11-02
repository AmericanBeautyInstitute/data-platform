"""BigQuery helper functions."""

import pyarrow as pa
from google.cloud import bigquery

from config.loaders import BigQueryTableConfig

ARROW_TO_BIGQUERY_TYPE_MAP = {
    pa.int8(): "INTEGER",
    pa.int16(): "INTEGER",
    pa.int32(): "INTEGER",
    pa.int64(): "INTEGER",
    pa.uint8(): "INTEGER",
    pa.uint16(): "INTEGER",
    pa.uint32(): "INTEGER",
    pa.uint64(): "INTEGER",
    pa.float32(): "FLOAT",
    pa.float64(): "FLOAT",
    pa.string(): "STRING",
    pa.bool_(): "BOOLEAN",
    pa.date32(): "DATE",
    pa.date64(): "DATE",
}


def arrow_to_bigquery_schema(arrow_schema: pa.Schema) -> list[bigquery.SchemaField]:
    """Converts PyArrow schema to BigQuery schema."""
    bigquery_schema = []

    for field in arrow_schema:
        bigquery_type = _get_bigquery_type(field.type)
        bigquery_schema.append(
            bigquery.SchemaField(field.name, bigquery_type, mode="NULLABLE")
        )

    return bigquery_schema


def ensure_table_exists(
    client: bigquery.Client,
    config: BigQueryTableConfig,
) -> None:
    """Creates BigQuery table if it doesn't exist."""
    table_reference = f"{client.project}.{config.table_reference}"

    if _table_exists(client, table_reference):
        print(f"✓ Table {table_reference} exists")
        return

    schema = _prepare_schema(config.schema)
    table = _create_table_object(table_reference, schema, config)

    client.create_table(table)
    print(f"Created table {table_reference}")


def _get_bigquery_type(arrow_type: pa.DataType) -> str:
    """Maps PyArrow type to BigQuery type string."""
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"
    return ARROW_TO_BIGQUERY_TYPE_MAP.get(arrow_type, "STRING")


def _table_exists(client: bigquery.Client, table_reference: str) -> bool:
    """Checks if table exists."""
    try:
        client.get_table(table_reference)
        return True
    except Exception:
        return False


def _prepare_schema(
    schema: list[bigquery.SchemaField] | pa.Schema,
) -> list[bigquery.SchemaField]:
    """Converts schema to BigQuery format if needed."""
    if isinstance(schema, pa.Schema):
        return arrow_to_bigquery_schema(schema)
    return schema


def _create_table_object(
    table_reference: str,
    schema: list[bigquery.SchemaField],
    config: BigQueryTableConfig,
) -> bigquery.Table:
    """Creates BigQuery Table object with configuration."""
    table = bigquery.Table(table_reference, schema=schema)

    if config.description:
        table.description = config.description

    if config.partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            field=config.partition_field,
            type_=bigquery.TimePartitioningType.DAY,
        )

    if config.cluster_fields:
        table.clustering_fields = config.cluster_fields

    return table
