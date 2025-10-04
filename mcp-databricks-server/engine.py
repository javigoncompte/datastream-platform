import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service import sql
from sqlalchemy import Table as SQLAlchemyTable
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session, SQLModel, create_engine

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def get_classic_sql_warehouse() -> sql.EndpointInfo:
    workspace_client = WorkspaceClient(
        config=Config(profile=os.environ.get("DATABRICKS_PROFILE"))
    )
    sql_warehouse = next(
        warehouse
        for warehouse in workspace_client.warehouses.list()
        if warehouse.warehouse_type == sql.EndpointInfoWarehouseType.CLASSIC
    )
    return sql_warehouse


def get_sql_serverless_warehouse() -> sql.EndpointInfo:
    workspace_client = WorkspaceClient(
        config=Config(
            host=os.environ.get("DATABRICKS_HOST"),
            token=os.environ.get("DATABRICKS_TOKEN"),
            auth="pat",
        )
    )
    sql_warehouse = next(
        warehouse
        for warehouse in workspace_client.warehouses.list()
        if warehouse.enable_serverless_compute
    )
    return sql_warehouse


def get_engine(catalog: str):
    """
    Returns a SQLAlchemy engine for the Databricks SQL database.
    The catalog and schema just for connection purposes.
    """
    sql_warehouse = get_sql_serverless_warehouse()

    http_path = sql_warehouse.odbc_params.path
    host = sql_warehouse.odbc_params.hostname
    access_token = os.environ.get("DATABRICKS_TOKEN")
    engine = create_engine(
        f"databricks://token:{access_token}@{host}?http_path={http_path}&catalog={catalog}&schema=raw",
        echo=True,
    )

    return engine


def get_table_model(table_name) -> SQLAlchemyTable:
    catalog, schema, table = table_name.split(".")
    try:
        metadata = SQLModel.metadata
        metadata.reflect(
            bind=get_engine(catalog),
            schema=schema,
            only=[table],
        )
        table_model = metadata.tables[f"{schema}.{table}"]
        return table_model
    except Exception:
        log.warning(
            "No ORM model setup missing environment variables : "
            "DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN"
        )
        return SQLModel.Table(table_name)


@contextmanager
def sql_session(catalog: str):
    engine = get_engine(catalog)
    session = Session(bind=engine, autocommit=False)
    try:
        yield session
    finally:
        session.close()


def execute_sql_query(
    sql_query: str,
    catalog: str = "default",
    parameters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Execute a SQL query using SQLAlchemy engine with proper error handling.

    Args:
        sql_query: The SQL query to execute
        catalog: The catalog to use for the connection
        parameters: Optional parameters for parameterized queries

    Returns:
        Dict with status, data, and metadata
    """
    try:
        engine = get_engine(catalog)
        with engine.connect() as conn:
            # Use text() for raw SQL and handle parameters
            if parameters:
                result = conn.execute(text(sql_query), parameters)
            else:
                result = conn.execute(text(sql_query))

            # Get column names
            column_names = [col.name for col in result.keys()]

            # Fetch all results
            rows = result.fetchall()

            # Convert to list of dictionaries
            data = [dict(zip(column_names, row, strict=False)) for row in rows]

            return {
                "status": "success",
                "row_count": len(data),
                "data": data,
                "columns": column_names,
            }

    except SQLAlchemyError as e:
        log.error(f"SQLAlchemy error executing query: {e}")
        return {
            "status": "error",
            "error": f"Database error: {str(e)}",
            "row_count": 0,
            "data": [],
        }
    except Exception as e:
        log.error(f"Unexpected error executing query: {e}")
        return {
            "status": "error",
            "error": f"Unexpected error: {str(e)}",
            "row_count": 0,
            "data": [],
        }
