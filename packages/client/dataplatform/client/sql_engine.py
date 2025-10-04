import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service import sql
from sqlalchemy import Table as SQLAlchemyTable
from sqlalchemy.engine.base import Engine
from sqlmodel import Session, SQLModel, create_engine
from dataplatform.client.managed_table_client import ManagedTableClient
from dataplatform.core.logger import get_logger

log = get_logger(__name__)


def get_sql_serverless_warehouse() -> tuple[sql.EndpointInfo, WorkspaceClient]:
    if os.environ.get("DATABRICKS_PROFILE"):
        databricks_config = Config(
            host=os.environ.get("DATABRICKS_HOST"),
            token=os.environ.get("DATABRICKS_TOKEN"),
            profile=os.environ.get("DATABRICKS_PROFILE"),
        )
    else:
        databricks_config = Config(
            host=os.environ.get("DATABRICKS_HOST"),
            token=os.environ.get("DATABRICKS_TOKEN"),
        )
    workspace_client = WorkspaceClient(config=databricks_config)

    sql_warehouse = next(
        warehouse
        for warehouse in workspace_client.warehouses.list()
        if warehouse.enable_serverless_compute
    )
    return sql_warehouse, workspace_client


def get_engine(catalog: str, schema: str) -> Engine:
    """
    Returns a SQLAlchemy engine for the Databricks SQL database.
    The catalog and schema just for connection purposes.
    """
    sql_warehouse, workspace_client = get_sql_serverless_warehouse()

    http_path = sql_warehouse.odbc_params.path
    access_token = workspace_client.config.token
    host = sql_warehouse.odbc_params.hostname

    engine = create_engine(
        f"databricks://token:{access_token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}",
        echo=True,
    )

    return engine


def get_table_model(table_name) -> SQLAlchemyTable:
    managed_table_client = ManagedTableClient()
    table_name = managed_table_client._get_table_name(table_name)
    catalog, schema, table = table_name.split(".")
    try:
        metadata = SQLModel.metadata
        metadata.reflect(
            bind=get_engine(catalog, schema),
            schema=schema,
            only=[table],
        )
        table_model = metadata.tables[f"{schema}.{table}"]
        return table_model
    except Exception:
        log.warning(
            "No ORM model setup missing environment variables : "
            "DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, "
            "DATABRICKS_TOKEN"
        )
        return SQLModel.Table(table_name)


@contextmanager
def sql_session(catalog: str, schema: str) -> Generator[Session, Any, None]:
    engine = get_engine(catalog, schema)
    session = Session(bind=engine, autocommit=False)
    try:
        yield session
    finally:
        session.close()
