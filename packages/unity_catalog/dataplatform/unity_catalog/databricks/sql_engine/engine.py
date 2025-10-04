import logging
import os
from contextlib import contextmanager

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from sqlalchemy import Table as SQLAlchemyTable
from sqlmodel import Session, SQLModel, create_engine

log = logging.getLogger(__name__)


def get_engine(catalog: str):
    """
    Returns a SQLAlchemy engine for the Databricks SQL database. The catalog and schema just for connection purposes.
    """
    workspace_client = WorkspaceClient(
        config=Config(
            host=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            azure_client_id=os.getenv("DATABRICKS_AZURE_CLIENT_ID"),
            azure_client_secret=os.getenv("DATABRICKS_AZURE_CLIENT_SECRET"),
            azure_tenant_id=os.getenv("DATABRICKS_AZURE_TENANT_ID"),
            auth="azure-client-secret",
        )
    )
    sql_warehouse = next(
        warehouse
        for warehouse in workspace_client.warehouses.list()
        if warehouse.enable_serverless_compute
    )

    http_path = sql_warehouse.odbc_params.path
    host = sql_warehouse.odbc_params.hostname
    access_token = os.getenv("DATABRICKS_TOKEN")
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
