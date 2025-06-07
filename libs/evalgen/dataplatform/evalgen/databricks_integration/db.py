import os

import fastsql
import polars as pl
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


def convert_to_polars(model: BaseModel):
    return pl.DataFrame(model.model_dump())


def convert_polars_to_arrow(df: pl.DataFrame):
    return df.to_arrow()


def get_table_info(table_name: str) -> tuple[str, str, str]:
    catalog, schema, table_name = table_name.split(".")
    return catalog, schema, table_name


def get_uri(catalog: str, schema: str) -> str:
    host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")
    return f"databricks://token:{access_token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}"


def get_fastsql_db(table_name: str) -> fastsql.Database:
    catalog, schema, table_name = table_name.split(".")
    engine = get_uri(catalog, schema)
    db = fastsql.Database(engine)
    return db


def get_serving_endpoints():
    workspace_client = WorkspaceClient()
    serving_endpoints = workspace_client.serving_endpoints.list()
    return serving_endpoints


def get_spark():
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless().getOrCreate()
    return spark
