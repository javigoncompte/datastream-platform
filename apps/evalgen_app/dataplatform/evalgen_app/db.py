import os

import fastsql
import polars as pl
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


class Annotation(BaseModel):
    input_id: str
    test_case_id: str
    input: str
    output: str
    notes: str
    eval_type: str
    features: str
    scenarios: str
    constraints: str
    personas: str
    assumptions: str
    generate_test_case_prompt: str
    agent_input: str
    agent_output: str


def convert_to_polars(model: BaseModel):
    return pl.DataFrame(model.model_dump())


def convert_polars_to_arrow(df: pl.DataFrame):
    return df.to_arrow()


def get_table_info(table_name: str) -> tuple[str, str, str]:
    catalog, schema, table_name = table_name.split(".")
    return catalog, schema, table_name


def get_db(table_name: str) -> fastsql.Database:
    host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")
    catalog, schema, table_name = get_table_info(table_name)
    engine = f"databricks://token:{access_token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
    db = fastsql.Database(engine)
    return db


def get_tables(table_name: str) -> fastsql.DBTable:
    print(table_name)
    db = get_db(table_name)
    name = table_name
    test_cases_table = db.create(Annotation, pk=("id"), name=f"{name}_annotations")
    return test_cases_table


def get_serving_endpoints():
    workspace_client = WorkspaceClient()
    serving_endpoints = workspace_client.serving_endpoints.list()
    return serving_endpoints


def get_spark():
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless().getOrCreate()
    return spark
