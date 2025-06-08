from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()


def get_serving_endpoints():
    workspace_client = WorkspaceClient()
    serving_endpoints = workspace_client.serving_endpoints.list()
    return serving_endpoints


def get_spark():
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless().getOrCreate()
    return spark
