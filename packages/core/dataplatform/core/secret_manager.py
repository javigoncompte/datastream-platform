import json
import os

import boto3
from botocore.exceptions import ClientError
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

os.environ["AWS_ACCESS_KEY_ID"] = w.dbutils.secrets.get(
    scope="prod-aws-secret-manager", key="aws-access-key-id"
)
os.environ["AWS_SECRET_ACCESS_KEY"] = w.dbutils.secrets.get(
    scope="prod-aws-secret-manager", key="aws-secret-access-key"
)


def get_secret(secret_name: str):
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name="us-east-1")

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response["SecretString"]
    secret = json.loads(secret)

    for _, value in secret.items():
        return value

    return secret
